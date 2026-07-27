"""Kafka consumer ingestion service using aiokafka."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pyarrow as pa
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from olap_engine.config.pipeline_config import PipelineConfig
from olap_engine.schemas.event_schema import FACT_EVENT_SCHEMA

logger = logging.getLogger(__name__)

# Exponential backoff constants
_INITIAL_BACKOFF_MS = 100
_MAX_BACKOFF_MS = 30_000
_BACKOFF_MULTIPLIER = 2


class IngestionService:
    """Kafka consumer that ingests events and deserializes to Arrow format.

    Consumes from the configured fact_topic, deserializes JSON payloads into
    Arrow RecordBatches, and routes malformed events to a dead-letter topic.
    Uses manual offset commit for at-least-once delivery guarantees.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
        self._throughput_counter: int = 0
        self._throughput_last_reset: float = time.monotonic()
        self._events_per_sec: float = 0.0
        self._current_backoff_ms: int = _INITIAL_BACKOFF_MS
        self._running: bool = False

    async def start(self) -> None:
        """Start consumer, subscribe to fact_topic, resume from committed offsets.

        Creates the aiokafka consumer with manual offset commits and subscribes
        to the configured fact topic. Also creates a producer for the dead-letter
        topic routing.
        """
        self._consumer = AIOKafkaConsumer(
            self._config.fact_topic,
            bootstrap_servers=self._config.kafka_bootstrap_servers,
            group_id=self._config.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: v,  # raw bytes, we deserialize manually
        )
        self._dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self._config.kafka_bootstrap_servers,
            value_serializer=lambda v: v if isinstance(v, bytes) else v.encode("utf-8"),
        )

        await self._consumer.start()
        await self._dlq_producer.start()
        self._running = True
        self._current_backoff_ms = _INITIAL_BACKOFF_MS
        logger.info(
            "IngestionService started, consuming from topic=%s group=%s",
            self._config.fact_topic,
            self._config.consumer_group,
        )

    async def stop(self) -> None:
        """Gracefully stop consumer and producer, committing final offsets."""
        self._running = False
        if self._consumer is not None:
            try:
                await self._consumer.commit()
            except Exception as exc:
                logger.warning("Failed to commit offsets on stop: %s", exc)
            await self._consumer.stop()
            self._consumer = None
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
            self._dlq_producer = None
        logger.info("IngestionService stopped")

    async def consume_batch(
        self, max_records: int = 1000, timeout_ms: int = 100
    ) -> pa.RecordBatch:
        """Consume up to max_records from Kafka, return as Arrow RecordBatch.

        Malformed events (JSON parse errors, missing required fields) are
        routed to the dead-letter topic. Valid events are deserialized into
        an Arrow RecordBatch matching FACT_EVENT_SCHEMA.

        On broker disconnect, applies exponential backoff (100ms → 30s).

        Args:
            max_records: Maximum number of records to consume in one batch.
            timeout_ms: Timeout in milliseconds to wait for messages.

        Returns:
            Arrow RecordBatch of successfully deserialized events.
            May be empty if no valid events were consumed.
        """
        if self._consumer is None:
            raise RuntimeError("IngestionService not started. Call start() first.")

        try:
            data = await self._consumer.getmany(
                timeout_ms=timeout_ms, max_records=max_records
            )
            # Reset backoff on successful fetch
            self._current_backoff_ms = _INITIAL_BACKOFF_MS
        except Exception as exc:
            logger.error("Broker fetch error: %s, backing off %dms", exc, self._current_backoff_ms)
            await asyncio.sleep(self._current_backoff_ms / 1000.0)
            self._current_backoff_ms = min(
                self._current_backoff_ms * _BACKOFF_MULTIPLIER, _MAX_BACKOFF_MS
            )
            return pa.RecordBatch.from_pydict(
                {field.name: [] for field in FACT_EVENT_SCHEMA}, schema=FACT_EVENT_SCHEMA
            )

        # Collect arrays for the RecordBatch
        event_ids: list[str] = []
        event_times: list[datetime] = []
        ingest_times: list[datetime] = []
        join_keys: list[str] = []
        payload_jsons: list[str] = []
        source_topics: list[str] = []
        partitions: list[int] = []
        offsets: list[int] = []

        ingest_now = datetime.now(timezone.utc)

        for tp, messages in data.items():
            for msg in messages:
                try:
                    raw_value = msg.value
                    if isinstance(raw_value, bytes):
                        payload = json.loads(raw_value.decode("utf-8"))
                    elif isinstance(raw_value, str):
                        payload = json.loads(raw_value)
                    else:
                        payload = raw_value

                    # Extract required fields
                    event_id = payload.get("event_id", str(uuid4()))
                    event_time_raw = payload.get("event_time")
                    join_key = payload.get("join_key", "")

                    if not join_key:
                        raise ValueError("Missing or empty 'join_key' field")

                    if event_time_raw is None:
                        raise ValueError("Missing 'event_time' field")

                    # Parse event_time (ISO format or epoch)
                    if isinstance(event_time_raw, (int, float)):
                        event_time = datetime.fromtimestamp(event_time_raw, tz=timezone.utc)
                    else:
                        event_time = datetime.fromisoformat(str(event_time_raw))
                        if event_time.tzinfo is None:
                            event_time = event_time.replace(tzinfo=timezone.utc)

                    # Build payload JSON (full payload minus extracted fields)
                    payload_json = json.dumps(payload)

                    event_ids.append(str(event_id))
                    event_times.append(event_time)
                    ingest_times.append(ingest_now)
                    join_keys.append(join_key)
                    payload_jsons.append(payload_json)
                    source_topics.append(tp.topic)
                    partitions.append(msg.partition)
                    offsets.append(msg.offset)

                except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
                    # Route malformed event to dead-letter topic
                    raw_bytes = msg.value if isinstance(msg.value, bytes) else str(msg.value).encode()
                    await self.route_to_dead_letter(raw_bytes, str(e))

        # Update throughput counter
        valid_count = len(event_ids)
        self._throughput_counter += valid_count
        self._update_throughput()

        # Commit offsets after successful processing
        if data:
            await self._consumer.commit()

        # Build Arrow RecordBatch
        if valid_count == 0:
            return pa.RecordBatch.from_pydict(
                {field.name: [] for field in FACT_EVENT_SCHEMA}, schema=FACT_EVENT_SCHEMA
            )

        arrays = {
            "event_id": event_ids,
            "event_time": event_times,
            "ingest_time": ingest_times,
            "join_key": join_keys,
            "payload_json": payload_jsons,
            "source_topic": source_topics,
            "partition": partitions,
            "offset": offsets,
        }

        return pa.RecordBatch.from_pydict(arrays, schema=FACT_EVENT_SCHEMA)

    async def route_to_dead_letter(self, raw_message: bytes, error: str) -> None:
        """Send malformed message to dead-letter topic with error metadata.

        Args:
            raw_message: The raw bytes of the malformed message.
            error: Description of the deserialization error.
        """
        if self._dlq_producer is None:
            logger.warning("DLQ producer not available, dropping malformed message")
            return

        dlq_envelope = json.dumps({
            "original_message": raw_message.decode("utf-8", errors="replace"),
            "error": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source_topic": self._config.fact_topic,
        }).encode("utf-8")

        try:
            await self._dlq_producer.send_and_wait(
                self._config.dead_letter_topic, dlq_envelope
            )
            logger.debug("Routed malformed message to dead-letter topic: %s", error)
        except Exception as exc:
            logger.error("Failed to send to dead-letter topic: %s", exc)

    def get_throughput(self) -> float:
        """Return current events-per-second throughput.

        Throughput is calculated as a rolling counter that resets every second.
        """
        self._update_throughput()
        return self._events_per_sec

    def _update_throughput(self) -> None:
        """Update the rolling throughput counter."""
        now = time.monotonic()
        elapsed = now - self._throughput_last_reset
        if elapsed >= 1.0:
            self._events_per_sec = self._throughput_counter / elapsed
            self._throughput_counter = 0
            self._throughput_last_reset = now
