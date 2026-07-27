"""Unit tests for the IngestionService (Kafka consumer with aiokafka)."""

from __future__ import annotations

import asyncio
import json
import time
from collections import namedtuple
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pyarrow as pa
import pytest

from olap_engine.config.pipeline_config import PipelineConfig
from olap_engine.ingestion.service import (
    IngestionService,
    _INITIAL_BACKOFF_MS,
    _MAX_BACKOFF_MS,
)
from olap_engine.schemas.event_schema import FACT_EVENT_SCHEMA


# --- Helpers ---

TopicPartition = namedtuple("TopicPartition", ["topic", "partition"])


def _make_kafka_message(
    payload: dict | bytes | str,
    topic: str = "fact-events",
    partition: int = 0,
    offset: int = 0,
) -> MagicMock:
    """Create a mock Kafka message."""
    msg = MagicMock()
    if isinstance(payload, dict):
        msg.value = json.dumps(payload).encode("utf-8")
    elif isinstance(payload, str):
        msg.value = payload.encode("utf-8")
    else:
        msg.value = payload
    msg.topic = topic
    msg.partition = partition
    msg.offset = offset
    return msg


def _valid_event_payload(
    event_id: str | None = None,
    join_key: str = "user_123",
    event_time: str | None = None,
) -> dict:
    """Create a valid event payload dict."""
    return {
        "event_id": event_id or str(uuid4()),
        "event_time": event_time or datetime.now(timezone.utc).isoformat(),
        "join_key": join_key,
        "action": "click",
        "page": "/home",
    }


def _default_config() -> PipelineConfig:
    """Create a default pipeline config for testing."""
    return PipelineConfig()


# --- Tests ---


class TestIngestionServiceStartStop:
    """Tests for start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_consumer_and_producer(self):
        """start() should create consumer and producer and start them."""
        config = _default_config()
        service = IngestionService(config)

        with patch("olap_engine.ingestion.service.AIOKafkaConsumer") as MockConsumer, \
             patch("olap_engine.ingestion.service.AIOKafkaProducer") as MockProducer:
            mock_consumer = AsyncMock()
            mock_producer = AsyncMock()
            MockConsumer.return_value = mock_consumer
            MockProducer.return_value = mock_producer

            await service.start()

            # Consumer created with correct args
            MockConsumer.assert_called_once()
            call_kwargs = MockConsumer.call_args
            assert config.fact_topic in call_kwargs.args
            assert call_kwargs.kwargs["group_id"] == config.consumer_group
            assert call_kwargs.kwargs["enable_auto_commit"] is False

            # Both started
            mock_consumer.start.assert_awaited_once()
            mock_producer.start.assert_awaited_once()

            await service.stop()

    @pytest.mark.asyncio
    async def test_stop_commits_offsets_and_stops(self):
        """stop() should commit offsets and stop consumer and producer."""
        config = _default_config()
        service = IngestionService(config)

        with patch("olap_engine.ingestion.service.AIOKafkaConsumer") as MockConsumer, \
             patch("olap_engine.ingestion.service.AIOKafkaProducer") as MockProducer:
            mock_consumer = AsyncMock()
            mock_producer = AsyncMock()
            MockConsumer.return_value = mock_consumer
            MockProducer.return_value = mock_producer

            await service.start()
            await service.stop()

            mock_consumer.commit.assert_awaited_once()
            mock_consumer.stop.assert_awaited_once()
            mock_producer.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_consume_batch_raises_if_not_started(self):
        """consume_batch() should raise RuntimeError if start() not called."""
        config = _default_config()
        service = IngestionService(config)

        with pytest.raises(RuntimeError, match="not started"):
            await service.consume_batch()


class TestIngestionServiceConsumeBatch:
    """Tests for consume_batch deserialization logic."""

    @pytest.mark.asyncio
    async def test_consume_valid_events_returns_record_batch(self):
        """Valid events should be deserialized into an Arrow RecordBatch."""
        config = _default_config()
        service = IngestionService(config)

        tp = TopicPartition(topic="fact-events", partition=0)
        event1 = _valid_event_payload(join_key="user_a")
        event2 = _valid_event_payload(join_key="user_b")
        messages = [
            _make_kafka_message(event1, offset=0),
            _make_kafka_message(event2, offset=1),
        ]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: messages})
        mock_consumer.commit = AsyncMock()

        mock_producer = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = mock_producer
        service._running = True

        batch = await service.consume_batch()

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 2
        assert batch.schema == FACT_EVENT_SCHEMA
        assert batch.column("join_key").to_pylist() == ["user_a", "user_b"]
        mock_consumer.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_consume_empty_returns_empty_batch(self):
        """When no messages available, return an empty RecordBatch."""
        config = _default_config()
        service = IngestionService(config)

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={})
        mock_consumer.commit = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True

        batch = await service.consume_batch()

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 0
        assert batch.schema == FACT_EVENT_SCHEMA

    @pytest.mark.asyncio
    async def test_consume_with_epoch_event_time(self):
        """Events with epoch timestamp event_time should be correctly parsed."""
        config = _default_config()
        service = IngestionService(config)

        epoch_time = 1700000000  # 2023-11-14T22:13:20Z
        event = {
            "event_id": str(uuid4()),
            "event_time": epoch_time,
            "join_key": "user_x",
            "data": "test",
        }

        tp = TopicPartition(topic="fact-events", partition=0)
        messages = [_make_kafka_message(event, offset=5)]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: messages})
        mock_consumer.commit = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True

        batch = await service.consume_batch()

        assert batch.num_rows == 1
        # The event_time should be parsed from epoch
        event_time_col = batch.column("event_time").to_pylist()[0]
        expected = datetime.fromtimestamp(epoch_time, tz=timezone.utc)
        assert event_time_col == expected


class TestIngestionServiceDeadLetter:
    """Tests for malformed event routing to dead-letter topic."""

    @pytest.mark.asyncio
    async def test_malformed_json_routes_to_dlq(self):
        """Malformed JSON messages should be routed to the dead-letter topic."""
        config = _default_config()
        service = IngestionService(config)

        tp = TopicPartition(topic="fact-events", partition=0)
        messages = [_make_kafka_message(b"not valid json", offset=0)]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: messages})
        mock_consumer.commit = AsyncMock()

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = mock_producer
        service._running = True

        batch = await service.consume_batch()

        # No valid events
        assert batch.num_rows == 0
        # DLQ producer was called
        mock_producer.send_and_wait.assert_awaited_once()
        call_args = mock_producer.send_and_wait.call_args
        assert call_args.args[0] == config.dead_letter_topic

    @pytest.mark.asyncio
    async def test_missing_join_key_routes_to_dlq(self):
        """Events missing join_key should be routed to dead-letter topic."""
        config = _default_config()
        service = IngestionService(config)

        event_no_key = {
            "event_id": str(uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "join_key": "",  # empty
            "data": "test",
        }

        tp = TopicPartition(topic="fact-events", partition=0)
        messages = [_make_kafka_message(event_no_key, offset=0)]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: messages})
        mock_consumer.commit = AsyncMock()

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = mock_producer
        service._running = True

        batch = await service.consume_batch()

        assert batch.num_rows == 0
        mock_producer.send_and_wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_event_time_routes_to_dlq(self):
        """Events missing event_time should be routed to dead-letter topic."""
        config = _default_config()
        service = IngestionService(config)

        event_no_time = {
            "event_id": str(uuid4()),
            "join_key": "user_1",
            "data": "test",
            # No event_time
        }

        tp = TopicPartition(topic="fact-events", partition=0)
        messages = [_make_kafka_message(event_no_time, offset=0)]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: messages})
        mock_consumer.commit = AsyncMock()

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = mock_producer
        service._running = True

        batch = await service.consume_batch()

        assert batch.num_rows == 0
        mock_producer.send_and_wait.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_mixed_valid_and_malformed_events(self):
        """Valid events should still be processed even when malformed events exist."""
        config = _default_config()
        service = IngestionService(config)

        valid_event = _valid_event_payload(join_key="good_user")
        tp = TopicPartition(topic="fact-events", partition=0)
        messages = [
            _make_kafka_message(b"bad json", offset=0),
            _make_kafka_message(valid_event, offset=1),
            _make_kafka_message(b"{invalid", offset=2),
        ]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: messages})
        mock_consumer.commit = AsyncMock()

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = mock_producer
        service._running = True

        batch = await service.consume_batch()

        # Only 1 valid event
        assert batch.num_rows == 1
        assert batch.column("join_key").to_pylist() == ["good_user"]
        # 2 malformed events routed to DLQ
        assert mock_producer.send_and_wait.await_count == 2

    @pytest.mark.asyncio
    async def test_route_to_dead_letter_sends_envelope(self):
        """route_to_dead_letter should send an envelope with error metadata."""
        config = _default_config()
        service = IngestionService(config)

        mock_producer = AsyncMock()
        mock_producer.send_and_wait = AsyncMock()
        service._dlq_producer = mock_producer
        service._config = config

        raw = b"broken message"
        error = "JSON decode error"

        await service.route_to_dead_letter(raw, error)

        mock_producer.send_and_wait.assert_awaited_once()
        call_args = mock_producer.send_and_wait.call_args
        assert call_args.args[0] == config.dead_letter_topic

        # Parse the envelope
        envelope = json.loads(call_args.args[1])
        assert envelope["error"] == error
        assert envelope["original_message"] == "broken message"
        assert envelope["source_topic"] == config.fact_topic
        assert "timestamp" in envelope


class TestIngestionServiceBackoff:
    """Tests for exponential backoff on broker disconnect."""

    @pytest.mark.asyncio
    async def test_broker_error_triggers_backoff(self):
        """On broker error, should wait with exponential backoff and return empty batch."""
        config = _default_config()
        service = IngestionService(config)

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(side_effect=Exception("Broker unreachable"))

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            batch = await service.consume_batch()

            assert batch.num_rows == 0
            mock_sleep.assert_awaited_once_with(_INITIAL_BACKOFF_MS / 1000.0)

    @pytest.mark.asyncio
    async def test_backoff_increases_exponentially(self):
        """Repeated broker errors should increase the backoff delay."""
        config = _default_config()
        service = IngestionService(config)

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(side_effect=Exception("Broker unreachable"))

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            # First error: 100ms
            await service.consume_batch()
            assert mock_sleep.await_args_list[0].args[0] == pytest.approx(0.1)

            # Second error: 200ms
            await service.consume_batch()
            assert mock_sleep.await_args_list[1].args[0] == pytest.approx(0.2)

            # Third error: 400ms
            await service.consume_batch()
            assert mock_sleep.await_args_list[2].args[0] == pytest.approx(0.4)

    @pytest.mark.asyncio
    async def test_backoff_caps_at_max(self):
        """Backoff should not exceed 30 seconds."""
        config = _default_config()
        service = IngestionService(config)

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(side_effect=Exception("Broker unreachable"))

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True

        # Set backoff near max
        service._current_backoff_ms = _MAX_BACKOFF_MS

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await service.consume_batch()
            # Should sleep at max
            assert mock_sleep.await_args_list[0].args[0] == _MAX_BACKOFF_MS / 1000.0

    @pytest.mark.asyncio
    async def test_successful_fetch_resets_backoff(self):
        """After a successful fetch, backoff should reset to initial value."""
        config = _default_config()
        service = IngestionService(config)

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={})
        mock_consumer.commit = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True
        service._current_backoff_ms = 5000  # Simulate previous backoff

        await service.consume_batch()

        assert service._current_backoff_ms == _INITIAL_BACKOFF_MS


class TestIngestionServiceThroughput:
    """Tests for throughput tracking."""

    @pytest.mark.asyncio
    async def test_throughput_tracks_events_per_second(self):
        """get_throughput() should return events/sec based on rolling counter."""
        config = _default_config()
        service = IngestionService(config)

        # Simulate having processed events
        service._throughput_counter = 500
        service._throughput_last_reset = time.monotonic() - 1.0  # 1 second ago

        throughput = service.get_throughput()

        assert throughput == pytest.approx(500.0, rel=0.1)

    @pytest.mark.asyncio
    async def test_throughput_starts_at_zero(self):
        """Initial throughput should be 0."""
        config = _default_config()
        service = IngestionService(config)

        # Just created, counter is 0, less than 1 second elapsed
        assert service.get_throughput() == 0.0

    @pytest.mark.asyncio
    async def test_consume_batch_increments_throughput_counter(self):
        """Each successful consume_batch should increment the throughput counter."""
        config = _default_config()
        service = IngestionService(config)

        tp = TopicPartition(topic="fact-events", partition=0)
        events = [_make_kafka_message(_valid_event_payload(), offset=i) for i in range(5)]

        mock_consumer = AsyncMock()
        mock_consumer.getmany = AsyncMock(return_value={tp: events})
        mock_consumer.commit = AsyncMock()

        service._consumer = mock_consumer
        service._dlq_producer = AsyncMock()
        service._running = True

        await service.consume_batch()

        # Counter should have 5 events (might have been reset if elapsed > 1s)
        # Check that events were counted
        assert service._throughput_counter == 5 or service._events_per_sec > 0
