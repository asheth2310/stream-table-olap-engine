"""Pipeline orchestration: Ingestion → Watermark → Join → Window → DuckDB.

Wires all backend components together into a single async processing loop.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone

from olap_engine.config.pipeline_config import PipelineConfig
from olap_engine.ingestion.service import IngestionService
from olap_engine.join.dimension_table import DimensionTableManager
from olap_engine.join.engine import JoinEngine
from olap_engine.storage.duckdb_store import DuckDBStore
from olap_engine.watermark.manager import WatermarkDecision, WatermarkManager
from olap_engine.window.aggregator import WindowAggregator

logger = logging.getLogger(__name__)


class Pipeline:
    """Orchestrates: Ingestion → Watermark → Join → Window → DuckDB.

    The pipeline runs as an async task that continuously:
    1. Consumes batches from Kafka via IngestionService
    2. Filters events through the WatermarkManager
    3. Joins accepted events with the dimension table via JoinEngine
    4. Feeds joined events to the WindowAggregator
    5. Persists joined records and window results to DuckDB
    6. Emits watermarks to trigger window closures
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._config = config
        self._running = False
        self._loop_task: asyncio.Task | None = None
        self._watermark_task: asyncio.Task | None = None
        self._cleanup_task: asyncio.Task | None = None

        # Initialize components
        self.store = DuckDBStore(config.duckdb_path)
        self.watermark = WatermarkManager(
            allowed_lateness_sec=config.allowed_lateness_sec,
            idle_timeout_sec=config.idle_timeout_sec,
        )
        self.dimension_manager = DimensionTableManager(config.join_key_column)
        self.join_engine = JoinEngine(self.dimension_manager)
        self.window_aggregator = WindowAggregator(
            window_size_sec=config.window_size_sec,
            slide_interval_sec=config.slide_interval_sec,
        )
        self.ingestion = IngestionService(config)

        # Metrics
        self._events_processed = 0
        self._events_dropped = 0
        self._last_metrics_time = time.monotonic()

    async def start(self) -> None:
        """Start all pipeline components.

        Starts the Kafka consumer and launches the main processing loop.
        """
        logger.info("Pipeline starting...")

        try:
            await self.ingestion.start()
            logger.info("Ingestion service started")
        except Exception as e:
            logger.warning("Ingestion service failed to start (Kafka unavailable): %s", e)
            # Pipeline can still serve stored data without active ingestion

        self._running = True
        self._loop_task = asyncio.create_task(self._run_loop())
        self._watermark_task = asyncio.create_task(self._watermark_emitter())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Pipeline started successfully")

    async def _run_loop(self) -> None:
        """Main processing loop: consume → watermark → join → window → persist."""
        while self._running:
            try:
                # Consume a batch of events
                batch = await self.ingestion.consume_batch(max_records=1000, timeout_ms=100)

                if batch.num_rows == 0:
                    # No events; check idle watermark advancement
                    self.watermark.advance_on_idle()
                    await asyncio.sleep(0.01)
                    continue

                # Process each event through the pipeline
                for i in range(batch.num_rows):
                    event_time = batch.column("event_time")[i].as_py()
                    if event_time.tzinfo is None:
                        event_time = event_time.replace(tzinfo=timezone.utc)

                    # Watermark decision
                    decision = self.watermark.process_event(event_time)

                    if decision == WatermarkDecision.DROP:
                        self._events_dropped += 1
                        # Try as late event in closed windows
                        event_dict = self._batch_row_to_dict(batch, i)
                        joined = self.join_engine.join_single(event_dict)
                        correction = self.window_aggregator.add_late_event(joined, event_time)
                        if correction is not None:
                            self.store.persist_window_result(correction.corrected_result)
                        continue

                    # Join event with dimension table
                    event_dict = self._batch_row_to_dict(batch, i)
                    joined = self.join_engine.join_single(event_dict)

                    # Add to window aggregator
                    self.window_aggregator.add_event(joined, event_time)

                    # Persist joined record
                    self.store.persist_joined_record(joined)
                    self._events_processed += 1

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Pipeline loop error: %s", e, exc_info=True)
                await asyncio.sleep(0.1)

    async def _watermark_emitter(self) -> None:
        """Emit watermark at 1Hz to trigger window closures."""
        while self._running:
            try:
                wm_event = self.watermark.emit_watermark()
                if wm_event is not None:
                    # Close windows past the watermark
                    results = self.window_aggregator.on_watermark(wm_event.watermark)
                    for result in results:
                        self.store.persist_window_result(result)

                    # Update metrics for SSE
                    from olap_engine.api.app import update_metrics
                    update_metrics(
                        throughput=self.ingestion.get_throughput(),
                        watermark_lag=wm_event.lag_seconds,
                    )

                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Watermark emitter error: %s", e)
                await asyncio.sleep(1.0)

    async def _cleanup_loop(self) -> None:
        """Periodic cleanup of old data based on retention policy."""
        while self._running:
            try:
                await asyncio.sleep(3600)  # Run every hour
                self.store.cleanup_old_data(self._config.retention_days)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Cleanup error: %s", e)

    async def stop(self) -> None:
        """Graceful shutdown of all components."""
        logger.info("Pipeline stopping...")
        self._running = False

        # Cancel background tasks
        for task in [self._loop_task, self._watermark_task, self._cleanup_task]:
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Stop ingestion
        try:
            await self.ingestion.stop()
        except Exception as e:
            logger.warning("Error stopping ingestion: %s", e)

        # Close storage
        self.store.close()
        logger.info("Pipeline stopped")

    def get_metrics(self) -> dict:
        """Get current pipeline metrics."""
        return {
            "events_processed": self._events_processed,
            "events_dropped": self._events_dropped,
            "throughput": self.ingestion.get_throughput(),
            "watermark_lag": self.watermark.get_watermark_lag(),
            "active_windows": len(self.window_aggregator.get_active_windows()),
        }

    @staticmethod
    def _batch_row_to_dict(batch, row_idx: int) -> dict:
        """Convert a single row from an Arrow RecordBatch to a dict."""
        result = {}
        for col_name in batch.schema.names:
            value = batch.column(col_name)[row_idx].as_py()
            result[col_name] = value
        return result
