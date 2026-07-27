"""Tests for the Pipeline orchestration."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from olap_engine.config.pipeline_config import PipelineConfig
from olap_engine.pipeline import Pipeline


@pytest.fixture
def config(tmp_path):
    """Create a test pipeline config with temp DuckDB path."""
    return PipelineConfig(
        kafka_bootstrap_servers="localhost:9092",
        fact_topic="test-events",
        dimension_topic="test-dimensions",
        dead_letter_topic="test-dlq",
        consumer_group="test-group",
        duckdb_path=str(tmp_path / "test_pipeline.duckdb"),
        window_size_sec=60,
        slide_interval_sec=10,
    )


class TestPipelineInit:
    """Test pipeline initialization."""

    def test_pipeline_creates_components(self, config):
        """Pipeline should create all components on init."""
        pipeline = Pipeline(config)

        assert pipeline.store is not None
        assert pipeline.watermark is not None
        assert pipeline.join_engine is not None
        assert pipeline.window_aggregator is not None
        assert pipeline.ingestion is not None
        pipeline.store.close()

    def test_pipeline_creates_duckdb(self, config, tmp_path):
        """Pipeline should create DuckDB tables on init."""
        pipeline = Pipeline(config)

        # Tables should exist
        result = pipeline.store.execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
        table_names = {r["table_name"] for r in result}
        assert "joined_events" in table_names
        assert "window_results" in table_names
        pipeline.store.close()


class TestPipelineMetrics:
    """Test pipeline metrics."""

    def test_get_metrics(self, config):
        """Metrics should return expected fields."""
        pipeline = Pipeline(config)
        metrics = pipeline.get_metrics()

        assert "events_processed" in metrics
        assert "events_dropped" in metrics
        assert "throughput" in metrics
        assert "watermark_lag" in metrics
        assert "active_windows" in metrics
        assert metrics["events_processed"] == 0
        pipeline.store.close()


class TestPipelineStartStop:
    """Test pipeline start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_without_kafka(self, config):
        """Pipeline should start even if Kafka is unavailable (graceful degradation)."""
        pipeline = Pipeline(config)

        # Start should not raise even without Kafka
        # We patch the ingestion start to simulate Kafka being down
        with patch.object(pipeline.ingestion, "start", new_callable=AsyncMock) as mock_start:
            mock_start.side_effect = Exception("Connection refused")
            await pipeline.start()

        # Pipeline should still be running (degraded mode)
        assert pipeline._running is True

        await pipeline.stop()
        assert pipeline._running is False

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks(self, config):
        """Stop should cancel background tasks."""
        pipeline = Pipeline(config)

        with patch.object(pipeline.ingestion, "start", new_callable=AsyncMock):
            with patch.object(pipeline.ingestion, "stop", new_callable=AsyncMock):
                await pipeline.start()
                assert pipeline._loop_task is not None
                assert pipeline._watermark_task is not None

                await pipeline.stop()
                # Tasks should be cancelled
                assert pipeline._loop_task.done()
                assert pipeline._watermark_task.done()


class TestBatchRowToDict:
    """Test Arrow RecordBatch row extraction."""

    def test_batch_row_to_dict(self):
        """Should correctly extract a row from an Arrow batch."""
        import pyarrow as pa

        batch = pa.RecordBatch.from_pydict({
            "event_id": ["e1", "e2"],
            "join_key": ["k1", "k2"],
            "value": [10, 20],
        })

        row0 = Pipeline._batch_row_to_dict(batch, 0)
        assert row0 == {"event_id": "e1", "join_key": "k1", "value": 10}

        row1 = Pipeline._batch_row_to_dict(batch, 1)
        assert row1 == {"event_id": "e2", "join_key": "k2", "value": 20}
