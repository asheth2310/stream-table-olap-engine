"""Tests for DuckDB storage layer."""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone

import pytest

from olap_engine.models.window import WindowResult
from olap_engine.storage.duckdb_store import DuckDBStore


@pytest.fixture
def store(tmp_path):
    """Create a DuckDBStore with a temporary database file."""
    db_path = str(tmp_path / "test.duckdb")
    s = DuckDBStore(db_path=db_path)
    yield s
    s.close()


class TestDuckDBMigrations:
    """Test schema migrations run correctly on startup."""

    def test_tables_created_on_init(self, store: DuckDBStore):
        """Tables should exist after initialization."""
        result = store.execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
        table_names = {r["table_name"] for r in result}
        assert "joined_events" in table_names
        assert "window_results" in table_names

    def test_idempotent_migrations(self, tmp_path):
        """Running migrations twice should not fail."""
        db_path = str(tmp_path / "test2.duckdb")
        store1 = DuckDBStore(db_path=db_path)
        store1.close()
        # Re-open same DB — migrations run again
        store2 = DuckDBStore(db_path=db_path)
        result = store2.execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        )
        table_names = {r["table_name"] for r in result}
        assert "joined_events" in table_names
        assert "window_results" in table_names
        store2.close()


class TestPersistJoinedRecord:
    """Test writing joined events."""

    def test_persist_and_query(self, store: DuckDBStore):
        """A persisted record should be queryable."""
        now = datetime.now(timezone.utc)
        record = {
            "event_id": "evt-001",
            "event_time": now,
            "ingest_time": now,
            "join_key": "user_123",
            "payload_json": '{"amount": 99.5}',
            "source_topic": "fact-events",
            "partition": 0,
            "offset": 42,
            "region": "us-east",  # dimension attribute
        }

        store.persist_joined_record(record)

        results = store.execute_query("SELECT * FROM joined_events WHERE event_id = 'evt-001'")
        assert len(results) == 1
        assert results[0]["join_key"] == "user_123"
        assert results[0]["partition_id"] == 0
        assert results[0]["offset_id"] == 42

    def test_dimension_attrs_stored(self, store: DuckDBStore):
        """Dimension attributes should be stored in dimension_attrs_json."""
        now = datetime.now(timezone.utc)
        record = {
            "event_id": "evt-002",
            "event_time": now,
            "ingest_time": now,
            "join_key": "user_456",
            "payload_json": "{}",
            "source_topic": "fact-events",
            "partition": 1,
            "offset": 10,
            "tier": "premium",
            "country": "US",
        }

        store.persist_joined_record(record)
        results = store.execute_query("SELECT dimension_attrs_json FROM joined_events WHERE event_id = 'evt-002'")
        import json
        attrs = json.loads(results[0]["dimension_attrs_json"])
        assert attrs["tier"] == "premium"
        assert attrs["country"] == "US"


class TestPersistWindowResult:
    """Test writing window results with deduplication."""

    def _make_result(self, window_id="w1", correction_version=0, event_count=10):
        now = datetime.now(timezone.utc)
        return WindowResult(
            window_id=window_id,
            window_start=now - timedelta(minutes=5),
            window_end=now,
            aggregations={"count": event_count, "amount_sum": 500.0},
            event_count=event_count,
            is_correction=correction_version > 0,
            correction_version=correction_version,
            emitted_at=now,
        )

    def test_persist_window_result(self, store: DuckDBStore):
        """A window result should be persisted and queryable."""
        result = self._make_result()
        store.persist_window_result(result)

        rows = store.execute_query("SELECT * FROM window_results WHERE window_id = 'w1'")
        assert len(rows) == 1
        assert rows[0]["event_count"] == 10

    def test_correction_deduplication(self, store: DuckDBStore):
        """A correction should replace the previous version."""
        result_v0 = self._make_result(correction_version=0, event_count=10)
        store.persist_window_result(result_v0)

        # Persist correction v0 again (upsert)
        result_v0_updated = self._make_result(correction_version=0, event_count=15)
        store.persist_window_result(result_v0_updated)

        rows = store.execute_query(
            "SELECT * FROM window_results WHERE window_id = 'w1' AND correction_version = 0"
        )
        assert len(rows) == 1
        assert rows[0]["event_count"] == 15

    def test_multiple_correction_versions(self, store: DuckDBStore):
        """Multiple correction versions should coexist."""
        store.persist_window_result(self._make_result(correction_version=0, event_count=10))
        store.persist_window_result(self._make_result(correction_version=1, event_count=12))
        store.persist_window_result(self._make_result(correction_version=2, event_count=14))

        rows = store.execute_query(
            "SELECT * FROM window_results WHERE window_id = 'w1' ORDER BY correction_version"
        )
        assert len(rows) == 3
        assert rows[0]["event_count"] == 10
        assert rows[2]["event_count"] == 14


class TestExecuteQuery:
    """Test analytical query execution."""

    def test_simple_select(self, store: DuckDBStore):
        """Simple SELECT should work."""
        results = store.execute_query("SELECT 1 AS val")
        assert results == [{"val": 1}]

    def test_query_with_params(self, store: DuckDBStore):
        """Parameterized queries should work."""
        results = store.execute_query("SELECT ? AS val", [42])
        assert results[0]["val"] == 42

    def test_invalid_query_raises(self, store: DuckDBStore):
        """Invalid SQL should raise an error."""
        with pytest.raises(Exception):
            store.execute_query("INVALID SQL NONSENSE")


class TestGetWindowAtTime:
    """Test time-based window queries."""

    def test_window_at_time(self, store: DuckDBStore):
        """Should return windows containing the given timestamp."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=5)
        end = now

        result = WindowResult(
            window_id="w_time_test",
            window_start=start,
            window_end=end,
            aggregations={"count": 100.0},
            event_count=100,
            is_correction=False,
            correction_version=0,
            emitted_at=now,
        )
        store.persist_window_result(result)

        # Query a time in the middle of the window
        mid = start + timedelta(minutes=2)
        windows = store.get_window_at_time(mid)
        assert len(windows) >= 1
        assert any(w["window_id"] == "w_time_test" for w in windows)


class TestCleanup:
    """Test data retention cleanup."""

    def test_cleanup_removes_old_data(self, store: DuckDBStore):
        """Data older than retention should be deleted."""
        old_time = datetime.now(timezone.utc) - timedelta(days=10)

        # Insert old event
        store.persist_joined_record({
            "event_id": "old-evt",
            "event_time": old_time,
            "ingest_time": old_time,
            "join_key": "user_old",
            "payload_json": "{}",
            "source_topic": "fact-events",
            "partition": 0,
            "offset": 0,
        })

        # Insert recent event
        now = datetime.now(timezone.utc)
        store.persist_joined_record({
            "event_id": "new-evt",
            "event_time": now,
            "ingest_time": now,
            "join_key": "user_new",
            "payload_json": "{}",
            "source_topic": "fact-events",
            "partition": 0,
            "offset": 1,
        })

        deleted = store.cleanup_old_data(retention_days=7)
        assert deleted >= 1

        # Old event should be gone, new event should remain
        results = store.execute_query("SELECT event_id FROM joined_events")
        event_ids = [r["event_id"] for r in results]
        assert "old-evt" not in event_ids
        assert "new-evt" in event_ids
