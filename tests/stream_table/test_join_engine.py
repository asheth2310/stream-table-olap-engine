"""Unit tests for JoinEngine vectorized stream-table join."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa
import pytest

from olap_engine.join.dimension_table import DimensionTableManager
from olap_engine.join.engine import JoinEngine
from olap_engine.schemas.event_schema import FACT_EVENT_SCHEMA


def _make_fact_batch(events: list[dict]) -> pa.RecordBatch:
    """Helper to create a fact event RecordBatch from a list of dicts."""
    arrays = {
        "event_id": pa.array([e["event_id"] for e in events], type=pa.string()),
        "event_time": pa.array(
            [e["event_time"] for e in events],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "ingest_time": pa.array(
            [e["ingest_time"] for e in events],
            type=pa.timestamp("us", tz="UTC"),
        ),
        "join_key": pa.array([e["join_key"] for e in events], type=pa.string()),
        "payload_json": pa.array(
            [e["payload_json"] for e in events], type=pa.string()
        ),
        "source_topic": pa.array(
            [e["source_topic"] for e in events], type=pa.string()
        ),
        "partition": pa.array([e["partition"] for e in events], type=pa.int32()),
        "offset": pa.array([e["offset"] for e in events], type=pa.int64()),
    }
    return pa.RecordBatch.from_pydict(arrays, schema=FACT_EVENT_SCHEMA)


def _make_event(
    event_id: str = "evt-1",
    join_key: str = "u1",
    offset: int = 0,
) -> dict:
    """Helper to create a single fact event dict."""
    now = datetime.now(timezone.utc)
    return {
        "event_id": event_id,
        "event_time": now,
        "ingest_time": now,
        "join_key": join_key,
        "payload_json": json.dumps({"amount": 42.0}),
        "source_topic": "fact-events",
        "partition": 0,
        "offset": offset,
    }


@pytest.fixture
def dimension_manager() -> DimensionTableManager:
    """Create a DimensionTableManager with sample data."""
    manager = DimensionTableManager(join_key="user_id")
    manager.load_initial([
        {"user_id": "u1", "name": "Alice", "region": "US"},
        {"user_id": "u2", "name": "Bob", "region": "EU"},
        {"user_id": "u3", "name": "Charlie", "region": "APAC"},
    ])
    return manager


@pytest.fixture
def empty_dimension_manager() -> DimensionTableManager:
    """Create an empty DimensionTableManager."""
    manager = DimensionTableManager(join_key="user_id")
    manager.load_initial([])
    return manager


@pytest.fixture
def join_engine(dimension_manager: DimensionTableManager) -> JoinEngine:
    """Create a JoinEngine with sample dimension data."""
    return JoinEngine(dimension_manager)


@pytest.fixture
def empty_join_engine(empty_dimension_manager: DimensionTableManager) -> JoinEngine:
    """Create a JoinEngine with no dimension data."""
    return JoinEngine(empty_dimension_manager)


class TestJoinBatch:
    """Tests for join_batch method."""

    def test_join_batch_all_keys_match(self, join_engine: JoinEngine) -> None:
        """All fact events have matching dimension keys → enriched output."""
        events = [
            _make_event(event_id="e1", join_key="u1", offset=0),
            _make_event(event_id="e2", join_key="u2", offset=1),
        ]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)

        assert isinstance(result, pa.RecordBatch)
        assert result.num_rows == 2

        # Should have fact columns + dimension attribute columns
        col_names = result.schema.names
        assert "join_key" in col_names
        assert "event_id" in col_names
        assert "name" in col_names
        assert "region" in col_names

        # Check values
        result_df = pl.from_arrow(result)
        row_u1 = result_df.filter(pl.col("join_key") == "u1")
        assert row_u1["name"][0] == "Alice"
        assert row_u1["region"][0] == "US"

        row_u2 = result_df.filter(pl.col("join_key") == "u2")
        assert row_u2["name"][0] == "Bob"
        assert row_u2["region"][0] == "EU"

    def test_join_batch_unmatched_keys_get_nulls(self, join_engine: JoinEngine) -> None:
        """Unmatched keys produce null-filled dimension fields (left outer join)."""
        events = [
            _make_event(event_id="e1", join_key="u1", offset=0),
            _make_event(event_id="e2", join_key="unknown_key", offset=1),
        ]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)

        assert result.num_rows == 2
        result_df = pl.from_arrow(result)

        # Matched row should have values
        row_u1 = result_df.filter(pl.col("join_key") == "u1")
        assert row_u1["name"][0] == "Alice"

        # Unmatched row should have nulls
        row_unknown = result_df.filter(pl.col("join_key") == "unknown_key")
        assert row_unknown["name"][0] is None
        assert row_unknown["region"][0] is None

    def test_join_batch_preserves_all_fact_columns(self, join_engine: JoinEngine) -> None:
        """All original fact columns are present in the output."""
        events = [_make_event(event_id="e1", join_key="u1", offset=42)]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)

        col_names = result.schema.names
        for fact_col in FACT_EVENT_SCHEMA.names:
            assert fact_col in col_names

    def test_join_batch_preserves_row_count(self, join_engine: JoinEngine) -> None:
        """Output row count equals input row count (left join invariant)."""
        events = [
            _make_event(event_id=f"e{i}", join_key=f"u{i % 5}", offset=i)
            for i in range(10)
        ]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)
        assert result.num_rows == 10

    def test_join_batch_empty_dimension_table(self, empty_join_engine: JoinEngine) -> None:
        """With an empty dimension table, output equals input (no extra columns)."""
        events = [_make_event(event_id="e1", join_key="u1", offset=0)]
        batch = _make_fact_batch(events)

        result = empty_join_engine.join_batch(batch)

        assert result.num_rows == 1
        # Should still have all fact columns
        for fact_col in FACT_EVENT_SCHEMA.names:
            assert fact_col in result.schema.names

    def test_join_batch_single_event(self, join_engine: JoinEngine) -> None:
        """Single-event batch works correctly."""
        events = [_make_event(event_id="e1", join_key="u3", offset=0)]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)

        assert result.num_rows == 1
        result_df = pl.from_arrow(result)
        assert result_df["name"][0] == "Charlie"
        assert result_df["region"][0] == "APAC"

    def test_join_batch_updates_throughput(self, join_engine: JoinEngine) -> None:
        """join_batch updates the throughput metric."""
        events = [_make_event(event_id=f"e{i}", join_key="u1", offset=i) for i in range(100)]
        batch = _make_fact_batch(events)

        join_engine.join_batch(batch)

        # Throughput should be > 0 after processing
        assert join_engine.get_join_throughput() > 0


class TestJoinSingle:
    """Tests for join_single method."""

    def test_join_single_matching_key(self, join_engine: JoinEngine) -> None:
        """Single event with matching key gets enriched."""
        event = {"join_key": "u1", "event_id": "e1", "amount": 100.0}

        result = join_engine.join_single(event)

        assert result["event_id"] == "e1"
        assert result["amount"] == 100.0
        assert result["name"] == "Alice"
        assert result["region"] == "US"

    def test_join_single_no_match_returns_nulls(self, join_engine: JoinEngine) -> None:
        """Single event with no matching key gets null dimension attributes."""
        event = {"join_key": "nonexistent", "event_id": "e1"}

        result = join_engine.join_single(event)

        assert result["event_id"] == "e1"
        assert result["join_key"] == "nonexistent"
        assert result["name"] is None
        assert result["region"] is None

    def test_join_single_preserves_all_event_fields(self, join_engine: JoinEngine) -> None:
        """All original event fields are preserved in the output."""
        event = {
            "join_key": "u2",
            "event_id": "e1",
            "custom_field": "value",
            "nested": {"a": 1},
        }

        result = join_engine.join_single(event)

        assert result["custom_field"] == "value"
        assert result["nested"] == {"a": 1}
        assert result["name"] == "Bob"

    def test_join_single_empty_dimension_table(
        self, empty_join_engine: JoinEngine
    ) -> None:
        """With empty dimension table, event is returned unchanged."""
        event = {"join_key": "u1", "event_id": "e1"}

        result = empty_join_engine.join_single(event)

        assert result == {"join_key": "u1", "event_id": "e1"}

    def test_join_single_after_dimension_update(
        self, dimension_manager: DimensionTableManager
    ) -> None:
        """Join reflects dimension table updates."""
        engine = JoinEngine(dimension_manager)

        # Before update
        result = engine.join_single({"join_key": "u1", "event_id": "e1"})
        assert result["name"] == "Alice"

        # Update dimension
        dimension_manager.update_row("u1", {"name": "Alice Updated", "region": "CA"}, version=2)

        # After update
        result = engine.join_single({"join_key": "u1", "event_id": "e2"})
        assert result["name"] == "Alice Updated"
        assert result["region"] == "CA"

    def test_join_single_after_dimension_delete(
        self, dimension_manager: DimensionTableManager
    ) -> None:
        """Join returns nulls after dimension row is soft-deleted."""
        engine = JoinEngine(dimension_manager)

        # Delete dimension row
        dimension_manager.delete_row("u1")

        result = engine.join_single({"join_key": "u1", "event_id": "e1"})
        assert result["name"] is None
        assert result["region"] is None


class TestGetJoinThroughput:
    """Tests for get_join_throughput method."""

    def test_initial_throughput_is_zero(self, join_engine: JoinEngine) -> None:
        """Throughput starts at zero before any joins."""
        assert join_engine.get_join_throughput() == 0.0

    def test_throughput_after_batch(self, join_engine: JoinEngine) -> None:
        """Throughput is positive after processing a batch."""
        events = [_make_event(event_id=f"e{i}", join_key="u1", offset=i) for i in range(50)]
        batch = _make_fact_batch(events)

        join_engine.join_batch(batch)

        assert join_engine.get_join_throughput() > 0.0


class TestLeftOuterJoinSemantics:
    """Integration tests verifying left outer join contract."""

    def test_mixed_matched_and_unmatched_preserves_order(
        self, join_engine: JoinEngine
    ) -> None:
        """Output preserves input order with mixed matched/unmatched keys."""
        events = [
            _make_event(event_id="e1", join_key="u1", offset=0),
            _make_event(event_id="e2", join_key="missing", offset=1),
            _make_event(event_id="e3", join_key="u2", offset=2),
            _make_event(event_id="e4", join_key="also_missing", offset=3),
        ]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)
        result_df = pl.from_arrow(result)

        assert result.num_rows == 4
        # Verify matched rows have data
        assert result_df.filter(pl.col("event_id") == "e1")["name"][0] == "Alice"
        assert result_df.filter(pl.col("event_id") == "e3")["name"][0] == "Bob"
        # Verify unmatched rows have nulls
        assert result_df.filter(pl.col("event_id") == "e2")["name"][0] is None
        assert result_df.filter(pl.col("event_id") == "e4")["name"][0] is None

    def test_duplicate_join_keys_in_fact_batch(self, join_engine: JoinEngine) -> None:
        """Multiple fact events with the same join key all get enriched."""
        events = [
            _make_event(event_id="e1", join_key="u1", offset=0),
            _make_event(event_id="e2", join_key="u1", offset=1),
            _make_event(event_id="e3", join_key="u1", offset=2),
        ]
        batch = _make_fact_batch(events)

        result = join_engine.join_batch(batch)
        result_df = pl.from_arrow(result)

        assert result.num_rows == 3
        # All should have Alice's data
        names = result_df["name"].to_list()
        assert all(n == "Alice" for n in names)
