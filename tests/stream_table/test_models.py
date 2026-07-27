"""Unit tests for core data models and their validation rules."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from olap_engine.models import (
    DimensionRow,
    Event,
    QueryResult,
    QueryValidationResult,
    WindowCorrection,
    WindowResult,
    WindowState,
)


# ---------------------------------------------------------------------------
# Event model tests
# ---------------------------------------------------------------------------


class TestEvent:
    """Tests for Event validation rules."""

    def _make_event(self, **overrides):
        """Create a valid event with optional overrides."""
        now = datetime.now(timezone.utc)
        defaults = {
            "event_id": uuid4(),
            "event_time": now - timedelta(seconds=1),
            "ingest_time": now,
            "join_key": "user_123",
            "payload": {"amount": 42.0},
            "source_topic": "fact-events",
            "partition": 0,
            "offset": 100,
        }
        defaults.update(overrides)
        return Event(**defaults)

    def test_valid_event_creation(self):
        """A fully valid event should be created without error."""
        event = self._make_event()
        assert event.join_key == "user_123"
        assert event.partition == 0
        assert event.offset == 100

    def test_join_key_cannot_be_empty(self):
        """join_key must be a non-empty string."""
        with pytest.raises(ValueError, match="join_key must be a non-empty string"):
            self._make_event(join_key="")

    def test_partition_must_be_non_negative(self):
        """partition must be >= 0."""
        with pytest.raises(ValueError, match="partition must be >= 0"):
            self._make_event(partition=-1)

    def test_offset_must_be_non_negative(self):
        """offset must be >= 0."""
        with pytest.raises(ValueError, match="offset must be >= 0"):
            self._make_event(offset=-1)

    def test_event_time_cannot_be_far_in_future(self):
        """event_time must not be more than 5 minutes in the future."""
        future_time = datetime.now(timezone.utc) + timedelta(minutes=6)
        with pytest.raises(ValueError, match="event_time must not be more than 5 minutes"):
            self._make_event(event_time=future_time)

    def test_event_time_slightly_in_future_is_ok(self):
        """event_time within 5 minutes in the future is acceptable."""
        future_time = datetime.now(timezone.utc) + timedelta(minutes=4)
        event = self._make_event(event_time=future_time)
        assert event.event_time == future_time

    def test_event_is_frozen(self):
        """Event is immutable (frozen dataclass)."""
        event = self._make_event()
        with pytest.raises(AttributeError):
            event.join_key = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DimensionRow model tests
# ---------------------------------------------------------------------------


class TestDimensionRow:
    """Tests for DimensionRow validation rules."""

    def _make_row(self, **overrides):
        """Create a valid dimension row with optional overrides."""
        defaults = {
            "dimension_key": "product_42",
            "attributes": {"name": "Widget", "category": "gadgets"},
            "version": 1,
            "updated_at": datetime.now(timezone.utc),
            "is_active": True,
        }
        defaults.update(overrides)
        return DimensionRow(**defaults)

    def test_valid_dimension_row_creation(self):
        """A fully valid dimension row should be created without error."""
        row = self._make_row()
        assert row.dimension_key == "product_42"
        assert row.version == 1
        assert row.is_active is True

    def test_dimension_key_cannot_be_empty(self):
        """dimension_key must be a non-empty string."""
        with pytest.raises(ValueError, match="dimension_key must be a non-empty string"):
            self._make_row(dimension_key="")

    def test_version_must_be_at_least_one(self):
        """version must be >= 1."""
        with pytest.raises(ValueError, match="version must be >= 1"):
            self._make_row(version=0)

    def test_negative_version_rejected(self):
        """Negative version values are rejected."""
        with pytest.raises(ValueError, match="version must be >= 1"):
            self._make_row(version=-1)

    def test_attributes_must_be_dict(self):
        """attributes must be a dict."""
        with pytest.raises(TypeError, match="attributes must be a dict"):
            self._make_row(attributes="not a dict")

    def test_dimension_row_is_frozen(self):
        """DimensionRow is immutable."""
        row = self._make_row()
        with pytest.raises(AttributeError):
            row.dimension_key = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# WindowState model tests
# ---------------------------------------------------------------------------


class TestWindowState:
    """Tests for WindowState validation rules."""

    def _make_window(self, **overrides):
        """Create a valid window state with optional overrides."""
        now = datetime.now(timezone.utc)
        defaults = {
            "window_id": "metric_1000_1300",
            "window_start": now,
            "window_end": now + timedelta(minutes=5),
        }
        defaults.update(overrides)
        return WindowState(**defaults)

    def test_valid_window_state(self):
        """A valid window state should be created without error."""
        ws = self._make_window()
        assert ws.event_count == 0
        assert ws.is_closed is False
        assert ws.correction_count == 0

    def test_window_start_must_be_before_end(self):
        """window_start must be < window_end."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="window_start must be < window_end"):
            self._make_window(window_start=now, window_end=now - timedelta(seconds=1))

    def test_window_start_equals_end_rejected(self):
        """window_start == window_end is rejected."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="window_start must be < window_end"):
            self._make_window(window_start=now, window_end=now)

    def test_event_count_cannot_be_negative(self):
        """event_count must be >= 0."""
        with pytest.raises(ValueError, match="event_count must be >= 0"):
            self._make_window(event_count=-1)

    def test_correction_count_cannot_be_negative(self):
        """correction_count must be >= 0."""
        with pytest.raises(ValueError, match="correction_count must be >= 0"):
            self._make_window(correction_count=-1)

    def test_closed_window_must_have_last_emitted_at(self):
        """If is_closed is True, last_emitted_at must not be None."""
        with pytest.raises(ValueError, match="last_emitted_at must not be None"):
            self._make_window(is_closed=True, last_emitted_at=None)

    def test_closed_window_with_emitted_at_is_ok(self):
        """A closed window with a last_emitted_at value is valid."""
        now = datetime.now(timezone.utc)
        ws = self._make_window(is_closed=True, last_emitted_at=now)
        assert ws.is_closed is True
        assert ws.last_emitted_at == now

    def test_window_state_is_mutable(self):
        """WindowState is mutable (not frozen) for incremental updates."""
        ws = self._make_window()
        ws.event_count = 10
        assert ws.event_count == 10


# ---------------------------------------------------------------------------
# WindowResult model tests
# ---------------------------------------------------------------------------


class TestWindowResult:
    """Tests for WindowResult."""

    def test_valid_window_result(self):
        """A valid window result should be created without error."""
        now = datetime.now(timezone.utc)
        result = WindowResult(
            window_id="metric_1000_1300",
            window_start=now,
            window_end=now + timedelta(minutes=5),
            aggregations={"sum_amount": 1000.0, "count": 50.0},
            event_count=50,
            is_correction=False,
            correction_version=0,
            emitted_at=now,
        )
        assert result.event_count == 50
        assert result.is_correction is False

    def test_window_result_is_frozen(self):
        """WindowResult is immutable."""
        now = datetime.now(timezone.utc)
        result = WindowResult(
            window_id="w1",
            window_start=now,
            window_end=now + timedelta(minutes=5),
            aggregations={},
            event_count=0,
            is_correction=False,
            correction_version=0,
            emitted_at=now,
        )
        with pytest.raises(AttributeError):
            result.event_count = 99  # type: ignore[misc]


# ---------------------------------------------------------------------------
# WindowCorrection model tests
# ---------------------------------------------------------------------------


class TestWindowCorrection:
    """Tests for WindowCorrection."""

    def test_valid_correction(self):
        """A valid correction pairs previous and corrected results."""
        now = datetime.now(timezone.utc)
        prev = WindowResult(
            window_id="w1",
            window_start=now,
            window_end=now + timedelta(minutes=5),
            aggregations={"sum": 100.0},
            event_count=10,
            is_correction=False,
            correction_version=0,
            emitted_at=now,
        )
        corrected = WindowResult(
            window_id="w1",
            window_start=now,
            window_end=now + timedelta(minutes=5),
            aggregations={"sum": 150.0},
            event_count=15,
            is_correction=True,
            correction_version=1,
            emitted_at=now + timedelta(seconds=5),
        )
        correction = WindowCorrection(
            window_id="w1", previous_result=prev, corrected_result=corrected
        )
        assert correction.previous_result.event_count == 10
        assert correction.corrected_result.event_count == 15


# ---------------------------------------------------------------------------
# QueryResult model tests
# ---------------------------------------------------------------------------


class TestQueryResult:
    """Tests for QueryResult."""

    def test_valid_query_result(self):
        """A valid query result should be created without error."""
        result = QueryResult(
            query_id=uuid4(),
            columns=["user_id", "amount"],
            rows=[["user_1", 42.0], ["user_2", 99.0]],
            row_count=2,
            execution_time_ms=12.5,
        )
        assert result.row_count == 2
        assert result.is_partial is False
        assert result.truncated is False
        assert result.total_available is None

    def test_query_result_truncated(self):
        """A truncated result reports total available rows."""
        result = QueryResult(
            query_id=uuid4(),
            columns=["id"],
            rows=[],
            row_count=0,
            execution_time_ms=5.0,
            truncated=True,
            total_available=50000,
        )
        assert result.truncated is True
        assert result.total_available == 50000

    def test_query_result_is_frozen(self):
        """QueryResult is immutable."""
        result = QueryResult(
            query_id=uuid4(),
            columns=[],
            rows=[],
            row_count=0,
            execution_time_ms=0.0,
        )
        with pytest.raises(AttributeError):
            result.row_count = 999  # type: ignore[misc]


# ---------------------------------------------------------------------------
# QueryValidationResult model tests
# ---------------------------------------------------------------------------


class TestQueryValidationResult:
    """Tests for QueryValidationResult."""

    def test_valid_query(self):
        """A valid query validation result."""
        result = QueryValidationResult(is_valid=True)
        assert result.is_valid is True
        assert result.error_message is None

    def test_invalid_query_with_location(self):
        """An invalid query carries error details."""
        result = QueryValidationResult(
            is_valid=False,
            error_message="Unexpected token near 'SELCT'",
            error_line=1,
            error_column=1,
        )
        assert result.is_valid is False
        assert result.error_line == 1
        assert result.error_column == 1
