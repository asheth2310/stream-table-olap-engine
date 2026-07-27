"""Unit tests for the WindowAggregator class."""

from datetime import datetime, timedelta, timezone

import pytest

from olap_engine.models.window import WindowCorrection, WindowResult, WindowState
from olap_engine.window import WindowAggregator


class TestWindowAggregatorInit:
    """Tests for WindowAggregator initialization."""

    def test_default_initialization(self):
        agg = WindowAggregator()
        assert agg.get_active_windows() == []

    def test_custom_window_size(self):
        agg = WindowAggregator(window_size_sec=60, slide_interval_sec=10)
        assert agg._window_size == timedelta(seconds=60)
        assert agg._slide_interval == timedelta(seconds=10)

    def test_invalid_window_size_zero(self):
        with pytest.raises(ValueError, match="window_size_sec must be > 0"):
            WindowAggregator(window_size_sec=0)

    def test_invalid_slide_interval_zero(self):
        with pytest.raises(ValueError, match="slide_interval_sec must be > 0"):
            WindowAggregator(slide_interval_sec=0)

    def test_window_size_less_than_slide(self):
        with pytest.raises(ValueError, match="window_size_sec.*must be >= slide_interval_sec"):
            WindowAggregator(window_size_sec=5, slide_interval_sec=10)


class TestAddEvent:
    """Tests for WindowAggregator.add_event()."""

    def test_single_event_creates_windows(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=5)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        event = {"value": 42.0, "name": "test"}
        agg.add_event(event, event_time)
        # Should be in at least one window
        assert len(agg.get_active_windows()) >= 1

    def test_event_updates_count(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, event_time)
        agg.add_event({"value": 2.0}, event_time)
        windows = agg.get_active_windows()
        assert len(windows) >= 1
        window = agg._active_windows[windows[0]]
        assert window.event_count == 2

    def test_sum_aggregation(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"amount": 10.0}, event_time)
        agg.add_event({"amount": 20.0}, event_time)
        agg.add_event({"amount": 30.0}, event_time)
        windows = agg.get_active_windows()
        window = agg._active_windows[windows[0]]
        assert window.sum_values["amount"] == 60.0

    def test_min_aggregation(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"amount": 30.0}, event_time)
        agg.add_event({"amount": 10.0}, event_time)
        agg.add_event({"amount": 20.0}, event_time)
        windows = agg.get_active_windows()
        window = agg._active_windows[windows[0]]
        assert window.min_values["amount"] == 10.0

    def test_max_aggregation(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"amount": 30.0}, event_time)
        agg.add_event({"amount": 10.0}, event_time)
        agg.add_event({"amount": 50.0}, event_time)
        windows = agg.get_active_windows()
        window = agg._active_windows[windows[0]]
        assert window.max_values["amount"] == 50.0

    def test_avg_accumulator(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"amount": 10.0}, event_time)
        agg.add_event({"amount": 20.0}, event_time)
        agg.add_event({"amount": 30.0}, event_time)
        windows = agg.get_active_windows()
        window = agg._active_windows[windows[0]]
        total, count = window.avg_accumulators["amount"]
        assert total == 60.0
        assert count == 3
        assert total / count == 20.0

    def test_multiple_numeric_fields(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"price": 100.0, "quantity": 5}, event_time)
        agg.add_event({"price": 200.0, "quantity": 3}, event_time)
        windows = agg.get_active_windows()
        window = agg._active_windows[windows[0]]
        assert window.sum_values["price"] == 300.0
        assert window.sum_values["quantity"] == 8.0

    def test_non_numeric_fields_ignored(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"name": "test", "active": True, "value": 42.0}, event_time)
        windows = agg.get_active_windows()
        window = agg._active_windows[windows[0]]
        assert "name" not in window.sum_values
        assert "active" not in window.sum_values
        assert "value" in window.sum_values

    def test_overlapping_windows(self):
        # 10-second window, 5-second slide => 2 overlapping windows
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=5)
        # Event at exactly second 7 should be in two windows
        event_time = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, event_time)
        # Should be in at least 2 windows (window_size/slide = 10/5 = 2)
        assert len(agg.get_active_windows()) == 2

    def test_naive_datetime_treated_as_utc(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5)  # naive
        agg.add_event({"value": 1.0}, event_time)
        assert len(agg.get_active_windows()) >= 1


class TestOnWatermark:
    """Tests for WindowAggregator.on_watermark()."""

    def test_closes_windows_past_watermark(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 42.0}, event_time)
        assert len(agg.get_active_windows()) >= 1

        # Watermark past window end closes it
        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert len(results) >= 1
        assert all(isinstance(r, WindowResult) for r in results)
        assert agg.get_active_windows() == []

    def test_returns_empty_when_no_windows_to_close(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 42.0}, event_time)

        # Watermark before window end - nothing to close
        watermark = datetime(2024, 1, 1, 12, 0, 3, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert results == []

    def test_window_result_contains_aggregations(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)
        agg.add_event({"value": 20.0}, event_time)

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert len(results) >= 1
        result = results[0]
        assert result.event_count == 2
        assert result.aggregations["value_sum"] == 30.0
        assert result.aggregations["count"] == 2.0

    def test_result_is_not_correction(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert results[0].is_correction is False
        assert results[0].correction_version == 0

    def test_closed_windows_moved_to_closed_dict(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)
        active_before = agg.get_active_windows()
        assert len(active_before) >= 1

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)
        assert len(agg._closed_windows) >= 1
        assert agg.get_active_windows() == []

    def test_watermark_at_exactly_window_end_closes(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        # Window [0, 10) - event at t=5
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        # Watermark exactly at window_end (10 seconds into the epoch-aligned minute)
        watermark = datetime(2024, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert len(results) >= 1

    def test_multiple_windows_closed_at_once(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        # Add events to two different windows
        t1 = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, t1)
        agg.add_event({"value": 2.0}, t2)

        # Watermark past both windows
        watermark = datetime(2024, 1, 1, 12, 0, 25, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert len(results) == 2


class TestAddLateEvent:
    """Tests for WindowAggregator.add_late_event()."""

    def test_late_event_returns_correction(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        # Close the window
        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)

        # Add late event to the same window
        late_event_time = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        correction = agg.add_late_event({"value": 5.0}, late_event_time)
        assert correction is not None
        assert isinstance(correction, WindowCorrection)

    def test_late_event_updates_aggregations(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)

        late_event_time = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        correction = agg.add_late_event({"value": 5.0}, late_event_time)
        assert correction.corrected_result.aggregations["value_sum"] == 15.0
        assert correction.corrected_result.event_count == 2

    def test_late_event_increments_correction_version(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)

        late_time = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        correction1 = agg.add_late_event({"value": 5.0}, late_time)
        assert correction1.corrected_result.correction_version == 1

        correction2 = agg.add_late_event({"value": 3.0}, late_time)
        assert correction2.corrected_result.correction_version == 2

    def test_late_event_no_matching_window_returns_none(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)

        # Event for a completely different time range - no closed window exists
        unrelated_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        correction = agg.add_late_event({"value": 5.0}, unrelated_time)
        assert correction is None

    def test_late_event_previous_result_reflects_state_before(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)

        late_time = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        correction = agg.add_late_event({"value": 5.0}, late_time)
        # Previous result has only the original event
        assert correction.previous_result.event_count == 1
        assert correction.previous_result.aggregations["value_sum"] == 10.0


class TestGetPartialResult:
    """Tests for WindowAggregator.get_partial_result()."""

    def test_returns_partial_for_active_window(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)
        agg.add_event({"value": 20.0}, event_time)

        windows = agg.get_active_windows()
        result = agg.get_partial_result(windows[0])
        assert result is not None
        assert isinstance(result, WindowResult)
        assert result.event_count == 2
        assert result.aggregations["value_sum"] == 30.0

    def test_returns_none_for_unknown_window(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        result = agg.get_partial_result("nonexistent_window")
        assert result is None

    def test_returns_none_for_closed_window(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)
        windows = agg.get_active_windows()
        window_id = windows[0]

        # Close the window
        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)

        # Should not return partial result for closed window
        result = agg.get_partial_result(window_id)
        assert result is None


class TestGetActiveWindows:
    """Tests for WindowAggregator.get_active_windows()."""

    def test_empty_when_no_events(self):
        agg = WindowAggregator()
        assert agg.get_active_windows() == []

    def test_returns_window_ids_after_events(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, event_time)
        windows = agg.get_active_windows()
        assert len(windows) >= 1
        assert all(isinstance(w, str) for w in windows)

    def test_windows_decrease_after_closure(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, event_time)
        active_before = len(agg.get_active_windows())

        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        agg.on_watermark(watermark)
        active_after = len(agg.get_active_windows())
        assert active_after < active_before


class TestComputeWindowResult:
    """Tests for WindowAggregator._compute_window_result()."""

    def test_avg_computation(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, event_time)
        agg.add_event({"value": 20.0}, event_time)
        agg.add_event({"value": 30.0}, event_time)

        windows = agg.get_active_windows()
        result = agg.get_partial_result(windows[0])
        assert result.aggregations["value_avg"] == 20.0

    def test_min_max_in_result(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 5.0}, event_time)
        agg.add_event({"value": 50.0}, event_time)
        agg.add_event({"value": 25.0}, event_time)

        windows = agg.get_active_windows()
        result = agg.get_partial_result(windows[0])
        assert result.aggregations["value_min"] == 5.0
        assert result.aggregations["value_max"] == 50.0

    def test_empty_window_result(self):
        """A window with no events still produces a valid result."""
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        # Add and then test an event with no numeric fields
        agg.add_event({"name": "test"}, event_time)
        windows = agg.get_active_windows()
        result = agg.get_partial_result(windows[0])
        assert result.event_count == 1
        assert result.aggregations["count"] == 1.0


class TestSlidingWindowBehavior:
    """Integration tests for sliding window behavior."""

    def test_300_overlapping_windows_default_config(self):
        """With 300s window and 1s slide, an event belongs to up to 300 windows."""
        agg = WindowAggregator(window_size_sec=300, slide_interval_sec=1)
        event_time = datetime(2024, 1, 1, 12, 2, 30, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, event_time)
        # Should be in up to 300 windows
        active = agg.get_active_windows()
        assert len(active) == 300

    def test_events_at_different_times_in_overlapping_windows(self):
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=5)
        t1 = datetime(2024, 1, 1, 12, 0, 3, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, t1)
        agg.add_event({"value": 2.0}, t2)
        # t1 and t2 may share a window since both are within 10s
        # There should be windows containing both events
        active = agg.get_active_windows()
        assert len(active) >= 2

    def test_watermark_closes_only_expired_windows(self):
        """Watermark should only close windows whose end <= watermark."""
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=5)
        t1 = datetime(2024, 1, 1, 12, 0, 3, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, 12, 0, 12, tzinfo=timezone.utc)
        agg.add_event({"value": 1.0}, t1)
        agg.add_event({"value": 2.0}, t2)

        # Watermark at 11s - should close windows ending at <= 11
        watermark = datetime(2024, 1, 1, 12, 0, 11, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        # Some windows should be closed, some should remain active
        assert len(results) >= 1
        assert len(agg.get_active_windows()) >= 1

    def test_full_lifecycle(self):
        """Test complete event lifecycle: add -> watermark -> late event."""
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)

        # Phase 1: Add events
        t = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        agg.add_event({"value": 10.0}, t)
        agg.add_event({"value": 20.0}, t)
        assert len(agg.get_active_windows()) >= 1

        # Phase 2: Get partial result
        windows = agg.get_active_windows()
        partial = agg.get_partial_result(windows[0])
        assert partial.event_count == 2
        assert partial.aggregations["value_sum"] == 30.0

        # Phase 3: Close with watermark
        watermark = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        results = agg.on_watermark(watermark)
        assert len(results) >= 1
        assert results[0].event_count == 2
        assert results[0].is_correction is False

        # Phase 4: Late event correction
        late_t = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        correction = agg.add_late_event({"value": 5.0}, late_t)
        assert correction is not None
        assert correction.corrected_result.event_count == 3
        assert correction.corrected_result.aggregations["value_sum"] == 35.0
        assert correction.corrected_result.correction_version == 1


class TestExtractNumericFields:
    """Tests for WindowAggregator._extract_numeric_fields()."""

    def test_extracts_integers(self):
        agg = WindowAggregator()
        result = agg._extract_numeric_fields({"count": 5, "name": "test"})
        assert result == {"count": 5.0}

    def test_extracts_floats(self):
        agg = WindowAggregator()
        result = agg._extract_numeric_fields({"price": 99.99})
        assert result == {"price": 99.99}

    def test_ignores_booleans(self):
        agg = WindowAggregator()
        result = agg._extract_numeric_fields({"active": True, "value": 1.0})
        assert "active" not in result
        assert "value" in result

    def test_ignores_strings_and_none(self):
        agg = WindowAggregator()
        result = agg._extract_numeric_fields({"name": "test", "data": None})
        assert result == {}

    def test_empty_event(self):
        agg = WindowAggregator()
        result = agg._extract_numeric_fields({})
        assert result == {}


class TestGetApplicableWindows:
    """Tests for WindowAggregator._get_applicable_windows()."""

    def test_tumbling_window_single_window(self):
        """With tumbling windows (size == slide), event belongs to exactly 1 window."""
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        windows = agg._get_applicable_windows(event_time)
        assert len(windows) == 1

    def test_sliding_window_multiple_windows(self):
        """With sliding windows, event belongs to window_size/slide windows."""
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=5)
        event_time = datetime(2024, 1, 1, 12, 0, 7, tzinfo=timezone.utc)
        windows = agg._get_applicable_windows(event_time)
        assert len(windows) == 2

    def test_window_boundaries_correct(self):
        """Windows should contain the event time."""
        agg = WindowAggregator(window_size_sec=10, slide_interval_sec=10)
        event_time = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        event_epoch = event_time.timestamp()
        windows = agg._get_applicable_windows(event_time)
        for window_id in windows:
            start, end = agg._parse_window_id(window_id)
            assert start <= event_epoch < end
