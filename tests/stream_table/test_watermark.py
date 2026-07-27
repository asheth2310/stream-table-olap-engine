"""Unit tests for the WatermarkManager class."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from olap_engine.watermark import WatermarkDecision, WatermarkEvent, WatermarkManager


class TestWatermarkManagerInit:
    """Tests for WatermarkManager initialization."""

    def test_default_initialization(self):
        wm = WatermarkManager()
        assert wm.get_watermark() is None
        assert wm.get_watermark_lag() == 0.0
        assert wm.late_dropped_count == 0

    def test_custom_lateness_tolerance(self):
        wm = WatermarkManager(allowed_lateness_sec=5.0)
        # Process an event to establish watermark
        event_time = datetime.now(timezone.utc)
        wm.process_event(event_time)
        # Watermark should be event_time - 5s
        expected = event_time - timedelta(seconds=5)
        assert wm.get_watermark() == expected

    def test_custom_idle_timeout(self):
        wm = WatermarkManager(idle_timeout_sec=60.0)
        assert wm.get_watermark() is None


class TestProcessEvent:
    """Tests for WatermarkManager.process_event()."""

    def test_first_event_accepted(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime.now(timezone.utc)
        decision = wm.process_event(event_time)
        assert decision == WatermarkDecision.ACCEPT

    def test_watermark_established_after_first_event(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(event_time)
        expected_watermark = event_time - timedelta(seconds=10)
        assert wm.get_watermark() == expected_watermark

    def test_event_within_tolerance_accepted(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        # Establish watermark
        wm.process_event(base_time)
        # Event 15 seconds behind max_observed (5 seconds behind watermark)
        # Watermark = base_time - 10s. Threshold = watermark - 10s = base_time - 20s
        late_event = base_time - timedelta(seconds=15)
        decision = wm.process_event(late_event)
        assert decision == WatermarkDecision.ACCEPT

    def test_event_exactly_at_tolerance_boundary_accepted(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(base_time)
        # Watermark = base_time - 10s. Threshold = watermark - 10s = base_time - 20s
        boundary_event = base_time - timedelta(seconds=20)
        decision = wm.process_event(boundary_event)
        assert decision == WatermarkDecision.ACCEPT

    def test_event_beyond_tolerance_dropped(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(base_time)
        # Watermark = base_time - 10s. Threshold = watermark - 10s = base_time - 20s
        # Event that is beyond the threshold
        very_late_event = base_time - timedelta(seconds=21)
        decision = wm.process_event(very_late_event)
        assert decision == WatermarkDecision.DROP

    def test_dropped_count_increments(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(base_time)
        assert wm.late_dropped_count == 0

        # Drop events beyond tolerance
        very_late = base_time - timedelta(seconds=25)
        wm.process_event(very_late)
        assert wm.late_dropped_count == 1

        wm.process_event(very_late)
        assert wm.late_dropped_count == 2

    def test_watermark_monotonically_advances(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        t3 = datetime(2024, 1, 1, 12, 0, 10, tzinfo=timezone.utc)  # earlier than t2

        wm.process_event(t1)
        w1 = wm.get_watermark()

        wm.process_event(t2)
        w2 = wm.get_watermark()
        assert w2 >= w1  # Watermark advanced

        wm.process_event(t3)
        w3 = wm.get_watermark()
        assert w3 >= w2  # Watermark did NOT decrease

    def test_watermark_never_decreases_with_old_events(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        # Process a recent event
        recent = datetime(2024, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        wm.process_event(recent)
        watermark_after_recent = wm.get_watermark()

        # Process an older event - watermark should not go back
        old = datetime(2024, 1, 1, 11, 50, 0, tzinfo=timezone.utc)
        wm.process_event(old)
        watermark_after_old = wm.get_watermark()

        assert watermark_after_old >= watermark_after_recent

    def test_naive_datetime_treated_as_utc(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        # Naive datetime should be treated as UTC
        naive_time = datetime(2024, 1, 1, 12, 0, 0)
        decision = wm.process_event(naive_time)
        assert decision == WatermarkDecision.ACCEPT
        # Watermark should have timezone info
        assert wm.get_watermark().tzinfo is not None

    def test_sequential_ascending_events_all_accepted(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        for i in range(100):
            event_time = base_time + timedelta(seconds=i)
            decision = wm.process_event(event_time)
            assert decision == WatermarkDecision.ACCEPT
        assert wm.late_dropped_count == 0


class TestGetWatermark:
    """Tests for WatermarkManager.get_watermark()."""

    def test_none_before_any_events(self):
        wm = WatermarkManager()
        assert wm.get_watermark() is None

    def test_returns_datetime_after_event(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(event_time)
        watermark = wm.get_watermark()
        assert isinstance(watermark, datetime)
        assert watermark == event_time - timedelta(seconds=10)


class TestGetWatermarkLag:
    """Tests for WatermarkManager.get_watermark_lag()."""

    def test_zero_lag_when_no_watermark(self):
        wm = WatermarkManager()
        assert wm.get_watermark_lag() == 0.0

    def test_positive_lag_after_event(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        # Process event in the past to guarantee positive lag
        event_time = datetime.now(timezone.utc) - timedelta(seconds=20)
        wm.process_event(event_time)
        lag = wm.get_watermark_lag()
        # Lag should be at least ~30s (20s behind now + 10s lateness)
        assert lag >= 25.0  # approximate, giving some slack for test execution time

    def test_lag_never_negative(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        # Process an event that's very recent
        event_time = datetime.now(timezone.utc)
        wm.process_event(event_time)
        lag = wm.get_watermark_lag()
        assert lag >= 0.0


class TestAdvanceOnIdle:
    """Tests for WatermarkManager.advance_on_idle()."""

    def test_no_advance_when_events_are_recent(self):
        wm = WatermarkManager(idle_timeout_sec=30.0)
        event_time = datetime.now(timezone.utc)
        wm.process_event(event_time)
        # Just processed an event, should not advance on idle
        result = wm.advance_on_idle()
        assert result is False

    def test_advances_when_idle_timeout_exceeded(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0, idle_timeout_sec=30.0)
        event_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(event_time)
        watermark_before = wm.get_watermark()

        # Simulate idle by moving wall clock forward
        future_time = datetime.now(timezone.utc) + timedelta(seconds=60)
        with patch("olap_engine.watermark.manager.datetime") as mock_dt:
            mock_dt.now.return_value = future_time
            mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
            # Set the last event wall time far in the past
            wm._last_event_wall_time = future_time - timedelta(seconds=35)
            result = wm.advance_on_idle()

        assert result is True
        assert wm.get_watermark() >= watermark_before

    def test_advance_on_idle_with_no_prior_watermark(self):
        wm = WatermarkManager(idle_timeout_sec=30.0)
        # Simulate idle from the start by setting last event wall time in the past
        wm._last_event_wall_time = datetime.now(timezone.utc) - timedelta(seconds=35)
        result = wm.advance_on_idle()
        assert result is True
        assert wm.get_watermark() is not None

    def test_idle_advance_is_monotonic(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0, idle_timeout_sec=30.0)
        # Establish a high watermark
        recent_time = datetime.now(timezone.utc) - timedelta(seconds=5)
        wm.process_event(recent_time)
        watermark_before = wm.get_watermark()

        # Even if idle timeout exceeded, watermark should not go back
        wm._last_event_wall_time = datetime.now(timezone.utc) - timedelta(seconds=35)
        wm.advance_on_idle()
        watermark_after = wm.get_watermark()
        assert watermark_after >= watermark_before


class TestEmitWatermark:
    """Tests for WatermarkManager.emit_watermark()."""

    def test_returns_none_when_no_watermark(self):
        wm = WatermarkManager()
        result = wm.emit_watermark()
        assert result is None

    def test_returns_watermark_event_after_processing(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(event_time)

        result = wm.emit_watermark()
        assert result is not None
        assert isinstance(result, WatermarkEvent)
        assert result.watermark == event_time - timedelta(seconds=10)
        assert result.wall_clock is not None
        assert result.lag_seconds >= 0.0

    def test_emitted_watermark_matches_get_watermark(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime.now(timezone.utc) - timedelta(seconds=5)
        wm.process_event(event_time)

        emitted = wm.emit_watermark()
        assert emitted.watermark == wm.get_watermark()

    def test_lag_seconds_is_non_negative(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime.now(timezone.utc)
        wm.process_event(event_time)

        emitted = wm.emit_watermark()
        assert emitted.lag_seconds >= 0.0


class TestLateDroppedCount:
    """Tests for WatermarkManager.late_dropped_count property."""

    def test_starts_at_zero(self):
        wm = WatermarkManager()
        assert wm.late_dropped_count == 0

    def test_does_not_increment_on_accept(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime.now(timezone.utc)
        wm.process_event(event_time)
        assert wm.late_dropped_count == 0

    def test_increments_on_drop(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(base_time)

        # Create a very late event
        very_late = base_time - timedelta(seconds=30)
        wm.process_event(very_late)
        assert wm.late_dropped_count == 1


class TestWatermarkDecisionEnum:
    """Tests for WatermarkDecision enum."""

    def test_accept_value(self):
        assert WatermarkDecision.ACCEPT == "accept"
        assert WatermarkDecision.ACCEPT.value == "accept"

    def test_drop_value(self):
        assert WatermarkDecision.DROP == "drop"
        assert WatermarkDecision.DROP.value == "drop"

    def test_is_string(self):
        assert isinstance(WatermarkDecision.ACCEPT, str)
        assert isinstance(WatermarkDecision.DROP, str)


class TestWatermarkEvent:
    """Tests for WatermarkEvent dataclass."""

    def test_creation(self):
        now = datetime.now(timezone.utc)
        wm_time = now - timedelta(seconds=5)
        event = WatermarkEvent(watermark=wm_time, wall_clock=now, lag_seconds=5.0)
        assert event.watermark == wm_time
        assert event.wall_clock == now
        assert event.lag_seconds == 5.0


class TestEdgeCases:
    """Edge case tests for WatermarkManager."""

    def test_zero_lateness_tolerance(self):
        wm = WatermarkManager(allowed_lateness_sec=0.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        # With 0 lateness, watermark == max_observed_time
        decision = wm.process_event(base_time)
        assert decision == WatermarkDecision.ACCEPT
        assert wm.get_watermark() == base_time

        # Any event before the max observed should be dropped
        # (since threshold = watermark - 0 = max_observed)
        old_event = base_time - timedelta(microseconds=1)
        decision = wm.process_event(old_event)
        assert decision == WatermarkDecision.DROP

    def test_large_lateness_tolerance(self):
        wm = WatermarkManager(allowed_lateness_sec=3600.0)  # 1 hour
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.process_event(base_time)

        # Events up to 2 hours old should still be accepted
        old_event = base_time - timedelta(hours=2)
        decision = wm.process_event(old_event)
        assert decision == WatermarkDecision.ACCEPT

    def test_many_events_in_rapid_succession(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        # 1000 events 1ms apart - all should be accepted
        for i in range(1000):
            event_time = base_time + timedelta(milliseconds=i)
            decision = wm.process_event(event_time)
            assert decision == WatermarkDecision.ACCEPT
        assert wm.late_dropped_count == 0

    def test_same_timestamp_multiple_events(self):
        wm = WatermarkManager(allowed_lateness_sec=10.0)
        event_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        for _ in range(10):
            decision = wm.process_event(event_time)
            assert decision == WatermarkDecision.ACCEPT
        assert wm.late_dropped_count == 0
