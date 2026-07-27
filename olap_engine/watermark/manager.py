"""Watermark manager - Event-time watermark tracker with configurable lateness tolerance."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from enum import Enum


class WatermarkDecision(str, Enum):
    """Decision for whether an event should be accepted or dropped."""

    ACCEPT = "accept"
    DROP = "drop"


@dataclass
class WatermarkEvent:
    """Emitted watermark state for downstream operators."""

    watermark: datetime
    wall_clock: datetime
    lag_seconds: float


class WatermarkManager:
    """Event-time watermark tracker with configurable lateness tolerance.

    The watermark represents the system's understanding of event-time progress.
    It is computed as: max_observed_event_time - allowed_lateness.

    Key behaviors:
    - Watermark is monotonically advancing (never decreases)
    - Accept events within allowed_lateness behind the watermark
    - Drop events beyond tolerance, incrementing counter
    - Advance on idle after idle_timeout of no events using wall-clock time
    - Emit watermark state for downstream operators
    """

    def __init__(
        self, allowed_lateness_sec: float = 10.0, idle_timeout_sec: float = 30.0
    ) -> None:
        self._allowed_lateness = timedelta(seconds=allowed_lateness_sec)
        self._idle_timeout = timedelta(seconds=idle_timeout_sec)
        self._max_observed_time: datetime | None = None
        self._watermark: datetime | None = None
        self._last_event_wall_time: datetime = datetime.now(timezone.utc)
        self._late_dropped_count: int = 0

    def process_event(self, event_time: datetime) -> WatermarkDecision:
        """Process event timestamp and return accept/reject decision.

        Algorithm:
        1. Update max observed time
        2. Advance watermark monotonically: max(current_watermark, max_observed - allowed_lateness)
        3. Accept if event_time >= watermark - allowed_lateness
        4. Drop if event_time < watermark - allowed_lateness
        5. Increment late_dropped_count on drop
        """
        now = datetime.now(timezone.utc)
        self._last_event_wall_time = now

        # Ensure event_time is timezone-aware
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        # Update max observed event time
        if self._max_observed_time is None:
            self._max_observed_time = event_time
        else:
            self._max_observed_time = max(self._max_observed_time, event_time)

        # Compute candidate watermark = max_observed - allowed_lateness
        candidate_watermark = self._max_observed_time - self._allowed_lateness

        # Advance watermark monotonically (never decreases)
        if self._watermark is None:
            self._watermark = candidate_watermark
        else:
            self._watermark = max(self._watermark, candidate_watermark)

        # Decision: accept if event_time >= watermark - allowed_lateness
        threshold = self._watermark - self._allowed_lateness
        if event_time >= threshold:
            return WatermarkDecision.ACCEPT
        else:
            self._late_dropped_count += 1
            return WatermarkDecision.DROP

    def get_watermark(self) -> datetime | None:
        """Return current watermark value, or None if no events processed yet."""
        return self._watermark

    def get_watermark_lag(self) -> float:
        """Return wall-clock minus watermark in seconds.

        Returns 0.0 if no watermark has been established yet.
        """
        if self._watermark is None:
            return 0.0
        now = datetime.now(timezone.utc)
        lag = (now - self._watermark).total_seconds()
        return max(0.0, lag)

    def advance_on_idle(self) -> bool:
        """Advance watermark using wall-clock if no events for idle_timeout.

        Returns True if watermark was advanced, False otherwise.
        """
        now = datetime.now(timezone.utc)
        elapsed = now - self._last_event_wall_time

        if elapsed <= self._idle_timeout:
            return False

        # Advance watermark to now - idle_timeout (never in the future, monotonic)
        candidate = now - self._idle_timeout
        if self._watermark is None:
            self._watermark = candidate
            return True
        elif candidate > self._watermark:
            self._watermark = candidate
            return True

        return False

    def emit_watermark(self) -> WatermarkEvent | None:
        """Emit current watermark state for downstream operators.

        Returns None if no watermark has been established yet.
        Called by the pipeline orchestrator at least once per second.
        """
        if self._watermark is None:
            return None

        now = datetime.now(timezone.utc)
        lag = (now - self._watermark).total_seconds()
        return WatermarkEvent(
            watermark=self._watermark,
            wall_clock=now,
            lag_seconds=max(0.0, lag),
        )

    @property
    def late_dropped_count(self) -> int:
        """Number of events dropped as too late."""
        return self._late_dropped_count
