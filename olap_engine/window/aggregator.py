"""Sliding window aggregation engine with incremental O(1) updates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import floor

from olap_engine.models.window import WindowCorrection, WindowResult, WindowState


class WindowAggregator:
    """Incremental sliding window aggregation engine.

    Supports overlapping sliding windows with O(1) incremental updates for
    SUM, COUNT, MIN, MAX aggregations. AVG is tracked via (sum, count) accumulators.

    Default configuration: 5-minute window (300s), 1-second slide interval,
    resulting in up to 300 overlapping active windows.
    """

    def __init__(self, window_size_sec: int = 300, slide_interval_sec: int = 1) -> None:
        if window_size_sec <= 0:
            raise ValueError(f"window_size_sec must be > 0, got {window_size_sec}")
        if slide_interval_sec <= 0:
            raise ValueError(f"slide_interval_sec must be > 0, got {slide_interval_sec}")
        if window_size_sec < slide_interval_sec:
            raise ValueError(
                f"window_size_sec ({window_size_sec}) must be >= slide_interval_sec ({slide_interval_sec})"
            )

        self._window_size = timedelta(seconds=window_size_sec)
        self._slide_interval = timedelta(seconds=slide_interval_sec)
        self._active_windows: dict[str, WindowState] = {}
        self._closed_windows: dict[str, WindowState] = {}  # Keep for late corrections

    def add_event(self, event: dict, event_time: datetime) -> None:
        """Add event to all applicable active windows.

        O(1) updates for SUM, COUNT, MIN, MAX per window.
        Creates new windows as needed based on the event timestamp.
        """
        event_time = self._ensure_utc(event_time)
        numeric_fields = self._extract_numeric_fields(event)

        applicable_window_ids = self._get_applicable_windows(event_time)

        for window_id in applicable_window_ids:
            if window_id not in self._active_windows:
                # Parse window boundaries from the ID
                start_epoch, end_epoch = self._parse_window_id(window_id)
                window_start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
                window_end = datetime.fromtimestamp(end_epoch, tz=timezone.utc)
                self._active_windows[window_id] = WindowState(
                    window_id=window_id,
                    window_start=window_start,
                    window_end=window_end,
                    event_count=0,
                    sum_values={},
                    min_values={},
                    max_values={},
                    avg_accumulators={},
                    is_closed=False,
                    last_emitted_at=None,
                    correction_count=0,
                )

            window = self._active_windows[window_id]

            # Skip if window was already closed (shouldn't happen normally)
            if window.is_closed:
                continue

            # O(1) incremental aggregation updates
            window.event_count += 1
            for field, value in numeric_fields.items():
                # SUM: O(1) addition
                window.sum_values[field] = window.sum_values.get(field, 0.0) + value
                # MIN: O(1) comparison
                window.min_values[field] = min(
                    window.min_values.get(field, float("inf")), value
                )
                # MAX: O(1) comparison
                window.max_values[field] = max(
                    window.max_values.get(field, float("-inf")), value
                )
                # AVG accumulator: (sum, count) for deferred division
                acc = window.avg_accumulators.get(field, (0.0, 0))
                window.avg_accumulators[field] = (acc[0] + value, acc[1] + 1)

    def on_watermark(self, watermark: datetime) -> list[WindowResult]:
        """Close windows past the watermark, emit final results.

        A window is closed when window_end <= watermark.
        Closed windows are moved to _closed_windows for late event corrections.
        """
        watermark = self._ensure_utc(watermark)
        results: list[WindowResult] = []

        windows_to_close: list[str] = []
        for window_id, window in self._active_windows.items():
            if window.window_end <= watermark and not window.is_closed:
                windows_to_close.append(window_id)

        for window_id in windows_to_close:
            window = self._active_windows[window_id]
            window.is_closed = True
            window.last_emitted_at = datetime.now(timezone.utc)

            result = self._compute_window_result(window, is_correction=False)
            results.append(result)

            # Move to closed windows for late corrections
            self._closed_windows[window_id] = window
            del self._active_windows[window_id]

        return results

    def add_late_event(self, event: dict, event_time: datetime) -> WindowCorrection | None:
        """Add late event to already-closed window, emit correction.

        Returns None if event doesn't belong to any closed window.
        Updates the closed window's aggregation state and increments correction_version.
        """
        event_time = self._ensure_utc(event_time)
        numeric_fields = self._extract_numeric_fields(event)

        applicable_window_ids = self._get_applicable_windows(event_time)

        # Find the first closed window that this event belongs to
        for window_id in applicable_window_ids:
            if window_id in self._closed_windows:
                window = self._closed_windows[window_id]

                # Capture previous result before updating
                previous_result = self._compute_window_result(window, is_correction=True)

                # Update aggregation state with the late event
                window.event_count += 1
                for field, value in numeric_fields.items():
                    window.sum_values[field] = window.sum_values.get(field, 0.0) + value
                    window.min_values[field] = min(
                        window.min_values.get(field, float("inf")), value
                    )
                    window.max_values[field] = max(
                        window.max_values.get(field, float("-inf")), value
                    )
                    acc = window.avg_accumulators.get(field, (0.0, 0))
                    window.avg_accumulators[field] = (acc[0] + value, acc[1] + 1)

                # Increment correction version
                window.correction_count += 1
                window.last_emitted_at = datetime.now(timezone.utc)

                # Compute corrected result
                corrected_result = self._compute_window_result(window, is_correction=True)

                return WindowCorrection(
                    window_id=window_id,
                    previous_result=previous_result,
                    corrected_result=corrected_result,
                )

        return None

    def get_partial_result(self, window_id: str) -> WindowResult | None:
        """Get partial aggregation for an active window (for real-time queries).

        Returns None if the window_id is not found in active windows.
        """
        if window_id not in self._active_windows:
            return None

        window = self._active_windows[window_id]
        return self._compute_window_result(window, is_correction=False)

    def get_active_windows(self) -> list[str]:
        """List all currently active window identifiers."""
        return list(self._active_windows.keys())

    def _compute_window_result(
        self, window: WindowState, is_correction: bool = False
    ) -> WindowResult:
        """Compute final aggregation values from a WindowState.

        Produces a WindowResult with SUM, COUNT, MIN, MAX, and AVG for each field.
        """
        aggregations: dict[str, float] = {}

        # Add count
        aggregations["count"] = float(window.event_count)

        # Add sum, min, max, avg for each tracked field
        for field in window.sum_values:
            aggregations[f"{field}_sum"] = window.sum_values[field]

        for field in window.min_values:
            if window.min_values[field] != float("inf"):
                aggregations[f"{field}_min"] = window.min_values[field]

        for field in window.max_values:
            if window.max_values[field] != float("-inf"):
                aggregations[f"{field}_max"] = window.max_values[field]

        for field, (total, count) in window.avg_accumulators.items():
            if count > 0:
                aggregations[f"{field}_avg"] = total / count

        return WindowResult(
            window_id=window.window_id,
            window_start=window.window_start,
            window_end=window.window_end,
            aggregations=aggregations,
            event_count=window.event_count,
            is_correction=is_correction,
            correction_version=window.correction_count,
            emitted_at=window.last_emitted_at or datetime.now(timezone.utc),
        )

    def _get_applicable_windows(self, event_time: datetime) -> list[str]:
        """Determine which windows an event belongs to based on its timestamp.

        An event belongs to a window [start, end) if start <= event_time < end.
        With sliding windows, an event can belong to up to (window_size / slide_interval) windows.
        """
        event_time = self._ensure_utc(event_time)
        event_epoch = event_time.timestamp()
        slide_sec = self._slide_interval.total_seconds()
        window_size_sec = self._window_size.total_seconds()

        # The most recent window start that includes this event
        # A window [start, start+window_size) includes event if start <= event_epoch < start+window_size
        # => start <= event_epoch and event_epoch < start + window_size
        # => event_epoch - window_size < start <= event_epoch
        # Windows start at multiples of slide_interval

        # Latest window start at or before event_time (aligned to slide interval)
        latest_start_epoch = floor(event_epoch / slide_sec) * slide_sec

        window_ids: list[str] = []
        num_windows = int(window_size_sec / slide_sec)

        for i in range(num_windows):
            ws_epoch = latest_start_epoch - (slide_sec * i)
            we_epoch = ws_epoch + window_size_sec
            # Check that the event actually falls within this window
            if ws_epoch <= event_epoch < we_epoch:
                window_id = f"{ws_epoch}_{we_epoch}"
                window_ids.append(window_id)

        return window_ids

    def _extract_numeric_fields(self, event: dict) -> dict[str, float]:
        """Extract numeric fields from event payload for aggregation.

        Scans all top-level fields and returns those with int or float values.
        """
        numeric_fields: dict[str, float] = {}
        for key, value in event.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                numeric_fields[key] = float(value)
        return numeric_fields

    def _parse_window_id(self, window_id: str) -> tuple[float, float]:
        """Parse start and end epoch from a window_id string."""
        parts = window_id.split("_")
        start_epoch = float(parts[0])
        end_epoch = float(parts[1])
        return start_epoch, end_epoch

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        """Ensure a datetime is timezone-aware (UTC)."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
