"""Window aggregation data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class WindowState:
    """Incremental aggregation state for a single sliding window.

    Invariants:
    - window_start < window_end
    - event_count >= 0
    - correction_count >= 0
    - If is_closed is True, last_emitted_at is not None
    """

    window_id: str  # Unique identifier: "{metric}_{start_epoch}_{end_epoch}"
    window_start: datetime  # Window start boundary
    window_end: datetime  # Window end boundary
    event_count: int = 0  # Number of events in window
    sum_values: dict[str, float] = field(default_factory=dict)
    min_values: dict[str, float] = field(default_factory=dict)
    max_values: dict[str, float] = field(default_factory=dict)
    avg_accumulators: dict[str, tuple[float, int]] = field(default_factory=dict)
    is_closed: bool = False
    last_emitted_at: datetime | None = None
    correction_count: int = 0

    def __post_init__(self) -> None:
        _validate_window_state(self)


def _validate_window_state(state: WindowState) -> None:
    """Validate window state invariants."""
    if state.window_start >= state.window_end:
        raise ValueError(
            f"window_start must be < window_end. "
            f"Got start={state.window_start}, end={state.window_end}"
        )

    if state.event_count < 0:
        raise ValueError(f"event_count must be >= 0, got {state.event_count}")

    if state.correction_count < 0:
        raise ValueError(f"correction_count must be >= 0, got {state.correction_count}")

    if state.is_closed and state.last_emitted_at is None:
        raise ValueError("last_emitted_at must not be None when is_closed is True")


@dataclass(frozen=True)
class WindowResult:
    """Emitted result for a closed or corrected window."""

    window_id: str
    window_start: datetime
    window_end: datetime
    aggregations: dict[str, float]  # {metric_name: aggregated_value}
    event_count: int
    is_correction: bool  # True if this supersedes a previous emission
    correction_version: int  # Monotonically increasing for corrections
    emitted_at: datetime


@dataclass(frozen=True)
class WindowCorrection:
    """Correction pairing a previous and corrected window result."""

    window_id: str
    previous_result: WindowResult
    corrected_result: WindowResult
