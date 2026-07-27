"""Event (Fact Stream) data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID


_MAX_FUTURE_TOLERANCE = timedelta(minutes=5)


@dataclass(frozen=True)
class Event:
    """A single fact stream event from the Kafka topic.

    Validation rules:
    - event_time must not be more than 5 minutes in the future
    - join_key must be a non-empty string
    - payload must be a dict (JSON-serializable)
    - partition >= 0
    - offset >= 0
    """

    event_id: UUID
    event_time: datetime  # Event-time (source timestamp)
    ingest_time: datetime  # Wall-clock time at ingestion
    join_key: str  # Key used for dimension table lookup
    payload: dict[str, Any]  # Event-specific fields (flexible schema)
    source_topic: str  # Originating Kafka topic
    partition: int  # Kafka partition number
    offset: int  # Kafka offset for exactly-once tracking

    def __post_init__(self) -> None:
        _validate_event(self)


def _validate_event(event: Event) -> None:
    """Validate event fields according to business rules."""
    now = datetime.now(timezone.utc)

    # event_time must not be more than 5 minutes in the future
    event_time = event.event_time
    if event_time.tzinfo is None:
        # Treat naive datetimes as UTC for comparison
        event_time_aware = event_time.replace(tzinfo=timezone.utc)
    else:
        event_time_aware = event_time
    if event_time_aware > now + _MAX_FUTURE_TOLERANCE:
        raise ValueError(
            f"event_time must not be more than 5 minutes in the future. "
            f"Got {event.event_time}, current time is {now}"
        )

    # join_key must be non-empty
    if not event.join_key:
        raise ValueError("join_key must be a non-empty string")

    # partition must be >= 0
    if event.partition < 0:
        raise ValueError(f"partition must be >= 0, got {event.partition}")

    # offset must be >= 0
    if event.offset < 0:
        raise ValueError(f"offset must be >= 0, got {event.offset}")
