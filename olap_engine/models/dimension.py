"""Dimension table row data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class DimensionRow:
    """A single row in the dimension table.

    Validation rules:
    - dimension_key must be a non-empty string
    - attributes must be a dict (JSON-serializable)
    - version >= 1
    """

    dimension_key: str  # Primary lookup key (indexed)
    attributes: dict[str, Any]  # Dimension attributes (e.g., user_name, region, tier)
    version: int  # Version for slowly-changing dimension tracking
    updated_at: datetime  # Last modification timestamp
    is_active: bool  # Soft-delete flag

    def __post_init__(self) -> None:
        _validate_dimension_row(self)


def _validate_dimension_row(row: DimensionRow) -> None:
    """Validate dimension row fields."""
    if not row.dimension_key:
        raise ValueError("dimension_key must be a non-empty string")

    if not isinstance(row.attributes, dict):
        raise TypeError(f"attributes must be a dict, got {type(row.attributes).__name__}")

    if row.version < 1:
        raise ValueError(f"version must be >= 1, got {row.version}")
