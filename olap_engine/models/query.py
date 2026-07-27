"""Query result data models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class QueryResult:
    """Result of an analytical query execution."""

    query_id: UUID
    columns: list[str]  # Column names in result set
    rows: list[list[Any]]  # Result rows
    row_count: int  # Total rows returned
    execution_time_ms: float  # Query execution duration
    is_partial: bool = False  # True if result is from active window (not finalized)
    truncated: bool = False  # True if result exceeded max rows
    total_available: int | None = None  # Total rows available if truncated


@dataclass(frozen=True)
class QueryValidationResult:
    """Result of SQL query validation."""

    is_valid: bool
    error_message: str | None = None
    error_line: int | None = None
    error_column: int | None = None
