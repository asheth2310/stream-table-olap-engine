"""Dimension table management for stream-table joins.

Provides an in-memory dimension table backed by a polars DataFrame for
batch SIMD joins, with a separate hash index for O(1) single-event lookups.
Supports slowly-changing dimensions via versioned upserts and soft deletes.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import polars as pl


class DimensionTableManager:
    """Manages the in-memory dimension table for stream-table joins.

    The manager maintains two parallel data structures:
    - A polars DataFrame for batch SIMD-accelerated joins
    - A dict-based hash index for O(1) single-event lookups

    The DataFrame schema includes columns:
        dimension_key (str), attributes_json (str), version (int),
        updated_at (datetime), is_active (bool)
    """

    def __init__(self, join_key: str = "user_id") -> None:
        self._join_key = join_key
        self._table: pl.DataFrame | None = None
        self._hash_index: dict[str, dict[str, Any]] = {}
        self._row_count: int = 0
        self._last_refresh_time: datetime | None = None
        # Internal version tracking per key for SCD support
        self._versions: dict[str, int] = {}

    def load_initial(self, data: list[dict]) -> None:
        """Load initial dimension data from a list of dicts.

        Each dict should contain at minimum a key field matching the configured
        join_key. Additional fields are stored as attributes.

        Args:
            data: List of dicts representing dimension rows. Each dict must
                  contain the join_key field. Other fields become attributes.
        """
        if not data:
            self._table = self._empty_dataframe()
            self._hash_index = {}
            self._versions = {}
            self._row_count = 0
            self._last_refresh_time = datetime.now(timezone.utc)
            return

        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)

        for record in data:
            key = str(record.get(self._join_key, ""))
            if not key:
                continue

            # Separate the join key from attributes
            attributes = {k: v for k, v in record.items() if k != self._join_key}
            version = record.get("version", 1)
            if isinstance(version, int) and version >= 1:
                pass
            else:
                version = 1

            rows.append({
                "dimension_key": key,
                "attributes_json": json.dumps(attributes),
                "version": version,
                "updated_at": now,
                "is_active": True,
            })

            # Build hash index
            self._hash_index[key] = attributes
            self._versions[key] = version

        if rows:
            self._table = pl.DataFrame(rows, schema={
                "dimension_key": pl.Utf8,
                "attributes_json": pl.Utf8,
                "version": pl.Int64,
                "updated_at": pl.Datetime("us", time_zone="UTC"),
                "is_active": pl.Boolean,
            })
        else:
            self._table = self._empty_dataframe()

        self._row_count = len(self._hash_index)
        self._last_refresh_time = now

    def update_row(self, key: str, attributes: dict, version: int) -> None:
        """Update or insert a dimension row (upsert by join_key).

        Only applies if version > existing version for that key (slowly-changing
        dimension semantics). If the key doesn't exist, it's inserted.

        Args:
            key: The dimension key value.
            attributes: Dict of dimension attributes to store.
            version: The version number for this update. Must be > existing.
        """
        existing_version = self._versions.get(key, 0)
        if version <= existing_version:
            return  # Ignore stale updates

        now = datetime.now(timezone.utc)
        new_row = {
            "dimension_key": key,
            "attributes_json": json.dumps(attributes),
            "version": version,
            "updated_at": now,
            "is_active": True,
        }

        # Update hash index
        self._hash_index[key] = attributes
        self._versions[key] = version

        # Update DataFrame
        if self._table is None or self._table.is_empty():
            self._table = pl.DataFrame([new_row], schema={
                "dimension_key": pl.Utf8,
                "attributes_json": pl.Utf8,
                "version": pl.Int64,
                "updated_at": pl.Datetime("us", time_zone="UTC"),
                "is_active": pl.Boolean,
            })
        else:
            # Remove existing row for this key (if any)
            filtered = self._table.filter(pl.col("dimension_key") != key)
            # Append new row
            new_df = pl.DataFrame([new_row], schema={
                "dimension_key": pl.Utf8,
                "attributes_json": pl.Utf8,
                "version": pl.Int64,
                "updated_at": pl.Datetime("us", time_zone="UTC"),
                "is_active": pl.Boolean,
            })
            self._table = pl.concat([filtered, new_df])

        self._row_count = len(self._hash_index)
        self._last_refresh_time = now

    def delete_row(self, key: str) -> None:
        """Soft-delete a dimension row (set is_active=False, remove from hash index).

        The row remains in the DataFrame with is_active=False for historical
        tracking, but is removed from the hash index so lookups return None.

        Args:
            key: The dimension key to soft-delete.
        """
        if key not in self._hash_index and key not in self._versions:
            return  # Nothing to delete

        # Remove from hash index
        self._hash_index.pop(key, None)

        # Update DataFrame: set is_active=False for this key
        if self._table is not None and not self._table.is_empty():
            self._table = self._table.with_columns(
                pl.when(pl.col("dimension_key") == key)
                .then(pl.lit(False))
                .otherwise(pl.col("is_active"))
                .alias("is_active")
            )

        self._row_count = len(self._hash_index)
        self._last_refresh_time = datetime.now(timezone.utc)

    def lookup(self, key: str) -> dict[str, Any] | None:
        """O(1) hash lookup for a single key.

        Args:
            key: The dimension key to look up.

        Returns:
            Attributes dict if found and active, None otherwise.
        """
        return self._hash_index.get(key)

    def get_polars_dataframe(self) -> pl.DataFrame:
        """Get the full dimension table as a polars DataFrame for batch joins.

        Returns only active rows suitable for joining.

        Returns:
            A polars DataFrame with active dimension rows.
        """
        if self._table is None:
            return self._empty_dataframe()

        return self._table.filter(pl.col("is_active") == True)  # noqa: E712

    @property
    def row_count(self) -> int:
        """Number of active rows in the dimension table."""
        return self._row_count

    def rebuild_index(self) -> None:
        """Rebuild the hash index from the polars DataFrame.

        Useful after bulk DataFrame operations or corruption recovery.
        Reconstructs the hash index from active rows in the DataFrame.
        """
        self._hash_index = {}
        self._versions = {}

        if self._table is None or self._table.is_empty():
            self._row_count = 0
            return

        active_rows = self._table.filter(pl.col("is_active") == True)  # noqa: E712

        for row in active_rows.iter_rows(named=True):
            key = row["dimension_key"]
            attributes = json.loads(row["attributes_json"])
            version = row["version"]
            self._hash_index[key] = attributes
            self._versions[key] = version

        self._row_count = len(self._hash_index)

    def _empty_dataframe(self) -> pl.DataFrame:
        """Create an empty DataFrame with the correct schema."""
        return pl.DataFrame(schema={
            "dimension_key": pl.Utf8,
            "attributes_json": pl.Utf8,
            "version": pl.Int64,
            "updated_at": pl.Datetime("us", time_zone="UTC"),
            "is_active": pl.Boolean,
        })
