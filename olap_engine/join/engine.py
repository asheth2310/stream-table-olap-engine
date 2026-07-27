"""Vectorized stream-table join engine using polars and pyarrow.

Provides both batch (SIMD-accelerated via polars) and single-event
(O(1) hash lookup) join paths for stream-table joins with left outer
join semantics.
"""

from __future__ import annotations

import json
import time
from typing import Any

import polars as pl
import pyarrow as pa

from olap_engine.join.dimension_table import DimensionTableManager


class JoinEngine:
    """Vectorized stream-table join using polars and pyarrow.

    The engine supports two join paths:
    - Batch path: converts Arrow RecordBatch → polars DataFrame, performs
      a left outer join with the dimension table, and converts back to Arrow.
    - Single-event path: uses the DimensionTableManager hash index for O(1)
      average-case lookup latency.

    Left outer join semantics: unmatched keys get null dimension attributes.
    """

    def __init__(self, dimension_manager: DimensionTableManager) -> None:
        self._dimension_manager = dimension_manager
        self._join_throughput: float = 0.0
        self._last_batch_time: float = 0.0
        self._last_batch_count: int = 0

    def join_batch(self, fact_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Join a batch of fact events with the dimension table (left outer join).

        1. Convert Arrow RecordBatch to polars DataFrame
        2. Get active dimension table as polars DataFrame from DimensionTableManager
        3. Perform left outer join on join_key = dimension_key
        4. Flatten dimension attributes_json into separate columns
        5. Convert back to Arrow RecordBatch

        Output schema: all fact columns + dimension attribute columns (parsed from
        attributes_json). Unmatched rows get null dimension fields.

        Args:
            fact_batch: Arrow RecordBatch with fact events (must have 'join_key' column).

        Returns:
            Arrow RecordBatch with fact columns + flattened dimension attributes.
        """
        start_time = time.perf_counter()

        # Convert Arrow RecordBatch to polars DataFrame
        fact_df = pl.from_arrow(fact_batch)
        num_rows = len(fact_df)

        # Get active dimension table
        dim_df = self._dimension_manager.get_polars_dataframe()

        if dim_df.is_empty():
            # No dimension data — return facts with no extra columns
            result_batch = self._polars_to_record_batch(fact_df)
            self._update_throughput(num_rows, start_time)
            return result_batch

        # Parse attributes_json into a struct column for flattening
        # First, collect all attribute keys from the dimension table
        attribute_keys = self._get_attribute_keys(dim_df)

        if not attribute_keys:
            # No attributes to join — just return fact batch as-is
            result_batch = self._polars_to_record_batch(fact_df)
            self._update_throughput(num_rows, start_time)
            return result_batch

        # Create a dimension DataFrame with flattened attributes for the join
        dim_with_attrs = self._flatten_dimension_attributes(dim_df, attribute_keys)

        # Perform left outer join: fact.join_key == dim.dimension_key
        joined_df = fact_df.join(
            dim_with_attrs,
            left_on="join_key",
            right_on="dimension_key",
            how="left",
        )

        # Convert back to Arrow RecordBatch
        result_batch = self._polars_to_record_batch(joined_df)

        self._update_throughput(num_rows, start_time)
        return result_batch

    def join_single(self, event: dict) -> dict:
        """Join a single event with dimension table for low-latency path.

        Uses hash index lookup (O(1) average case) via the DimensionTableManager.

        Args:
            event: Event dict that must contain a 'join_key' field.

        Returns:
            Event dict merged with dimension attributes. If the join key is
            not found in the dimension table, dimension attribute fields are
            set to None.
        """
        key_value = event.get("join_key", "")
        dimension_attrs = self._dimension_manager.lookup(str(key_value))

        if dimension_attrs is not None:
            return {**event, **dimension_attrs}
        else:
            # Get the attribute keys from the dimension table to null-fill
            dim_df = self._dimension_manager.get_polars_dataframe()
            if dim_df.is_empty():
                return dict(event)

            attribute_keys = self._get_attribute_keys(dim_df)
            null_attrs = {k: None for k in attribute_keys}
            return {**event, **null_attrs}

    def get_join_throughput(self) -> float:
        """Return current joins-per-second throughput."""
        return self._join_throughput

    def _update_throughput(self, count: int, start_time: float) -> None:
        """Update throughput metric after a batch join."""
        elapsed = time.perf_counter() - start_time
        if elapsed > 0:
            self._join_throughput = count / elapsed
        self._last_batch_time = elapsed
        self._last_batch_count = count

    @staticmethod
    def _polars_to_record_batch(df: pl.DataFrame) -> pa.RecordBatch:
        """Convert a polars DataFrame to an Arrow RecordBatch.

        polars `to_arrow()` returns a Table with ChunkedArrays; we combine
        chunks and convert to a single RecordBatch.
        """
        table = df.to_arrow()
        # Combine chunks so each column is a single contiguous array
        table = table.combine_chunks()
        return table.to_batches()[0] if table.num_rows > 0 else pa.RecordBatch.from_pydict(
            {col: pa.array([], type=table.schema.field(col).type) for col in table.column_names},
            schema=table.schema,
        )

    def _get_attribute_keys(self, dim_df: pl.DataFrame) -> list[str]:
        """Extract all unique attribute keys from the dimension table's attributes_json."""
        if dim_df.is_empty():
            return []

        # Sample all rows to get the full set of attribute keys
        all_keys: set[str] = set()
        for row in dim_df.select("attributes_json").iter_rows():
            try:
                attrs = json.loads(row[0])
                all_keys.update(attrs.keys())
            except (json.JSONDecodeError, TypeError):
                continue

        return sorted(all_keys)

    def _flatten_dimension_attributes(
        self, dim_df: pl.DataFrame, attribute_keys: list[str]
    ) -> pl.DataFrame:
        """Create a dimension DataFrame with dimension_key + flattened attribute columns.

        Parses the attributes_json column and extracts each key into its own column.
        """
        # Extract dimension_key and parse attributes into columns
        rows: list[dict[str, Any]] = []
        for row in dim_df.iter_rows(named=True):
            record: dict[str, Any] = {"dimension_key": row["dimension_key"]}
            try:
                attrs = json.loads(row["attributes_json"])
            except (json.JSONDecodeError, TypeError):
                attrs = {}

            for key in attribute_keys:
                record[key] = attrs.get(key)

            rows.append(record)

        if not rows:
            # Return empty DataFrame with correct schema
            schema = {"dimension_key": pl.Utf8}
            for key in attribute_keys:
                schema[key] = pl.Utf8  # Default to string, polars will infer
            return pl.DataFrame(schema=schema)

        return pl.DataFrame(rows)
