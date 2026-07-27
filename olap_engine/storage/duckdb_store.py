"""DuckDB analytical storage layer.

Provides time-partitioned storage for joined events and window aggregation
results. Supports concurrent reads during writes, correction deduplication
via (window_id, correction_version), and automated 7-day retention cleanup.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb

from olap_engine.models.window import WindowResult

logger = logging.getLogger(__name__)


class DuckDBStore:
    """DuckDB-backed analytical storage with schema migrations.

    Thread-safe via a lock around write operations. DuckDB supports concurrent
    reads natively, so read queries do not need serialization.
    """

    def __init__(self, db_path: str = "analytics.duckdb") -> None:
        self._db_path = db_path
        self._conn = duckdb.connect(db_path)
        self._write_lock = threading.Lock()
        self._run_migrations()
        logger.info("DuckDBStore initialized at %s", db_path)

    def _run_migrations(self) -> None:
        """Create tables if they don't exist: joined_events, window_results.

        Uses CREATE TABLE IF NOT EXISTS for idempotent startup.
        """
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS joined_events (
                event_id VARCHAR,
                event_time TIMESTAMP WITH TIME ZONE,
                ingest_time TIMESTAMP WITH TIME ZONE,
                join_key VARCHAR,
                payload_json VARCHAR,
                dimension_attrs_json VARCHAR,
                source_topic VARCHAR,
                partition_id INTEGER,
                offset_id BIGINT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
        """)

        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS window_results (
                window_id VARCHAR,
                window_start TIMESTAMP WITH TIME ZONE,
                window_end TIMESTAMP WITH TIME ZONE,
                aggregations_json VARCHAR,
                event_count INTEGER,
                is_correction BOOLEAN,
                correction_version INTEGER,
                emitted_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            )
        """)

        # Create index for deduplication lookups
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_window_results_dedup
            ON window_results (window_id, correction_version)
        """)

        # Create index for time-based queries
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_joined_events_time
            ON joined_events (event_time)
        """)

        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_window_results_time
            ON window_results (window_start, window_end)
        """)

        logger.info("DuckDB schema migrations completed")

    def persist_joined_record(self, record: dict) -> None:
        """Write a joined event record to the joined_events table.

        Args:
            record: Dict containing event fields. Expected keys:
                event_id, event_time, ingest_time, join_key, payload_json,
                source_topic, partition, offset. Dimension attributes are
                extracted from remaining fields.
        """
        event_id = record.get("event_id", "")
        event_time = record.get("event_time")
        ingest_time = record.get("ingest_time")
        join_key = record.get("join_key", "")
        payload_json = record.get("payload_json", "{}")
        source_topic = record.get("source_topic", "")
        partition_id = record.get("partition", 0)
        offset_id = record.get("offset", 0)

        # Collect dimension attributes (all keys not in the base event fields)
        base_keys = {
            "event_id", "event_time", "ingest_time", "join_key",
            "payload_json", "source_topic", "partition", "offset",
        }
        dimension_attrs = {k: v for k, v in record.items() if k not in base_keys}
        dimension_attrs_json = json.dumps(dimension_attrs, default=str)

        # Normalize timestamps
        if isinstance(event_time, str):
            event_time = datetime.fromisoformat(event_time)
        if isinstance(ingest_time, str):
            ingest_time = datetime.fromisoformat(ingest_time)

        with self._write_lock:
            self._conn.execute(
                """
                INSERT INTO joined_events
                    (event_id, event_time, ingest_time, join_key, payload_json,
                     dimension_attrs_json, source_topic, partition_id, offset_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(event_id), event_time, ingest_time, str(join_key),
                    str(payload_json), dimension_attrs_json,
                    str(source_topic), int(partition_id), int(offset_id),
                ],
            )

    def persist_window_result(self, result: WindowResult) -> None:
        """Write window aggregation result with upsert semantics.

        Uses deduplication by (window_id, correction_version):
        if a result with the same window_id and correction_version exists,
        it is replaced (ReplacingMergeTree semantics).
        """
        aggregations_json = json.dumps(result.aggregations)

        with self._write_lock:
            # Delete existing entry with same window_id and correction_version
            self._conn.execute(
                """
                DELETE FROM window_results
                WHERE window_id = ? AND correction_version = ?
                """,
                [result.window_id, result.correction_version],
            )

            self._conn.execute(
                """
                INSERT INTO window_results
                    (window_id, window_start, window_end, aggregations_json,
                     event_count, is_correction, correction_version, emitted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    result.window_id, result.window_start, result.window_end,
                    aggregations_json, result.event_count,
                    result.is_correction, result.correction_version,
                    result.emitted_at,
                ],
            )

    def execute_query(self, query: str, params: list | None = None) -> list[dict]:
        """Execute an analytical query and return results as list of dicts.

        Args:
            query: SQL query string.
            params: Optional positional parameters for the query.

        Returns:
            List of dicts where each dict represents a row.
        """
        try:
            if params:
                result = self._conn.execute(query, params)
            else:
                result = self._conn.execute(query)

            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            return [dict(zip(columns, row)) for row in rows]
        except duckdb.Error as e:
            logger.error("Query execution error: %s", e)
            raise

    def get_window_at_time(self, timestamp: datetime) -> list[dict]:
        """Get window aggregation state at a specific timestamp.

        Returns windows that contain the given timestamp (for timeline slider).

        Args:
            timestamp: The point in time to query.

        Returns:
            List of window result dicts active at the given time.
        """
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        results = self._conn.execute(
            """
            SELECT window_id, window_start, window_end, aggregations_json,
                   event_count, is_correction, correction_version, emitted_at
            FROM window_results
            WHERE window_start <= ? AND window_end > ?
            ORDER BY correction_version DESC
            """,
            [timestamp, timestamp],
        )

        columns = [desc[0] for desc in results.description]
        rows = results.fetchall()

        # Deduplicate: keep only the latest correction_version per window_id
        seen: dict[str, dict] = {}
        for row in rows:
            row_dict = dict(zip(columns, row))
            wid = row_dict["window_id"]
            if wid not in seen:
                seen[wid] = row_dict

        return list(seen.values())

    def cleanup_old_data(self, retention_days: int = 7) -> int:
        """Delete data older than retention period.

        Args:
            retention_days: Number of days to retain data.

        Returns:
            Total number of rows deleted.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        deleted = 0

        with self._write_lock:
            result = self._conn.execute(
                "DELETE FROM joined_events WHERE event_time < ? RETURNING *",
                [cutoff],
            )
            deleted += len(result.fetchall())

            result = self._conn.execute(
                "DELETE FROM window_results WHERE window_end < ? RETURNING *",
                [cutoff],
            )
            deleted += len(result.fetchall())

        logger.info("Cleaned up %d rows older than %d days", deleted, retention_days)
        return deleted

    def get_table_stats(self) -> dict[str, int]:
        """Get row counts for monitoring."""
        events_count = self._conn.execute(
            "SELECT COUNT(*) FROM joined_events"
        ).fetchone()[0]
        windows_count = self._conn.execute(
            "SELECT COUNT(*) FROM window_results"
        ).fetchone()[0]
        return {
            "joined_events_count": events_count,
            "window_results_count": windows_count,
        }

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("DuckDBStore connection closed")
