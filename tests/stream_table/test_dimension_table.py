"""Unit tests for DimensionTableManager."""

from __future__ import annotations

import json

import polars as pl
import pytest

from olap_engine.join.dimension_table import DimensionTableManager


class TestLoadInitial:
    """Tests for load_initial method."""

    def test_load_initial_with_valid_data(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        data = [
            {"user_id": "u1", "name": "Alice", "region": "US"},
            {"user_id": "u2", "name": "Bob", "region": "EU"},
        ]
        manager.load_initial(data)

        assert manager.row_count == 2
        assert manager.lookup("u1") == {"name": "Alice", "region": "US"}
        assert manager.lookup("u2") == {"name": "Bob", "region": "EU"}

    def test_load_initial_empty_list(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        assert manager.row_count == 0
        assert manager.lookup("any_key") is None

    def test_load_initial_skips_records_without_join_key(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        data = [
            {"user_id": "u1", "name": "Alice"},
            {"name": "NoKey"},  # Missing join key
            {"user_id": "", "name": "EmptyKey"},  # Empty join key
        ]
        manager.load_initial(data)

        assert manager.row_count == 1
        assert manager.lookup("u1") == {"name": "Alice"}

    def test_load_initial_with_custom_join_key(self) -> None:
        manager = DimensionTableManager(join_key="product_id")
        data = [
            {"product_id": "p1", "category": "electronics", "price": 99.99},
        ]
        manager.load_initial(data)

        assert manager.row_count == 1
        assert manager.lookup("p1") == {"category": "electronics", "price": 99.99}

    def test_load_initial_with_explicit_version(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        data = [
            {"user_id": "u1", "name": "Alice", "version": 3},
        ]
        manager.load_initial(data)

        # Version field goes into internal tracking, not attributes
        assert manager.row_count == 1
        # version is part of the record dict, so it ends up in attributes minus join_key
        result = manager.lookup("u1")
        assert result is not None
        assert result["name"] == "Alice"

    def test_load_initial_builds_polars_dataframe(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        data = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ]
        manager.load_initial(data)

        df = manager.get_polars_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert "dimension_key" in df.columns
        assert "attributes_json" in df.columns
        assert "version" in df.columns
        assert "updated_at" in df.columns
        assert "is_active" in df.columns


class TestUpdateRow:
    """Tests for update_row method (upsert with version check)."""

    def test_insert_new_row(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        manager.update_row("u1", {"name": "Alice", "region": "US"}, version=1)

        assert manager.row_count == 1
        assert manager.lookup("u1") == {"name": "Alice", "region": "US"}

    def test_update_existing_row_higher_version(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice", "region": "US"}])

        manager.update_row("u1", {"name": "Alice Updated", "region": "EU"}, version=2)

        assert manager.row_count == 1
        assert manager.lookup("u1") == {"name": "Alice Updated", "region": "EU"}

    def test_reject_stale_update_same_version(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice", "version": 2}])

        # Same version should be rejected
        manager.update_row("u1", {"name": "Stale"}, version=2)

        result = manager.lookup("u1")
        assert result is not None
        assert result["name"] == "Alice"

    def test_reject_stale_update_lower_version(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice", "version": 3}])

        # Lower version should be rejected
        manager.update_row("u1", {"name": "Stale"}, version=1)

        result = manager.lookup("u1")
        assert result is not None
        assert result["name"] == "Alice"

    def test_update_reflects_in_dataframe(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice"}])

        manager.update_row("u1", {"name": "Alice V2"}, version=2)

        df = manager.get_polars_dataframe()
        assert len(df) == 1
        row = df.row(0, named=True)
        attrs = json.loads(row["attributes_json"])
        assert attrs["name"] == "Alice V2"
        assert row["version"] == 2

    def test_multiple_inserts(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        manager.update_row("u1", {"name": "Alice"}, version=1)
        manager.update_row("u2", {"name": "Bob"}, version=1)
        manager.update_row("u3", {"name": "Charlie"}, version=1)

        assert manager.row_count == 3
        assert manager.lookup("u1") == {"name": "Alice"}
        assert manager.lookup("u2") == {"name": "Bob"}
        assert manager.lookup("u3") == {"name": "Charlie"}


class TestDeleteRow:
    """Tests for delete_row (soft-delete) method."""

    def test_soft_delete_removes_from_hash_index(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ])

        manager.delete_row("u1")

        assert manager.lookup("u1") is None
        assert manager.lookup("u2") == {"name": "Alice"} is None or manager.lookup("u2") is not None
        assert manager.lookup("u2") == {"name": "Bob"}

    def test_soft_delete_decrements_row_count(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ])

        manager.delete_row("u1")

        assert manager.row_count == 1

    def test_soft_delete_sets_is_active_false_in_dataframe(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice"}])

        manager.delete_row("u1")

        # Full table (including inactive) should still have the row
        df = manager._table
        assert df is not None
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["is_active"] is False

    def test_soft_delete_excludes_from_active_dataframe(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ])

        manager.delete_row("u1")

        df = manager.get_polars_dataframe()
        assert len(df) == 1
        assert df["dimension_key"][0] == "u2"

    def test_soft_delete_nonexistent_key_is_noop(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice"}])

        manager.delete_row("nonexistent")  # Should not raise

        assert manager.row_count == 1


class TestLookup:
    """Tests for lookup method."""

    def test_lookup_existing_key(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice", "tier": "gold"}])

        result = manager.lookup("u1")
        assert result == {"name": "Alice", "tier": "gold"}

    def test_lookup_nonexistent_key_returns_none(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice"}])

        result = manager.lookup("nonexistent")
        assert result is None

    def test_lookup_on_empty_table(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        assert manager.lookup("any") is None

    def test_lookup_after_delete_returns_none(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice"}])

        manager.delete_row("u1")
        assert manager.lookup("u1") is None


class TestGetPolarsDataframe:
    """Tests for get_polars_dataframe method."""

    def test_returns_only_active_rows(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
            {"user_id": "u3", "name": "Charlie"},
        ])
        manager.delete_row("u2")

        df = manager.get_polars_dataframe()
        assert len(df) == 2
        keys = df["dimension_key"].to_list()
        assert "u1" in keys
        assert "u3" in keys
        assert "u2" not in keys

    def test_returns_empty_dataframe_when_no_data(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        df = manager.get_polars_dataframe()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0
        assert "dimension_key" in df.columns

    def test_dataframe_has_correct_schema(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "Alice"}])

        df = manager.get_polars_dataframe()
        expected_columns = {"dimension_key", "attributes_json", "version", "updated_at", "is_active"}
        assert set(df.columns) == expected_columns


class TestRebuildIndex:
    """Tests for rebuild_index method."""

    def test_rebuild_restores_hash_index(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ])

        # Manually clear the index to simulate corruption
        manager._hash_index = {}
        manager._versions = {}
        manager._row_count = 0

        manager.rebuild_index()

        assert manager.row_count == 2
        assert manager.lookup("u1") == {"name": "Alice"}
        assert manager.lookup("u2") == {"name": "Bob"}

    def test_rebuild_excludes_inactive_rows(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ])
        manager.delete_row("u1")

        # Clear and rebuild
        manager._hash_index = {}
        manager._versions = {}
        manager._row_count = 0

        manager.rebuild_index()

        assert manager.row_count == 1
        assert manager.lookup("u1") is None
        assert manager.lookup("u2") == {"name": "Bob"}

    def test_rebuild_on_empty_dataframe(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        manager.rebuild_index()

        assert manager.row_count == 0

    def test_rebuild_on_none_table(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        # Don't call load_initial, _table is None
        manager.rebuild_index()

        assert manager.row_count == 0


class TestRowCount:
    """Tests for row_count property."""

    def test_row_count_after_load(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([
            {"user_id": "u1", "name": "A"},
            {"user_id": "u2", "name": "B"},
            {"user_id": "u3", "name": "C"},
        ])
        assert manager.row_count == 3

    def test_row_count_after_insert(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])
        manager.update_row("u1", {"name": "Alice"}, version=1)
        assert manager.row_count == 1

    def test_row_count_after_delete(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "A"}, {"user_id": "u2", "name": "B"}])
        manager.delete_row("u1")
        assert manager.row_count == 1

    def test_row_count_update_doesnt_change_count(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([{"user_id": "u1", "name": "A"}])
        manager.update_row("u1", {"name": "B"}, version=2)
        assert manager.row_count == 1


class TestSlowlyChangingDimensions:
    """Integration tests for slowly-changing dimension behavior."""

    def test_version_ordering_enforced(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        # Insert v1
        manager.update_row("u1", {"tier": "bronze"}, version=1)
        assert manager.lookup("u1") == {"tier": "bronze"}

        # Update to v3 (skip v2)
        manager.update_row("u1", {"tier": "gold"}, version=3)
        assert manager.lookup("u1") == {"tier": "gold"}

        # Try to apply v2 (stale) — should be rejected
        manager.update_row("u1", {"tier": "silver"}, version=2)
        assert manager.lookup("u1") == {"tier": "gold"}

    def test_upsert_then_delete_then_re_insert(self) -> None:
        manager = DimensionTableManager(join_key="user_id")
        manager.load_initial([])

        manager.update_row("u1", {"name": "Alice"}, version=1)
        assert manager.lookup("u1") == {"name": "Alice"}

        manager.delete_row("u1")
        assert manager.lookup("u1") is None
        assert manager.row_count == 0

        # Re-insert with higher version should work
        manager.update_row("u1", {"name": "Alice Reactivated"}, version=2)
        assert manager.lookup("u1") == {"name": "Alice Reactivated"}
        assert manager.row_count == 1
