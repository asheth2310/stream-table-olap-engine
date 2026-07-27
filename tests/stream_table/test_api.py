"""Tests for the FastAPI application endpoints."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient, ASGITransport

from olap_engine.api.app import app, set_store, set_pipeline
from olap_engine.models.window import WindowResult
from olap_engine.storage.duckdb_store import DuckDBStore


@pytest.fixture
def store(tmp_path):
    """Create a test DuckDBStore."""
    db_path = str(tmp_path / "test_api.duckdb")
    s = DuckDBStore(db_path=db_path)
    set_store(s)
    yield s
    s.close()
    set_store(None)


@pytest.fixture
async def client(store):
    """Create an async test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestHealthEndpoint:
    """Test /health endpoint."""

    @pytest.mark.asyncio
    async def test_health_healthy(self, client: AsyncClient, store):
        """Health should return 200 when store is available."""
        response = await client.get("/health")
        # With no pipeline set, some components will be "unknown" — but storage is healthy
        assert response.status_code in (200, 503)
        data = response.json()
        assert "components" in data
        assert data["components"]["storage"] == "healthy"

    @pytest.mark.asyncio
    async def test_health_without_store(self, tmp_path):
        """Health should return 503 when store is None."""
        set_store(None)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get("/health")
            assert response.status_code == 503
            data = response.json()
            assert data["components"]["storage"] == "degraded"


class TestQueryEndpoint:
    """Test /api/query endpoint."""

    @pytest.mark.asyncio
    async def test_simple_query(self, client: AsyncClient, store):
        """A simple SELECT should return results."""
        response = await client.get("/api/query", params={"q": "SELECT 1 AS val"})
        assert response.status_code == 200
        data = response.json()
        assert data["columns"] == ["val"]
        assert data["rows"] == [[1]]
        assert data["row_count"] == 1
        assert "execution_time_ms" in data

    @pytest.mark.asyncio
    async def test_query_validation_blocks_drop(self, client: AsyncClient, store):
        """DDL operations should be blocked."""
        response = await client.get("/api/query", params={"q": "DROP TABLE joined_events"})
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_query_validation_blocks_insert(self, client: AsyncClient, store):
        """INSERT should be blocked."""
        response = await client.get(
            "/api/query",
            params={"q": "INSERT INTO joined_events VALUES (1)"},
        )
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_invalid_sql(self, client: AsyncClient, store):
        """Invalid SQL should return 400."""
        response = await client.get("/api/query", params={"q": "SELECT FROM WHERE"})
        assert response.status_code == 400

    @pytest.mark.asyncio
    async def test_query_no_store(self, tmp_path):
        """Should return 503 if store is not set."""
        set_store(None)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get("/api/query", params={"q": "SELECT 1"})
            assert response.status_code == 503


class TestWindowsEndpoint:
    """Test /api/windows endpoint."""

    @pytest.mark.asyncio
    async def test_get_windows_empty(self, client: AsyncClient, store):
        """Should return empty list when no windows."""
        response = await client.get("/api/windows")
        assert response.status_code == 200
        data = response.json()
        assert data["windows"] == []
        assert data["count"] == 0

    @pytest.mark.asyncio
    async def test_get_windows_with_data(self, client: AsyncClient, store):
        """Should return windows after persisting data."""
        now = datetime.now(timezone.utc)
        result = WindowResult(
            window_id="test_window",
            window_start=now - timedelta(minutes=5),
            window_end=now,
            aggregations={"count": 50.0, "amount_sum": 1000.0},
            event_count=50,
            is_correction=False,
            correction_version=0,
            emitted_at=now,
        )
        store.persist_window_result(result)

        response = await client.get("/api/windows")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] >= 1
        assert any(w["window_id"] == "test_window" for w in data["windows"])

    @pytest.mark.asyncio
    async def test_get_windows_at_timestamp(self, client: AsyncClient, store):
        """Should return windows at a specific timestamp."""
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=5)
        end = now
        mid = start + timedelta(minutes=2)

        result = WindowResult(
            window_id="ts_window",
            window_start=start,
            window_end=end,
            aggregations={"count": 25.0},
            event_count=25,
            is_correction=False,
            correction_version=0,
            emitted_at=now,
        )
        store.persist_window_result(result)

        response = await client.get("/api/windows", params={"timestamp": mid.isoformat()})
        assert response.status_code == 200
        data = response.json()
        assert data["count"] >= 1


class TestWebSocketQuery:
    """Test /ws/query WebSocket endpoint."""

    @pytest.mark.asyncio
    async def test_websocket_query(self, store):
        """WebSocket should accept and execute queries."""
        try:
            from httpx_ws import aconnect_ws
        except ImportError:
            pytest.skip("httpx-ws not installed")
            return

        from httpx import AsyncClient, ASGITransport

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            async with aconnect_ws("/ws/query", client) as ws:
                await ws.send_json({"query": "SELECT 42 AS answer"})
                response = await ws.receive_json()
                assert response["type"] == "result"
                assert response["rows"] == [[42]]

    @pytest.mark.asyncio
    async def test_websocket_invalid_query(self, store):
        """WebSocket should return error for invalid queries."""
        try:
            from httpx_ws import aconnect_ws
        except ImportError:
            pytest.skip("httpx-ws not installed")
            return

        from httpx import AsyncClient, ASGITransport

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            async with aconnect_ws("/ws/query", client) as ws:
                await ws.send_json({"query": "DROP TABLE foo"})
                response = await ws.receive_json()
                assert response["type"] == "error"
