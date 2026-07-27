"""FastAPI application for the Stream-Table OLAP Engine.

Provides:
- /health endpoint for component status
- /ws/query WebSocket endpoint for interactive SQL queries
- /metrics/stream SSE endpoint for real-time metrics at 1Hz
- /api/query HTTP endpoint for simple queries
- /api/windows HTTP endpoint for window aggregations
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator
from uuid import uuid4

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

logger = logging.getLogger(__name__)

# Global references set by the pipeline during startup
_store = None
_pipeline = None
_metrics_state: dict[str, Any] = {
    "throughput": 0.0,
    "watermark_lag": 0.0,
    "p99_latency_ms": 0.0,
    "active_queries": 0,
}
_query_latencies: list[float] = []
_max_concurrent_queries = 100
_active_query_count = 0
_query_count_lock = asyncio.Lock()


def set_store(store):
    """Set the DuckDB store reference (called during pipeline init)."""
    global _store
    _store = store


def set_pipeline(pipeline):
    """Set the pipeline reference (called during startup)."""
    global _pipeline
    _pipeline = pipeline


def update_metrics(throughput: float = None, watermark_lag: float = None):
    """Update metrics state (called by pipeline loop)."""
    if throughput is not None:
        _metrics_state["throughput"] = throughput
    if watermark_lag is not None:
        _metrics_state["watermark_lag"] = watermark_lag


def _compute_p99() -> float:
    """Compute p99 latency from recent query latencies."""
    if not _query_latencies:
        return 0.0
    sorted_latencies = sorted(_query_latencies)
    idx = int(len(sorted_latencies) * 0.99)
    idx = min(idx, len(sorted_latencies) - 1)
    return sorted_latencies[idx]


def _record_query_latency(latency_ms: float):
    """Record a query latency for p99 calculation."""
    _query_latencies.append(latency_ms)
    # Keep only last 1000 latencies
    if len(_query_latencies) > 1000:
        _query_latencies.pop(0)
    _metrics_state["p99_latency_ms"] = _compute_p99()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown."""
    logger.info("OLAP Engine API starting up")
    yield
    logger.info("OLAP Engine API shutting down")
    if _store is not None:
        _store.close()


app = FastAPI(
    title="Stream-Table OLAP Engine",
    description="Sub-second real-time stream-table join and OLAP engine",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware to allow frontend at localhost:3000
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://*.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    """Return component status.

    200 when all healthy, 503 with degraded component JSON.
    """
    components = {
        "storage": "healthy",
        "ingestion": "healthy",
        "watermark": "healthy",
        "join_engine": "healthy",
        "window_aggregator": "healthy",
    }

    # Check storage
    if _store is None:
        components["storage"] = "degraded"
    else:
        try:
            _store.execute_query("SELECT 1")
        except Exception:
            components["storage"] = "degraded"

    # Check pipeline
    if _pipeline is None:
        components["ingestion"] = "unknown"
        components["watermark"] = "unknown"
        components["join_engine"] = "unknown"
        components["window_aggregator"] = "unknown"

    all_healthy = all(v == "healthy" for v in components.values())

    if all_healthy:
        return JSONResponse(
            status_code=200,
            content={"status": "healthy", "components": components},
        )
    else:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "components": components},
        )


@app.websocket("/ws/query")
async def websocket_query(websocket: WebSocket):
    """Accept SQL queries via WebSocket, validate, execute, stream results.

    Protocol:
    - Client sends JSON: {"query": "SELECT ...", "params": {...}}
    - Server responds with JSON: {"type": "result", "columns": [...], "rows": [...]}
    - Or error: {"type": "error", "message": "...", "line": N, "column": N}
    """
    await websocket.accept()
    logger.info("WebSocket query connection opened")

    try:
        while True:
            data = await websocket.receive_text()

            try:
                msg = json.loads(data)
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON message",
                })
                continue

            query = msg.get("query", "")
            if not query.strip():
                await websocket.send_json({
                    "type": "error",
                    "message": "Empty query",
                })
                continue

            # Validate query
            validation = _validate_sql(query)
            if not validation["is_valid"]:
                await websocket.send_json({
                    "type": "error",
                    "message": validation["error_message"],
                    "line": validation.get("error_line"),
                    "column": validation.get("error_column"),
                })
                continue

            # Execute query
            if _store is None:
                await websocket.send_json({
                    "type": "error",
                    "message": "Storage not available",
                })
                continue

            start_time = time.perf_counter()
            try:
                results = _store.execute_query(query)
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                _record_query_latency(elapsed_ms)

                # Stream results
                columns = list(results[0].keys()) if results else []
                rows = [list(r.values()) for r in results]

                await websocket.send_json({
                    "type": "result",
                    "query_id": str(uuid4()),
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "execution_time_ms": round(elapsed_ms, 2),
                })
            except Exception as e:
                elapsed_ms = (time.perf_counter() - start_time) * 1000
                await websocket.send_json({
                    "type": "error",
                    "message": str(e),
                })

    except WebSocketDisconnect:
        logger.info("WebSocket query connection closed")
    except Exception as e:
        logger.error("WebSocket error: %s", e)


@app.get("/metrics/stream")
async def metrics_stream(request: Request):
    """SSE endpoint pushing throughput, watermark lag, p99 at 1Hz."""

    async def event_generator() -> AsyncGenerator[dict, None]:
        while True:
            if await request.is_disconnected():
                break

            metrics = {
                "throughput": round(_metrics_state["throughput"], 2),
                "watermark_lag": round(_metrics_state["watermark_lag"], 3),
                "p99_latency_ms": round(_metrics_state["p99_latency_ms"], 2),
                "active_queries": _metrics_state.get("active_queries", 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            yield {
                "event": "metrics",
                "data": json.dumps(metrics),
            }

            await asyncio.sleep(1.0)

    return EventSourceResponse(event_generator())


@app.get("/api/query")
async def execute_query(q: str = Query(..., description="SQL query to execute")):
    """HTTP query endpoint for simple queries.

    Returns JSON with columns, rows, and execution metadata.
    Rate-limited to max_concurrent_queries.
    """
    global _active_query_count

    async with _query_count_lock:
        if _active_query_count >= _max_concurrent_queries:
            raise HTTPException(
                status_code=429,
                detail="Too many concurrent queries",
                headers={"Retry-After": "5"},
            )
        _active_query_count += 1

    try:
        # Validate
        validation = _validate_sql(q)
        if not validation["is_valid"]:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": validation["error_message"],
                    "line": validation.get("error_line"),
                    "column": validation.get("error_column"),
                },
            )

        if _store is None:
            raise HTTPException(status_code=503, detail="Storage not available")

        start_time = time.perf_counter()
        results = _store.execute_query(q)
        elapsed_ms = (time.perf_counter() - start_time) * 1000
        _record_query_latency(elapsed_ms)

        columns = list(results[0].keys()) if results else []
        rows = [list(r.values()) for r in results]

        return {
            "query_id": str(uuid4()),
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "execution_time_ms": round(elapsed_ms, 2),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        async with _query_count_lock:
            _active_query_count -= 1


@app.get("/api/windows")
async def get_windows(timestamp: str | None = None):
    """Get window aggregations, optionally at a specific timestamp.

    Args:
        timestamp: ISO format timestamp to query windows at.
                  If None, returns the most recent windows.
    """
    if _store is None:
        raise HTTPException(status_code=503, detail="Storage not available")

    try:
        if timestamp:
            ts = datetime.fromisoformat(timestamp)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            windows = _store.get_window_at_time(ts)
        else:
            # Return most recent windows
            windows = _store.execute_query("""
                SELECT window_id, window_start, window_end, aggregations_json,
                       event_count, is_correction, correction_version, emitted_at
                FROM window_results
                ORDER BY emitted_at DESC
                LIMIT 100
            """)

        # Parse aggregations_json for each window
        for w in windows:
            if "aggregations_json" in w and isinstance(w["aggregations_json"], str):
                try:
                    w["aggregations"] = json.loads(w["aggregations_json"])
                except (json.JSONDecodeError, TypeError):
                    w["aggregations"] = {}
                del w["aggregations_json"]

        return {"windows": windows, "count": len(windows)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _validate_sql(query: str) -> dict:
    """Basic SQL validation.

    Checks for dangerous operations and basic syntax issues.
    Returns dict with is_valid, error_message, error_line, error_column.
    """
    query_stripped = query.strip().upper()

    # Block DDL operations
    dangerous_prefixes = ("DROP", "ALTER", "CREATE", "TRUNCATE", "DELETE", "UPDATE", "INSERT")
    for prefix in dangerous_prefixes:
        if query_stripped.startswith(prefix):
            return {
                "is_valid": False,
                "error_message": f"Operation '{prefix}' is not allowed. Only SELECT queries are permitted.",
                "error_line": 1,
                "error_column": 0,
            }

    # Must start with SELECT or WITH
    if not (query_stripped.startswith("SELECT") or query_stripped.startswith("WITH")):
        return {
            "is_valid": False,
            "error_message": "Only SELECT queries are permitted.",
            "error_line": 1,
            "error_column": 0,
        }

    # Basic syntax check: try to prepare the query
    if _store is not None:
        try:
            # Use DuckDB's built-in parser for validation
            _store._conn.execute(f"EXPLAIN {query}")
        except Exception as e:
            error_msg = str(e)
            return {
                "is_valid": False,
                "error_message": error_msg,
                "error_line": 1,
                "error_column": 0,
            }

    return {"is_valid": True}
