# ⚡ Stream-Table OLAP Engine

**Sub-second real-time stream-table join and OLAP engine** that ingests 100K+ events/sec, joins streams with dimension tables using SIMD-accelerated Apache Arrow, and serves sub-50ms analytical queries to an interactive dashboard.

<!-- 
🎬 DEMO VIDEO: Record a short video of the dashboard running locally and replace this section.
Upload to GitHub (drag into an issue to get a URL) or use a GIF.
-->

![Dashboard Preview](https://via.placeholder.com/900x500/1a1a2e/3b82f6?text=Stream-Table+OLAP+Engine+Dashboard)

## 🏗️ Architecture

```
Event Sources → Redpanda (Kafka) → Ingestion Service → Watermark Manager
                                                              ↓
                                                    Join Engine (polars/Arrow)
                                                    + Dimension Table (in-memory)
                                                              ↓
                                                    Window Aggregator (O(1) incremental)
                                                              ↓
                                                    DuckDB (embedded OLAP store)
                                                              ↓
                                                    FastAPI (HTTP + WebSocket + SSE)
                                                              ↓
                                                    Next.js Dashboard (Vercel)
```

## 🚀 Quick Start (Local)

```bash
# 1. Start Redpanda (optional — works without it in degraded mode)
docker compose up -d

# 2. Install Python dependencies
pip install -e ".[dev]"

# 3. Start the backend (API + pipeline)
python -m olap_engine
# → API running at http://localhost:8000

# 4. Start the dashboard (separate terminal)
cd dashboard
npm install
npm run dev
# → Dashboard at http://localhost:3000
```

## 📊 Dashboard Features

### Real-Time Throughput Gauge
- Events/sec with live sparkline
- Watermark lag (color-coded: 🟢 <2s, 🟡 2-5s, 🔴 >5s)
- P99 query latency

### Interactive Timeline
- 30-minute timeline with event density visualization
- Drag slider to inspect window state at any point in time

### SQL Query Playground
- CodeMirror editor with SQL syntax highlighting
- Execute queries against DuckDB (Ctrl+Enter)
- Auto-renders bar charts for numeric results
- Query history

## 🧠 How It Works

1. **Events arrive** via Kafka protocol (Redpanda)
2. **Watermark Manager** tracks event-time progress, accepts events within 10s tolerance
3. **Join Engine** enriches events with dimension table data using polars SIMD joins
4. **Window Aggregator** computes sliding-window aggregations (SUM, COUNT, MIN, MAX, AVG) with O(1) updates
5. **DuckDB** stores joined events + window results for historical queries
6. **FastAPI** serves queries via HTTP, WebSocket (streaming), and SSE (live metrics)
7. **Next.js Dashboard** visualizes everything in real-time

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Message Broker | Redpanda (Kafka-compatible) |
| Ingestion | Python + aiokafka |
| Processing | polars + pyarrow (SIMD-accelerated) |
| Storage | DuckDB (embedded) |
| API | FastAPI + WebSocket + SSE |
| Frontend | Next.js 14, Tailwind CSS, Recharts, CodeMirror |

## 📁 Project Structure

```
stream-table-olap-engine/
├── olap_engine/              # Python backend
│   ├── api/                  # FastAPI (health, query, WebSocket, SSE)
│   ├── ingestion/            # Kafka consumer + dead-letter routing
│   ├── watermark/            # Event-time watermark manager
│   ├── join/                 # Dimension table + vectorized join engine
│   ├── window/               # Sliding window aggregator
│   ├── storage/              # DuckDB store
│   ├── config/               # Pipeline configuration (TOML)
│   ├── models/               # Data models
│   └── schemas/              # Arrow + JSON schemas
├── dashboard/                # Next.js frontend
│   ├── src/app/              # App Router pages
│   ├── src/components/       # UI components
│   └── src/lib/              # API client
├── tests/                    # 263+ pytest tests
├── docker-compose.yml        # Local dev (Redpanda)
├── Dockerfile                # Railway deployment
└── pyproject.toml            # Python dependencies
```

## 🧪 Testing

```bash
cd olap-engine
python -m pytest tests/ -v
# 263 tests covering all components
```

## 🎯 Performance Targets

- **100K events/sec** ingestion throughput
- **Sub-1ms** per join (10M-row dimension table)
- **Sub-50ms** p95 query latency
- **< 32GB RAM** at peak load

## 📡 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Component health status |
| `/api/query?q=SQL` | GET | Execute analytical SQL |
| `/api/windows` | GET | Window aggregation results |
| `/metrics/stream` | GET (SSE) | Real-time metrics at 1Hz |
| `/ws/query` | WebSocket | Interactive SQL with streaming results |
| `/docs` | GET | Swagger UI |

## 🌐 Deployment

- **Dashboard**: Deployed on [Vercel](https://olap-dashboard-indol.vercel.app)
- **Backend**: Deploy via Docker on Railway, Render, or Fly.io
- **Local**: Docker Compose + uvicorn

## 📄 License

MIT
