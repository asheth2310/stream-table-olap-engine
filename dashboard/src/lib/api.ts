const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface StreamMetrics {
  throughput: number;
  watermark_lag: number;
  p99_latency_ms: number;
}

export interface HealthStatus {
  status: string;
  components: Record<string, { status: string; latency_ms?: number }>;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  execution_time_ms: number;
  error?: string;
}

export interface WindowData {
  window_start: string;
  window_end: string;
  count: number;
  aggregations: Record<string, number>;
}

export async function fetchQuery(sql: string): Promise<QueryResult> {
  const res = await fetch(`${API_URL}/api/query?q=${encodeURIComponent(sql)}`);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Query failed with status ${res.status}`);
  }
  return res.json();
}

export async function fetchWindows(timestamp?: string): Promise<WindowData[]> {
  const params = timestamp ? `?timestamp=${encodeURIComponent(timestamp)}` : '';
  const res = await fetch(`${API_URL}/api/windows${params}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch windows: ${res.status}`);
  }
  return res.json();
}

export async function fetchHealth(): Promise<HealthStatus> {
  const res = await fetch(`${API_URL}/health`);
  if (!res.ok) {
    throw new Error(`Health check failed: ${res.status}`);
  }
  return res.json();
}

export function createMetricsStream(onData: (metrics: StreamMetrics) => void): EventSource {
  const es = new EventSource(`${API_URL}/metrics/stream`);

  es.onmessage = (event) => {
    try {
      const data: StreamMetrics = JSON.parse(event.data);
      onData(data);
    } catch {
      // ignore parse errors
    }
  };

  es.onerror = () => {
    // EventSource will auto-reconnect
  };

  return es;
}

export function createQueryWebSocket(): WebSocket {
  const wsUrl = API_URL.replace(/^http/, 'ws');
  return new WebSocket(`${wsUrl}/ws/query`);
}
