"use client";

import { useEffect, useState, useRef } from "react";
import { createMetricsStream, StreamMetrics } from "@/lib/api";

export default function ThroughputGauge() {
  const [metrics, setMetrics] = useState<StreamMetrics | null>(null);
  const [connected, setConnected] = useState(false);
  const [history, setHistory] = useState<number[]>([]);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const es = createMetricsStream((data) => {
      setMetrics(data);
      setConnected(true);
      setHistory((prev) => [...prev.slice(-29), data.throughput]);
    });

    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);
    esRef.current = es;

    return () => {
      es.close();
    };
  }, []);

  const formatNumber = (n: number): string => {
    return n.toLocaleString("en-US");
  };

  const getLagColor = (lag: number): string => {
    if (lag < 2) return "text-emerald-400";
    if (lag < 5) return "text-amber-400";
    return "text-red-400";
  };

  const getLagGlow = (lag: number): string => {
    if (lag < 2) return "glow-green";
    if (lag < 5) return "glow-amber";
    return "glow-red";
  };

  const getLatencyColor = (ms: number): string => {
    if (ms < 50) return "text-emerald-400";
    if (ms < 200) return "text-amber-400";
    return "text-red-400";
  };

  // Sparkline SVG
  const renderSparkline = () => {
    if (history.length < 2) return null;
    const max = Math.max(...history, 1);
    const min = Math.min(...history, 0);
    const range = max - min || 1;
    const width = 200;
    const height = 40;

    const points = history
      .map((val, i) => {
        const x = (i / (history.length - 1)) * width;
        const y = height - ((val - min) / range) * height;
        return `${x},${y}`;
      })
      .join(" ");

    return (
      <svg width={width} height={height} className="opacity-60">
        <polyline
          points={points}
          fill="none"
          stroke="url(#sparkGradient)"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <defs>
          <linearGradient id="sparkGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#3b82f6" stopOpacity="0.4" />
            <stop offset="100%" stopColor="#3b82f6" stopOpacity="1" />
          </linearGradient>
        </defs>
      </svg>
    );
  };

  return (
    <div className="bg-gray-900/80 backdrop-blur-sm border border-gray-800 rounded-2xl p-6 glow-blue">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className={`w-2.5 h-2.5 rounded-full ${connected ? "bg-emerald-400 animate-pulse-glow" : "bg-red-400"}`} />
          <span className="text-sm text-gray-400 font-medium uppercase tracking-wider">
            Live Stream Metrics
          </span>
        </div>
        <span className="text-xs text-gray-500 font-mono">
          {connected ? "SSE Connected" : "Disconnected"}
        </span>
      </div>

      {/* Main metrics grid */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {/* Throughput - Primary */}
        <div className="md:col-span-1 flex flex-col items-center justify-center">
          <div className="text-center">
            <p className="text-5xl md:text-6xl font-bold text-blue-400 tabular-nums tracking-tight transition-all duration-300">
              {metrics ? formatNumber(metrics.throughput) : "—"}
            </p>
            <p className="text-sm text-gray-400 mt-2 font-medium">events/sec</p>
          </div>
          <div className="mt-4">{renderSparkline()}</div>
        </div>

        {/* Watermark Lag */}
        <div className="flex flex-col items-center justify-center">
          <div className={`text-center p-4 rounded-xl border border-gray-800 ${metrics ? getLagGlow(metrics.watermark_lag) : ""}`}>
            <p className={`text-4xl font-bold tabular-nums ${metrics ? getLagColor(metrics.watermark_lag) : "text-gray-500"}`}>
              {metrics ? metrics.watermark_lag.toFixed(2) : "—"}
              <span className="text-lg ml-1">s</span>
            </p>
            <p className="text-sm text-gray-400 mt-2 font-medium">Watermark Lag</p>
            <div className="flex items-center justify-center gap-2 mt-3">
              <div className="flex items-center gap-1">
                <div className="w-2 h-2 rounded-full bg-emerald-400" />
                <span className="text-xs text-gray-500">&lt;2s</span>
              </div>
              <div className="flex items-center gap-1">
                <div className="w-2 h-2 rounded-full bg-amber-400" />
                <span className="text-xs text-gray-500">2-5s</span>
              </div>
              <div className="flex items-center gap-1">
                <div className="w-2 h-2 rounded-full bg-red-400" />
                <span className="text-xs text-gray-500">&gt;5s</span>
              </div>
            </div>
          </div>
        </div>

        {/* P99 Latency */}
        <div className="flex flex-col items-center justify-center">
          <div className="text-center p-4 rounded-xl border border-gray-800">
            <p className={`text-4xl font-bold tabular-nums ${metrics ? getLatencyColor(metrics.p99_latency_ms) : "text-gray-500"}`}>
              {metrics ? metrics.p99_latency_ms.toFixed(1) : "—"}
              <span className="text-lg ml-1">ms</span>
            </p>
            <p className="text-sm text-gray-400 mt-2 font-medium">P99 Query Latency</p>
            <div className="mt-3">
              <div className="w-full bg-gray-800 rounded-full h-1.5">
                <div
                  className={`h-1.5 rounded-full transition-all duration-500 ${
                    metrics
                      ? metrics.p99_latency_ms < 50
                        ? "bg-emerald-400"
                        : metrics.p99_latency_ms < 200
                        ? "bg-amber-400"
                        : "bg-red-400"
                      : "bg-gray-700"
                  }`}
                  style={{
                    width: `${Math.min((metrics?.p99_latency_ms || 0) / 5, 100)}%`,
                  }}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
