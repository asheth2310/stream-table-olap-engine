"use client";

import { useEffect, useState } from "react";
import ThroughputGauge from "@/components/ThroughputGauge";
import TimelineSlider from "@/components/TimelineSlider";
import QueryPlayground from "@/components/QueryPlayground";
import { fetchHealth, HealthStatus } from "@/lib/api";

export default function DashboardPage() {
  const [health, setHealth] = useState<HealthStatus | null>(null);
  const [healthError, setHealthError] = useState(false);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const status = await fetchHealth();
        setHealth(status);
        setHealthError(false);
      } catch {
        setHealthError(true);
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 10000);
    return () => clearInterval(interval);
  }, []);

  const getHealthIndicator = () => {
    if (healthError) {
      return (
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-red-400 animate-pulse" />
          <span className="text-xs text-red-400 font-medium">Disconnected</span>
        </div>
      );
    }
    if (!health) {
      return (
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-gray-500 animate-pulse" />
          <span className="text-xs text-gray-400 font-medium">Connecting...</span>
        </div>
      );
    }
    const isHealthy = health.status === "healthy" || health.status === "ok";
    return (
      <div className="flex items-center gap-2">
        <div className={`w-2 h-2 rounded-full ${isHealthy ? "bg-emerald-400" : "bg-amber-400"} animate-pulse`} />
        <span className={`text-xs font-medium ${isHealthy ? "text-emerald-400" : "text-amber-400"}`}>
          {isHealthy ? "All Systems Operational" : "Degraded"}
        </span>
        {health.components && (
          <span className="text-xs text-gray-500 ml-2">
            {Object.keys(health.components).length} components
          </span>
        )}
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-gray-950">
      {/* Background gradient */}
      <div className="fixed inset-0 bg-gradient-to-br from-blue-950/20 via-gray-950 to-purple-950/10 pointer-events-none" />

      {/* Header */}
      <header className="relative border-b border-gray-800/50 backdrop-blur-sm bg-gray-950/80 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              {/* Logo / Icon */}
              <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 to-blue-700 flex items-center justify-center shadow-lg shadow-blue-600/20">
                <svg className="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
              </div>
              <div>
                <h1 className="text-lg font-bold text-gray-100 tracking-tight">
                  Stream-Table OLAP Engine
                </h1>
                <p className="text-xs text-gray-500">
                  Real-time analytics with sub-second latency
                </p>
              </div>
            </div>

            {/* Health status */}
            <div className="flex items-center gap-4">
              {getHealthIndicator()}
              <div className="h-6 w-px bg-gray-800" />
              <span className="text-xs text-gray-500 font-mono">
                {new Date().toLocaleDateString("en-US", {
                  weekday: "short",
                  month: "short",
                  day: "numeric",
                })}
              </span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="relative max-w-7xl mx-auto px-6 py-6 space-y-6">
        {/* Top Row: Throughput Gauge */}
        <section>
          <ThroughputGauge />
        </section>

        {/* Middle: Timeline Slider */}
        <section>
          <TimelineSlider />
        </section>

        {/* Bottom: Query Playground */}
        <section>
          <QueryPlayground />
        </section>
      </main>

      {/* Footer */}
      <footer className="relative border-t border-gray-800/50 mt-12">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between text-xs text-gray-600">
            <span>Stream-Table OLAP Engine v0.1.0</span>
            <span>Backend: localhost:8000</span>
          </div>
        </div>
      </footer>
    </div>
  );
}
