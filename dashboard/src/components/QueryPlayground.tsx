"use client";

import { useState, useCallback } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { fetchQuery, QueryResult } from "@/lib/api";

interface HistoryEntry {
  query: string;
  timestamp: Date;
}

export default function QueryPlayground() {
  const [query, setQuery] = useState("SELECT 'Stream-Table OLAP Engine' as engine, 42 as answer, current_timestamp as now");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [showHistory, setShowHistory] = useState(false);

  const executeQuery = useCallback(async () => {
    if (!query.trim()) return;

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const res = await fetchQuery(query);
      if (res.error) {
        setError(res.error);
      } else {
        setResult(res);
      }
      // Add to history
      setHistory((prev) => [
        { query: query.trim(), timestamp: new Date() },
        ...prev.slice(0, 9),
      ]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query execution failed");
    } finally {
      setLoading(false);
    }
  }, [query]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      executeQuery();
    }
  };

  // Check if result has numeric columns for chart
  const getChartData = (): { data: Record<string, unknown>[]; numericKeys: string[]; labelKey: string } | null => {
    if (!result || !result.rows || result.rows.length === 0) return null;

    // Convert array rows to object rows using column names
    const objectRows: Record<string, unknown>[] = result.rows.map((row) => {
      if (Array.isArray(row)) {
        const obj: Record<string, unknown> = {};
        result.columns.forEach((col, idx) => { obj[col] = row[idx]; });
        return obj;
      }
      return row as Record<string, unknown>;
    });

    const numericKeys = result.columns.filter((col) =>
      objectRows.every((row) => typeof row[col] === "number")
    );

    if (numericKeys.length === 0) return null;

    const labelKey =
      result.columns.find((col) => !numericKeys.includes(col)) || numericKeys[0];

    return { data: objectRows, numericKeys, labelKey };
  };

  const chartInfo = result ? getChartData() : null;

  const CHART_COLORS = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899"];

  return (
    <div className="bg-gray-900/80 backdrop-blur-sm border border-gray-800 rounded-2xl p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <svg className="w-5 h-5 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <h2 className="text-lg font-semibold text-gray-200">Query Playground</h2>
        </div>
        <div className="flex items-center gap-3">
          {/* History dropdown */}
          <div className="relative">
            <button
              onClick={() => setShowHistory(!showHistory)}
              className="text-xs text-gray-400 hover:text-gray-200 px-3 py-1.5 rounded-lg border border-gray-700 hover:border-gray-600 transition-colors"
            >
              History ({history.length})
            </button>
            {showHistory && history.length > 0 && (
              <div className="absolute right-0 top-full mt-2 w-80 bg-gray-800 border border-gray-700 rounded-xl shadow-2xl z-50 overflow-hidden">
                <div className="p-2 max-h-64 overflow-y-auto">
                  {history.map((entry, i) => (
                    <button
                      key={i}
                      onClick={() => {
                        setQuery(entry.query);
                        setShowHistory(false);
                      }}
                      className="w-full text-left px-3 py-2 rounded-lg hover:bg-gray-700/50 transition-colors group"
                    >
                      <p className="text-xs text-gray-300 font-mono truncate group-hover:text-blue-400">
                        {entry.query}
                      </p>
                      <p className="text-[10px] text-gray-500 mt-0.5">
                        {entry.timestamp.toLocaleTimeString()}
                      </p>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
          <span className="text-xs text-gray-500">Ctrl+Enter to run</span>
        </div>
      </div>

      {/* Editor */}
      <div className="rounded-xl overflow-hidden border border-gray-700/50 mb-4" onKeyDown={handleKeyDown}>
        <CodeMirror
          value={query}
          onChange={(val) => setQuery(val)}
          extensions={[sql()]}
          theme="dark"
          height="140px"
          basicSetup={{
            lineNumbers: true,
            highlightActiveLine: true,
            bracketMatching: true,
            autocompletion: true,
          }}
        />
      </div>

      {/* Run button */}
      <div className="flex items-center gap-4 mb-4">
        <button
          onClick={executeQuery}
          disabled={loading || !query.trim()}
          className="px-5 py-2.5 bg-blue-600 hover:bg-blue-500 disabled:bg-gray-700 disabled:text-gray-500
            text-white text-sm font-medium rounded-xl transition-all duration-200
            shadow-lg shadow-blue-600/20 hover:shadow-blue-500/30
            disabled:shadow-none flex items-center gap-2"
        >
          {loading ? (
            <>
              <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              Running...
            </>
          ) : (
            <>
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
              </svg>
              Run Query
            </>
          )}
        </button>

        {result && (
          <span className="text-xs text-gray-400">
            {result.rows.length} row{result.rows.length !== 1 ? "s" : ""} in{" "}
            <span className="text-blue-400 font-mono">{result.execution_time_ms}ms</span>
          </span>
        )}
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 p-4 bg-red-950/30 border border-red-800/50 rounded-xl">
          <div className="flex items-start gap-2">
            <svg className="w-4 h-4 text-red-400 mt-0.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <pre className="text-sm text-red-300 font-mono whitespace-pre-wrap break-all">
              {error}
            </pre>
          </div>
        </div>
      )}

      {/* Results Table */}
      {result && result.rows.length > 0 && (
        <div className="mb-4 overflow-hidden rounded-xl border border-gray-700/50">
          <div className="overflow-x-auto max-h-72">
            <table className="w-full text-sm">
              <thead className="bg-gray-800/80 sticky top-0">
                <tr>
                  {result.columns.map((col) => (
                    <th
                      key={col}
                      className="px-4 py-3 text-left text-xs font-semibold text-gray-400 uppercase tracking-wider border-b border-gray-700"
                    >
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-800">
                {result.rows.map((row, i) => (
                  <tr key={i} className="hover:bg-gray-800/30 transition-colors">
                    {result.columns.map((col, colIdx) => {
                      const value = Array.isArray(row) ? row[colIdx] : row[col];
                      return (
                        <td key={col} className="px-4 py-2.5 text-gray-300 font-mono text-xs whitespace-nowrap">
                          {value === null || value === undefined ? (
                            <span className="text-gray-600 italic">null</span>
                          ) : (
                            String(value)
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Chart visualization */}
      {chartInfo && (
        <div className="bg-gray-800/30 rounded-xl p-4 border border-gray-700/30">
          <p className="text-xs text-gray-400 mb-3 font-medium uppercase tracking-wide">
            Visualization
          </p>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartInfo.data as Record<string, string | number>[]} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                <XAxis
                  dataKey={chartInfo.labelKey}
                  tick={{ fontSize: 10, fill: "#9ca3af" }}
                  axisLine={{ stroke: "#374151" }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fontSize: 10, fill: "#9ca3af" }}
                  axisLine={{ stroke: "#374151" }}
                  tickLine={false}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: "#1f2937",
                    border: "1px solid #374151",
                    borderRadius: "8px",
                    color: "#e5e7eb",
                    fontSize: "12px",
                  }}
                />
                {chartInfo.numericKeys.map((key, i) => (
                  <Bar
                    key={key}
                    dataKey={key}
                    fill={CHART_COLORS[i % CHART_COLORS.length]}
                    radius={[4, 4, 0, 0]}
                    opacity={0.85}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Empty state */}
      {!result && !error && !loading && (
        <div className="text-center py-8">
          <svg className="w-12 h-12 mx-auto text-gray-700 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
          </svg>
          <p className="text-sm text-gray-500">Run a SQL query to see results</p>
          <p className="text-xs text-gray-600 mt-1">
            Try: SELECT * FROM joined_events LIMIT 10
          </p>
        </div>
      )}
    </div>
  );
}
