"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { fetchWindows, WindowData } from "@/lib/api";

interface TimelinePoint {
  time: string;
  label: string;
  count: number;
  timestamp: string;
}

export default function TimelineSlider() {
  const [data, setData] = useState<TimelinePoint[]>([]);
  const [sliderValue, setSliderValue] = useState(100);
  const [isDragging, setIsDragging] = useState(false);
  const [selectedWindow, setSelectedWindow] = useState<WindowData | null>(null);
  const [loading, setLoading] = useState(false);
  const intervalRef = useRef<NodeJS.Timeout | null>(null);

  const WINDOW_MINUTES = 30;
  const TICK_COUNT = 60; // One point per 30 seconds

  // Generate timeline data points
  const generateTimelinePoints = useCallback((): TimelinePoint[] => {
    const now = new Date();
    const points: TimelinePoint[] = [];

    for (let i = 0; i < TICK_COUNT; i++) {
      const t = new Date(now.getTime() - (TICK_COUNT - 1 - i) * 30000);
      points.push({
        time: t.toISOString(),
        label: t.toLocaleTimeString("en-US", {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hour12: false,
        }),
        count: Math.floor(Math.random() * 1000 + 500), // Simulated density until real data loads
        timestamp: t.toISOString(),
      });
    }

    return points;
  }, []);

  useEffect(() => {
    setData(generateTimelinePoints());

    // Auto-advance when not dragging
    intervalRef.current = setInterval(() => {
      if (!isDragging) {
        setData(generateTimelinePoints());
        setSliderValue(100);
      }
    }, 5000);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [isDragging, generateTimelinePoints]);

  // Fetch window data on slider release
  const handleSliderChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = parseInt(e.target.value);
    setSliderValue(val);
    setIsDragging(true);
  };

  const handleSliderRelease = async () => {
    setIsDragging(false);

    if (data.length === 0) return;

    const index = Math.floor((sliderValue / 100) * (data.length - 1));
    const point = data[index];

    if (point) {
      setLoading(true);
      try {
        const windows = await fetchWindows(point.timestamp);
        if (windows.length > 0) {
          setSelectedWindow(windows[0]);
        }
      } catch {
        // API might not be available yet
        setSelectedWindow(null);
      } finally {
        setLoading(false);
      }
    }
  };

  const currentIndex = Math.floor((sliderValue / 100) * (data.length - 1));
  const currentPoint = data[currentIndex];

  return (
    <div className="bg-gray-900/80 backdrop-blur-sm border border-gray-800 rounded-2xl p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <svg className="w-5 h-5 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <h2 className="text-lg font-semibold text-gray-200">Event Timeline</h2>
          <span className="text-xs text-gray-500 font-mono">Last {WINDOW_MINUTES} min</span>
        </div>
        {currentPoint && (
          <span className="text-sm text-blue-400 font-mono tabular-nums">
            {currentPoint.label}
          </span>
        )}
      </div>

      {/* Area Chart */}
      <div className="h-32 mb-4">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data} margin={{ top: 5, right: 5, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="areaGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4} />
                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="label"
              tick={{ fontSize: 10, fill: "#6b7280" }}
              axisLine={{ stroke: "#374151" }}
              tickLine={false}
              interval={Math.floor(data.length / 6)}
            />
            <YAxis hide />
            <Tooltip
              contentStyle={{
                backgroundColor: "#1f2937",
                border: "1px solid #374151",
                borderRadius: "8px",
                color: "#e5e7eb",
                fontSize: "12px",
              }}
              labelStyle={{ color: "#9ca3af" }}
              formatter={(value: number) => [`${value.toLocaleString()} events`, "Count"]}
            />
            <Area
              type="monotone"
              dataKey="count"
              stroke="#3b82f6"
              strokeWidth={2}
              fill="url(#areaGradient)"
              animationDuration={300}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Slider */}
      <div className="relative px-1">
        <input
          type="range"
          min={0}
          max={100}
          value={sliderValue}
          onChange={handleSliderChange}
          onMouseUp={handleSliderRelease}
          onTouchEnd={handleSliderRelease}
          className="w-full h-2 bg-gray-800 rounded-full appearance-none cursor-pointer
            [&::-webkit-slider-thumb]:appearance-none
            [&::-webkit-slider-thumb]:w-4
            [&::-webkit-slider-thumb]:h-4
            [&::-webkit-slider-thumb]:rounded-full
            [&::-webkit-slider-thumb]:bg-blue-500
            [&::-webkit-slider-thumb]:shadow-[0_0_10px_rgba(59,130,246,0.5)]
            [&::-webkit-slider-thumb]:cursor-grab
            [&::-webkit-slider-thumb]:active:cursor-grabbing
            [&::-moz-range-thumb]:w-4
            [&::-moz-range-thumb]:h-4
            [&::-moz-range-thumb]:rounded-full
            [&::-moz-range-thumb]:bg-blue-500
            [&::-moz-range-thumb]:border-none
            [&::-moz-range-thumb]:cursor-grab"
        />
        <div className="flex justify-between mt-2 text-xs text-gray-500">
          <span>-{WINDOW_MINUTES}m</span>
          <span>{isDragging ? "Release to query" : "Now"}</span>
        </div>
      </div>

      {/* Window Result */}
      {loading && (
        <div className="mt-4 flex items-center justify-center gap-2 text-sm text-gray-400">
          <div className="w-4 h-4 border-2 border-blue-400 border-t-transparent rounded-full animate-spin" />
          Querying window...
        </div>
      )}

      {selectedWindow && !loading && (
        <div className="mt-4 bg-gray-800/50 rounded-xl p-4 border border-gray-700/50">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-1.5 h-1.5 rounded-full bg-blue-400" />
            <span className="text-xs text-gray-400 font-medium uppercase tracking-wide">
              Window Aggregation
            </span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-xs text-gray-500">Start</p>
              <p className="text-sm text-gray-300 font-mono">
                {new Date(selectedWindow.window_start).toLocaleTimeString()}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500">End</p>
              <p className="text-sm text-gray-300 font-mono">
                {new Date(selectedWindow.window_end).toLocaleTimeString()}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500">Events</p>
              <p className="text-sm text-blue-400 font-bold">
                {selectedWindow.count.toLocaleString()}
              </p>
            </div>
            <div>
              <p className="text-xs text-gray-500">Aggregations</p>
              <p className="text-sm text-gray-300">
                {Object.keys(selectedWindow.aggregations).length} fields
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
