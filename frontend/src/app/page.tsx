"use client";

import { useEffect, useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

interface Alert {
  id: string;
  source: string;
  timestamp: string;
  video_id: string;
  confidence: number;
  risk_score: number;
  region: string;
}

export default function Dashboard() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [regionData, setRegionData] = useState<any[]>([]);
  const [soundEnabled, setSoundEnabled] = useState(false);

  // 🔊 Enable sound
  useEffect(() => {
    const enableSound = () => {
      setSoundEnabled(true);
      window.removeEventListener("click", enableSound);
    };
    window.addEventListener("click", enableSound);
  }, []);

  const playAlertSound = () => {
    if (!soundEnabled) return;
    const audio = new Audio(
      "https://actions.google.com/sounds/v1/alarms/alarm_clock.ogg"
    );
    audio.play().catch(() => {});
  };

  // 🔁 Fetch alerts
  useEffect(() => {
    const fetchAlerts = async () => {
      try {
        const res = await fetch("http://localhost:5000/api/alerts");
        const data = await res.json();

        // 🔥 Play sound only for NEW alerts
        if (alerts.length > 0 && data[0]?.id !== alerts[0]?.id) {
          playAlertSound();
        }

        setAlerts(data);

        const counts: any = {};
        data.forEach((a: Alert) => {
          const region = a.region || "Global";
          counts[region] = (counts[region] || 0) + 1;
        });

        const chartData = Object.keys(counts).map((key) => ({
          region: key,
          count: counts[key],
        }));

        setRegionData(chartData);
      } catch (err) {
        console.error(err);
      }
    };

    fetchAlerts();
    const interval = setInterval(fetchAlerts, 2000);
    return () => clearInterval(interval);
  }, [alerts]);

  const getRiskColor = (risk: number) => {
    if (risk > 80) return "bg-red-500";
    if (risk > 60) return "bg-orange-500";
    return "bg-yellow-500";
  };

  // 🌍 Map positions
  const positions: any = {
    India: { top: "55%", left: "65%" },
    US: { top: "45%", left: "25%" },
    UK: { top: "35%", left: "48%" },
    Brazil: { top: "65%", left: "35%" },
    Indonesia: { top: "60%", left: "75%" },
    Global: { top: "50%", left: "50%" },
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-[#0f172a] to-[#020617] text-white p-10 font-sans">

      {/* 🔊 SOUND NOTICE */}
      {!soundEnabled && (
        <div className="bg-yellow-400 text-black px-4 py-2 rounded mb-4 text-center">
          🔊 Click anywhere to enable alert sound
        </div>
      )}

      {/* HEADER */}
      <header className="mb-10">
        <h1 className="text-5xl font-extrabold text-blue-400">
          NetraX Intelligence Dashboard
        </h1>
        <p className="text-gray-400 mt-2">
          Real-time piracy detection & monitoring system
        </p>
      </header>

      <div className="grid grid-cols-3 gap-8">

        {/* ALERTS */}
        <div className="col-span-2 bg-[#1e293b] rounded-2xl p-6 shadow-xl border border-gray-700">
          <h2 className="text-2xl font-bold mb-6 text-red-400">
            🚨 Live Alerts
          </h2>

          <div className="space-y-5 max-h-[70vh] overflow-y-auto">
            {alerts.length === 0 ? (
              <p className="text-gray-400">Scanning streams...</p>
            ) : (
              alerts.map((alert) => (
                <div
                  key={alert.id}
                  className="relative bg-[#334155] p-5 rounded-xl border border-gray-600 hover:border-red-500 transition duration-300 shadow-md hover:shadow-red-500/20 animate-pulse"
                >
                  <div className="absolute inset-0 bg-red-500/10 blur-xl opacity-0 hover:opacity-100"></div>

                  <div className="flex justify-between">
                    <span className="font-bold">{alert.source}</span>
                    <span className="text-xs">
                      {new Date(alert.timestamp).toLocaleTimeString()}
                    </span>
                  </div>

                  <p className="mt-1">🎬 {alert.video_id}</p>
                  <p className="text-sm text-gray-400">
                    🌍 {alert.region} | 📊 {alert.confidence}%
                  </p>

                  <div className="mt-3">
                    <div className="w-full bg-gray-600 h-2 rounded">
                      <div
                        className={`${getRiskColor(alert.risk_score)} h-2 rounded`}
                        style={{ width: `${alert.risk_score}%` }}
                      ></div>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* RIGHT SIDE */}
        <div className="space-y-6">

          {/* 🌍 MAP */}
          <div className="bg-[#1e293b] p-4 rounded-xl">
            <h2 className="text-lg mb-3 text-orange-400">🌍 Live Map</h2>

            <div className="relative w-full h-[250px] bg-[#0f172a] rounded-lg overflow-hidden">

              <img
                src="https://upload.wikimedia.org/wikipedia/commons/8/80/World_map_-_low_resolution.svg"
                className="w-full h-full opacity-40"
              />

              {regionData.map((r, i) => {
                const pos = positions[r.region] || positions["Global"];

                return (
                  <div
                    key={i}
                    className="absolute"
                    style={{ top: pos.top, left: pos.left }}
                  >
                    <div className="relative">
                      <div className="w-3 h-3 bg-red-500 rounded-full animate-ping absolute"></div>
                      <div className="w-3 h-3 bg-red-500 rounded-full"></div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* 📊 CHART */}
          <div className="bg-[#1e293b] p-4 rounded-xl">
            <h2 className="text-lg mb-3 text-blue-400">📊 Region Stats</h2>

            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={regionData}>
                <XAxis dataKey="region" stroke="#ccc" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="count" fill="#ef4444" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* 📊 SUMMARY */}
          <div className="bg-[#1e293b] p-4 rounded-xl">
            <h2 className="text-lg mb-3 text-green-400">📊 Overview</h2>

            <p>Total Alerts: {alerts.length}</p>
            <p>Regions: {regionData.length}</p>
            <p>
              High Risk: {alerts.filter((a) => a.risk_score > 80).length}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}