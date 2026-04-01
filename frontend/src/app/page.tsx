"use client";

import { useEffect, useState, useRef } from "react";
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
  response: string;
  level: string;
}

interface PropagationLink {
  id: string;
  parent_id: string;
  child_id: string;
  similarity: number;
}

export default function Dashboard() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [propagationLinks, setPropagationLinks] = useState<PropagationLink[]>([]);
  const [regionData, setRegionData] = useState<any[]>([]);
  const [soundEnabled, setSoundEnabled] = useState(false);
  
  // 🔥 NEW: Event-Aware Mode Toggle
  const [isEventMode, setIsEventMode] = useState(false); 

  const soundEnabledRef = useRef(false);
  const lastAlertIdRef = useRef<string | null>(null);

  useEffect(() => {
    const enableSound = () => {
      setSoundEnabled(true);
      soundEnabledRef.current = true;
      window.removeEventListener("click", enableSound);
    };
    window.addEventListener("click", enableSound);
    return () => window.removeEventListener("click", enableSound);
  }, []);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch Alerts
        const alertRes = await fetch("http://localhost:5000/api/alerts");
        if (alertRes.ok) {
          const alertData = await alertRes.json();
          
          const latestAlertId = alertData[0]?.id;
          if (lastAlertIdRef.current && latestAlertId !== lastAlertIdRef.current) {
            if (soundEnabledRef.current) {
              const audio = new Audio("https://actions.google.com/sounds/v1/alarms/alarm_clock.ogg");
              audio.play().catch(() => {});
            }
          }
          if (latestAlertId) lastAlertIdRef.current = latestAlertId;

          setAlerts(alertData);

          const counts: any = {};
          alertData.forEach((a: Alert) => {
            const region = a.region || "Global";
            counts[region] = (counts[region] || 0) + 1;
          });

          setRegionData(Object.keys(counts).map((key) => ({ region: key, count: counts[key] })));
        }

        // 🔥 NEW: Fetch Propagation Data
        const propRes = await fetch("http://localhost:5000/api/propagation");
        if (propRes.ok) {
          const propData = await propRes.json();
          setPropagationLinks(propData);
        }

      } catch (error) {
        console.error("Failed to fetch data. Is localhost:5000 running?", error);
      }
    };

    fetchData();
    // If Event Mode is ON, scan 2x faster!
    const intervalTime = isEventMode ? 1000 : 2500; 
    const interval = setInterval(fetchData, intervalTime);
    
    return () => clearInterval(interval);
  }, [isEventMode]); // Re-run effect if mode changes to update scan rate

  const getRiskColor = (risk: number) => {
    if (risk > 80) return "bg-red-500";
    if (risk > 60) return "bg-orange-500";
    return "bg-yellow-500";
  };

  const getLevelColor = (level: string) => {
    if (level === "HIGH") return "text-red-600";
    if (level === "MEDIUM") return "text-orange-500";
    return "text-yellow-600";
  };

  const positions: Record<string, { top: string; left: string }> = {
    India: { top: "55%", left: "65%" },
    US: { top: "45%", left: "25%" },
    UK: { top: "35%", left: "48%" },
    Brazil: { top: "65%", left: "35%" },
    Indonesia: { top: "60%", left: "75%" },
    Global: { top: "50%", left: "50%" },
  };

  const highRisk = alerts.filter((a) => a.risk_score > 80).length;
  const avgConfidence = alerts.length > 0
      ? Math.round(alerts.reduce((sum, a) => sum + a.confidence, 0) / alerts.length)
      : 0;

  return (
    <div className="min-h-screen bg-gray-50 p-8 text-gray-900">
      {!soundEnabled && (
        <div className="bg-yellow-300 text-black px-4 py-2 rounded mb-4 text-center cursor-pointer shadow-sm">
          🔊 Click anywhere to enable alert sound
        </div>
      )}

      {/* HEADER */}
      <div className="flex justify-between items-center mb-8">
        <h1 className="text-3xl font-bold">
          Netra<span className="text-blue-500">X</span>
        </h1>
        
        {/* 🔥 NEW: Event-Aware Toggle */}
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-3 bg-white px-4 py-2 rounded-full shadow-sm border border-gray-200">
            <span className="text-sm font-bold text-gray-700">Event-Aware Mode</span>
            <button 
              onClick={() => setIsEventMode(!isEventMode)}
              className={`w-12 h-6 rounded-full transition-colors flex items-center px-1 ${isEventMode ? 'bg-red-500' : 'bg-gray-300'}`}
            >
              <div className={`w-4 h-4 rounded-full bg-white shadow-sm transform transition-transform ${isEventMode ? 'translate-x-6' : ''}`}></div>
            </button>
            <span className="text-xs text-gray-500">{isEventMode ? "Scanning 2x Faster" : "Standard"}</span>
          </div>

          <span className="text-red-500 font-semibold text-sm flex items-center gap-2">
            <span className="animate-pulse">●</span> LIVE
          </span>
        </div>
      </div>

      {/* STATS */}
      <div className="grid grid-cols-3 gap-6 mb-8">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-gray-500 text-sm font-medium">Live Alerts Today</p>
          <h2 className="text-3xl font-bold mt-2">{alerts.length}</h2>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-gray-500 text-sm font-medium">High-Risk Events</p>
          <h2 className="text-3xl font-bold mt-2">{highRisk}</h2>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100">
          <p className="text-gray-500 text-sm font-medium">Avg. Confidence</p>
          <h2 className="text-3xl font-bold mt-2">{avgConfidence}%</h2>
        </div>
      </div>

      {/* 🔥 NEW: PROPAGATION GRAPH */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 mb-8 overflow-hidden">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-semibold">Content Propagation Tracking (Node Graph)</h2>
          <span className="bg-blue-100 text-blue-700 text-xs font-bold px-2 py-1 rounded">NetraX Unique Edge</span>
        </div>
        <div className="flex items-center space-x-6 overflow-x-auto pb-4 pt-2">
          {propagationLinks.length === 0 ? (
            <p className="text-gray-400 italic text-sm">Waiting for content to spread...</p>
          ) : (
            propagationLinks.map((link, i) => (
              <div key={link.id} className="flex items-center group">
                <div className="bg-gray-50 border border-gray-200 p-3 rounded-lg min-w-[140px] text-center shadow-sm">
                  <p className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-1">Source Node</p>
                  <p className="text-sm font-mono text-gray-700 truncate">{link.parent_id.slice(0, 8)}...</p>
                </div>
                
                {/* Arrow */}
                <div className="flex flex-col items-center mx-2">
                  <span className="text-[10px] font-bold text-red-500 mb-1">{link.similarity}% match</span>
                  <div className="h-0.5 w-12 bg-red-300 relative">
                    <div className="absolute right-0 -top-1 w-2 h-2 border-t-2 border-r-2 border-red-300 transform rotate-45"></div>
                  </div>
                </div>

                <div className="bg-red-50 border border-red-200 p-3 rounded-lg min-w-[140px] text-center shadow-sm relative overflow-hidden">
                  <div className="absolute inset-0 bg-red-500/10 blur-md opacity-0 group-hover:opacity-100 transition-opacity"></div>
                  <p className="text-[10px] text-red-600 font-bold uppercase tracking-wider mb-1 relative z-10">Pirated Clone</p>
                  <p className="text-sm font-mono text-red-900 truncate relative z-10">{link.child_id.slice(0, 8)}...</p>
                </div>
                
                {i !== propagationLinks.length - 1 && <div className="ml-6 mr-2 text-gray-300 h-8 border-l-2 border-dashed"></div>}
              </div>
            ))
          )}
        </div>
      </div>

      {/* MAIN GRID */}
      <div className="grid grid-cols-3 gap-6">
        {/* ALERT FEED */}
        <div className="col-span-2 bg-white p-6 rounded-xl shadow-sm border border-gray-100 flex flex-col h-[600px]">
          <h2 className="text-lg font-semibold mb-4 shrink-0">Real-Time Event Feed</h2>
          <div className="flex-1 min-h-0 overflow-y-auto pr-2 space-y-4 custom-scrollbar">
            {alerts.length === 0 ? (
              <p className="text-gray-400 italic">Waiting for alerts...</p>
            ) : (
              alerts.map((alert) => (
                <div key={alert.id} className="relative overflow-hidden bg-white border border-gray-100 p-5 rounded-lg hover:bg-gray-50 transition duration-300 shadow-sm group">
                  <div className="absolute inset-0 bg-red-500/5 blur-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none"></div>
                  <div className="relative z-10 w-full">
                    <div className="flex justify-between items-center mb-2">
                      <span className="font-bold text-gray-800">{alert.source}</span>
                      <span className="text-xs text-gray-500 font-medium">{new Date(alert.timestamp).toLocaleTimeString()}</span>
                    </div>
                    <p className="text-gray-700 text-sm mb-1">🎬 {alert.video_id}</p>
                    <p className="text-sm text-gray-500 mb-2">🌍 {alert.region} | 📊 {alert.confidence}%</p>
                    <p className="text-sm text-blue-600 font-medium">⚡ {alert.response}</p>
                    <p className={`text-xs mt-1 font-bold ${getLevelColor(alert.level)}`}>Level: {alert.level}</p>
                    <div className="mt-3">
                      <div className="w-full bg-gray-200 h-1.5 rounded-full overflow-hidden">
                        <div className={`${getRiskColor(alert.risk_score)} h-1.5 rounded-full transition-all duration-500`} style={{ width: `${alert.risk_score}%` }}></div>
                      </div>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* RIGHT PANEL */}
        <div className="space-y-6">
          <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100">
            <h2 className="text-lg font-semibold mb-2">Global Activity</h2>
            <div className="relative w-full h-[150px] bg-blue-50/50 rounded overflow-hidden border border-gray-100">
              <img src="https://upload.wikimedia.org/wikipedia/commons/8/80/World_map_-_low_resolution.svg" alt="World Map" className="w-full h-full opacity-40 object-cover" />
              {regionData.map((r, i) => {
                const pos = positions[r.region] || positions["Global"];
                return (
                  <div key={i} className="absolute" style={{ top: pos.top, left: pos.left }}>
                    <div className="relative flex items-center justify-center">
                      <div className="w-3 h-3 bg-red-500 rounded-full animate-ping absolute"></div>
                      <div className="w-3 h-3 bg-red-500 rounded-full relative z-10"></div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="bg-white p-4 rounded-xl shadow-sm border border-gray-100">
            <h2 className="text-lg font-semibold mb-3">Top Regions by Activity</h2>
            <div className="max-h-[160px] overflow-y-auto pr-2">
              {regionData.map((r) => (
                <div key={r.region} className="mb-3">
                  <div className="flex justify-between text-sm font-medium mb-1">
                    <span>{r.region}</span>
                    <span>{r.count}</span>
                  </div>
                  <div className="w-full bg-gray-100 h-2 rounded-full overflow-hidden">
                    <div className="bg-blue-500 h-2 rounded-full transition-all duration-500" style={{ width: `${Math.min(r.count * 20, 100)}%` }}></div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}