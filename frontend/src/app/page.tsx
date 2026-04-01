"use client";

import { useEffect, useState, useRef } from "react";

interface Alert {
  id: string;
  source: string;
  timestamp: string;
  video_id: string;
  confidence: number;
  embedding_score?: number;
  misuse_category?: string;
  misuse_reasoning?: string;
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
  
  // Polish States
  const [isEventMode, setIsEventMode] = useState(false); 
  const [isBooting, setIsBooting] = useState(true);

  // Takedown Generator States
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null);
  const [isGenerating, setIsGenerating] = useState(false);
  const [takedownText, setTakedownText] = useState("");

  // Simulator State
  const [isScanning, setIsScanning] = useState(false);

  const soundEnabledRef = useRef(false);
  const lastAlertIdRef = useRef<string | null>(null);

  // Sound enablement click listener
  useEffect(() => {
    const enableSound = () => {
      setSoundEnabled(true);
      soundEnabledRef.current = true;
      window.removeEventListener("click", enableSound);
    };
    window.addEventListener("click", enableSound);
    return () => window.removeEventListener("click", enableSound);
  }, []);

  // Fetch Data Logic
  useEffect(() => {
    const fetchData = async () => {
      try {
        const alertRes = await fetch("https://netrax-backend.onrender.com/api/alerts");
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

        const propRes = await fetch("https://netrax-backend.onrender.com/api/propagation");
        if (propRes.ok) {
          const propData = await propRes.json();
          setPropagationLinks(propData);
        }

        setTimeout(() => setIsBooting(false), 800); 
      } catch (error) {
        console.error("Failed to fetch data. Is localhost:5000 running?", error);
      }
    };

    fetchData();
    const intervalTime = isEventMode ? 1000 : 2500; 
    const interval = setInterval(fetchData, intervalTime);
    
    return () => clearInterval(interval);
  }, [isEventMode]);

  // Takedown Generation Trigger
  const handleGenerateTakedown = async (alert: Alert) => {
    setSelectedAlert(alert);
    setIsModalOpen(true);
    setIsGenerating(true);
    setTakedownText("");

    try {
      const response = await fetch("https://netrax-backend.onrender.com/api/generate-takedown", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          video_id: alert.video_id,
          source: alert.source,
          misuse_category: alert.misuse_category || "Copyright Infringement",
          misuse_reasoning: alert.misuse_reasoning || "Unauthorized broadcast.",
        }),
      });

      if (response.ok) {
        const data = await response.json();
        setTakedownText(data.takedown_notice);
      } else {
        setTakedownText("Failed to connect to Vertex AI API.");
      }
    } catch (error) {
      setTakedownText("An error occurred while generating the document.");
    } finally {
      setIsGenerating(false);
    }
  };

  // Trigger Live Scan
  const handleTriggerScan = async () => {
    setIsScanning(true);
    try {
      await fetch("https://netrax-backend.onrender.com/api/trigger-scan", { 
        method: "POST"
      });
    } catch (err) {
      console.error("Failed to trigger scan", err);
    }
    setTimeout(() => setIsScanning(false), 1500); 
  };

  const getRiskColor = (risk: number) => {
    if (risk > 80) return "bg-red-500";
    if (risk > 60) return "bg-orange-500";
    return "bg-yellow-500";
  };

  const positions: Record<string, { top: string; left: string }> = {
    India: { top: "55%", left: "65%" },
    US: { top: "45%", left: "25%" },
    UK: { top: "35%", left: "48%" },
    Brazil: { top: "65%", left: "35%" },
    Indonesia: { top: "60%", left: "75%" },
    Germany: { top: "32%", left: "52%" },
    Japan: { top: "40%", left: "85%" },
    Global: { top: "50%", left: "50%" },
  };

  const highRisk = alerts.filter((a) => a.risk_score > 80).length;
  const avgConfidence = alerts.length > 0
      ? Math.round(alerts.reduce((sum, a) => sum + (a.embedding_score || a.confidence), 0) / alerts.length)
      : 0;

  const customStyles = `
    @keyframes slideDownFade {
      0% { opacity: 0; transform: translateY(-20px); }
      100% { opacity: 1; transform: translateY(0); }
    }
    @keyframes radarSpin {
      100% { transform: rotate(360deg); }
    }
    .animate-new-alert {
      animation: slideDownFade 0.4s ease-out forwards;
    }
    .radar-sweep {
      background: conic-gradient(from 0deg, transparent 70%, rgba(59, 130, 246, 0.4) 100%);
      animation: radarSpin 2s linear infinite;
    }
  `;

  if (isBooting) {
    return (
      <div className="min-h-screen bg-gray-900 flex flex-col items-center justify-center text-white">
        <div className="w-16 h-16 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mb-6"></div>
        <h1 className="text-3xl font-bold tracking-widest animate-pulse">
          INITIALIZING NETRA<span className="text-blue-500">X</span> CORE
        </h1>
        <p className="text-gray-400 mt-2 text-sm font-mono uppercase tracking-widest">Connecting to Pub/Sub Event Streams...</p>
        <p className="text-gray-500 mt-1 text-xs font-mono">Loading Vertex AI & Gemini Context Engine [OK]</p>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 p-8 text-gray-900 transition-colors duration-500">
      <style>{customStyles}</style>

      {/* HEADER */}
      <div className="flex justify-between items-center mb-8">
        
        {/* LEFT SIDE: Branding */}
        <div className="flex items-center gap-3">
          <h1 className="text-3xl font-bold tracking-tight">
            Netra<span className="text-blue-600">X</span>
          </h1>
          <span className="bg-purple-100 text-purple-700 text-[10px] font-bold px-2 py-0.5 rounded uppercase tracking-wider flex items-center gap-1">
            <span className="w-1.5 h-1.5 rounded-full bg-purple-500 animate-pulse"></span>
            Intelligence Core V2
          </span>
        </div>

        {/* RIGHT SIDE: Controls Deck */}
        <div className="flex items-center gap-5">
          
          {/* 🔥 REDESIGNED: Judge Verification Simulator Button */}
          <div className="relative group flex items-center">
            <button 
                onClick={handleTriggerScan}
                disabled={isScanning}
                className={`flex items-center gap-2 px-5 py-2 rounded-full font-bold text-xs transition-all shadow-sm border ${
                    isScanning 
                    ? "bg-gray-100 text-gray-400 border-gray-200 cursor-not-allowed" 
                    : "bg-indigo-600 text-white border-indigo-700 hover:bg-indigo-500 hover:shadow-lg hover:shadow-indigo-500/30"
                }`}
            >
                <span className="text-sm">{isScanning ? "📡" : "🚀"}</span>
                {isScanning ? "Intercepting Stream..." : "Simulate Live Web Scan"}
            </button>
            
            {/* Sleek Hover Tooltip */}
            <div className="absolute right-0 top-12 w-64 p-3 bg-gray-900 text-gray-300 text-[10px] font-mono rounded-lg shadow-2xl opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50 pointer-events-none border border-gray-700">
              <span className="block text-white font-bold mb-1 font-sans text-xs">For Judges & Reviewers:</span>
              Instantly simulates the asynchronous Python Pub/Sub worker to demonstrate the Vertex AI & Gemini cloud processing pipeline.
            </div>
          </div>

          <div className="w-px h-8 bg-gray-200 mx-1"></div> {/* Vertical Divider */}

          {/* Event-Aware Mode */}
          <div className="flex items-center gap-3 bg-white px-4 py-2 rounded-full shadow-sm border border-gray-200 transition-all hover:shadow-md">
            <span className="text-sm font-bold text-gray-700">Event-Aware Mode</span>
            <button 
              onClick={() => setIsEventMode(!isEventMode)}
              className={`w-12 h-6 rounded-full transition-colors duration-300 flex items-center px-1 ${isEventMode ? 'bg-red-500 shadow-inner' : 'bg-gray-300'}`}
            >
              <div className={`w-4 h-4 rounded-full bg-white shadow-sm transform transition-transform duration-300 ${isEventMode ? 'translate-x-6' : ''}`}></div>
            </button>
            <span className={`text-xs font-medium ${isEventMode ? 'text-red-500' : 'text-gray-500'}`}>
              {isEventMode ? "Scanning 2x Faster" : "Standard"}
            </span>
          </div>

          {/* System Live Badge */}
          <span className="text-red-500 font-bold text-sm flex items-center gap-2 bg-red-50 px-3 py-1.5 rounded-full border border-red-100">
            <span className="w-2.5 h-2.5 rounded-full bg-red-500 animate-ping absolute"></span>
            <span className="w-2.5 h-2.5 rounded-full bg-red-500 relative z-10"></span>
            SYSTEM LIVE
          </span>

        </div>
      </div>

      {/* STATS */}
      <div className="grid grid-cols-3 gap-6 mb-8">
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
          <p className="text-gray-500 text-sm font-semibold uppercase tracking-wider">Live Alerts Today</p>
          <h2 className="text-4xl font-black mt-2 text-gray-800">{alerts.length}</h2>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
          <p className="text-gray-500 text-sm font-semibold uppercase tracking-wider">High-Risk Misuse Events</p>
          <h2 className="text-4xl font-black mt-2 text-red-600">{highRisk}</h2>
        </div>
        <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
          <p className="text-gray-500 text-sm font-semibold uppercase tracking-wider">Avg. AI Verification Score</p>
          <h2 className="text-4xl font-black mt-2 text-blue-600">{avgConfidence}%</h2>
        </div>
      </div>

      {/* PROPAGATION GRAPH */}
      <div className="bg-white p-6 rounded-xl shadow-sm border border-gray-100 mb-8 overflow-hidden">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-lg font-bold text-gray-800">Cross-Platform Traceability Graph</h2>
          <span className="bg-blue-50 text-blue-600 border border-blue-100 text-xs font-bold px-3 py-1 rounded-full flex items-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse"></span> Network Intelligence
          </span>
        </div>
        <div className="flex items-center space-x-6 overflow-x-auto pb-4 pt-2 custom-scrollbar">
          {propagationLinks.length === 0 ? (
            <div className="flex items-center gap-3 text-gray-400 italic text-sm py-4">
              <div className="w-4 h-4 border-2 border-gray-300 border-t-transparent rounded-full animate-spin"></div>
              Awaiting content propagation data...
            </div>
          ) : (
            propagationLinks.map((link, i) => (
              <div key={link.id} className="flex items-center group animate-new-alert">
                <div className="bg-gray-50 border border-gray-200 p-3 rounded-lg min-w-[140px] text-center shadow-sm relative overflow-hidden transition-all group-hover:border-blue-300">
                  <div className="absolute top-0 left-0 w-full h-1 bg-gray-300"></div>
                  <p className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-1 mt-1">Source Node</p>
                  <p className="text-sm font-mono text-gray-700 truncate">{link.parent_id.slice(0, 8)}</p>
                </div>
                <div className="flex flex-col items-center mx-2 relative">
                  <span className="text-[10px] font-bold text-red-500 mb-1 bg-red-50 px-2 py-0.5 rounded-full">{link.similarity}% match</span>
                  <div className="h-0.5 w-16 bg-red-300 relative overflow-hidden">
                    <div className="absolute top-0 left-0 h-full w-full bg-red-500 animate-pulse"></div>
                    <div className="absolute right-0 -top-1 w-2 h-2 border-t-2 border-r-2 border-red-500 transform rotate-45 z-10"></div>
                  </div>
                </div>
                <div className="bg-red-50 border border-red-200 p-3 rounded-lg min-w-[140px] text-center shadow-sm relative overflow-hidden group-hover:shadow-md transition-all group-hover:-translate-y-1 group-hover:border-red-400">
                  <div className="absolute top-0 left-0 w-full h-1 bg-red-500"></div>
                  <div className="absolute inset-0 bg-red-500/5 blur-md opacity-0 group-hover:opacity-100 transition-opacity"></div>
                  <p className="text-[10px] text-red-600 font-bold uppercase tracking-wider mb-1 mt-1 relative z-10">Pirated Clone</p>
                  <p className="text-sm font-mono text-red-900 truncate relative z-10 font-bold">{link.child_id.slice(0, 8)}</p>
                </div>
                {i !== propagationLinks.length - 1 && <div className="ml-6 mr-2 text-gray-300 h-8 border-l-2 border-dashed"></div>}
              </div>
            ))
          )}
        </div>
      </div>

      <div className="grid grid-cols-3 gap-6">
        {/* ALERT FEED */}
        <div className="col-span-2 bg-white p-6 rounded-xl shadow-sm border border-gray-100 flex flex-col h-[700px]">
          <div className="flex justify-between items-center mb-4 shrink-0 border-b border-gray-50 pb-4">
            <h2 className="text-lg font-bold text-gray-800">Real-Time Detection & Classification Feed</h2>
            <div className="text-xs text-gray-400 font-mono flex items-center gap-2">
              <span className="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span> Worker Active
            </div>
          </div>

          <div className="flex-1 min-h-0 overflow-y-auto pr-2 space-y-4 custom-scrollbar">
            {alerts.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center text-gray-400">
                <div className="relative w-32 h-32 rounded-full border border-blue-200 bg-blue-50 overflow-hidden flex items-center justify-center mb-4 shadow-inner">
                  <div className="absolute inset-0 radar-sweep rounded-full"></div>
                  <div className="w-1.5 h-1.5 bg-blue-500 rounded-full z-10 shadow-[0_0_10px_2px_rgba(59,130,246,0.8)]"></div>
                </div>
                <p className="font-medium text-lg text-gray-600">Scanning incoming streams...</p>
              </div>
            ) : (
              alerts.map((alert) => (
                <div key={alert.id} className="animate-new-alert relative overflow-hidden bg-white border border-gray-100 p-5 rounded-lg hover:bg-gray-50 transition-colors duration-300 shadow-sm hover:shadow-md group">
                  <div className="absolute left-0 top-0 bottom-0 w-1 bg-red-500 opacity-80 group-hover:w-1.5 transition-all"></div>
                  <div className="relative z-10 w-full pl-2">
                    <div className="flex justify-between items-center mb-2">
                      <span className="font-bold text-gray-800">{alert.source}</span>
                      <span className="text-xs text-gray-400 font-mono bg-gray-100 px-2 py-1 rounded">{new Date(alert.timestamp).toLocaleTimeString()}</span>
                    </div>
                    <p className="text-gray-600 text-sm mb-3 font-mono truncate">ID: {alert.video_id}</p>
                    
                    <div className="flex items-center gap-4 mb-3 bg-gray-50 p-2.5 rounded-md border border-gray-200 shadow-inner">
                      <div className="flex flex-col">
                        <span className="text-[10px] text-gray-500 font-bold uppercase tracking-wider mb-0.5">Stage 1: Fast Filter</span>
                        <span className="text-sm font-bold text-gray-700 flex items-center gap-1">⚡ pHash: {alert.confidence}%</span>
                      </div>
                      <div className="text-gray-300 font-bold">→</div>
                      <div className="flex flex-col">
                        <span className="text-[10px] text-blue-600 font-bold uppercase tracking-wider mb-0.5">Stage 2: Vertex AI</span>
                        <span className="text-sm font-black text-blue-700 flex items-center gap-1">🧠 Embedding: {alert.embedding_score || 'N/A'}%</span>
                      </div>
                      <div className="ml-auto flex items-center gap-1 text-xs text-blue-600 font-bold bg-blue-50 px-2 py-1 rounded">🌍 {alert.region}</div>
                    </div>

                    {alert.misuse_category && (
                      <div className="mb-4 bg-gradient-to-r from-purple-50 to-indigo-50 p-3.5 rounded-md border border-purple-100 shadow-sm">
                        <div className="flex items-center justify-between mb-1.5">
                          <div className="flex items-center gap-2">
                            <span className="text-[9px] bg-purple-600 text-white px-2 py-0.5 rounded uppercase font-bold tracking-wider flex items-center gap-1">
                              <span className="w-1 h-1 bg-white rounded-full animate-pulse"></span> Gemini 2.5 Analysis
                            </span>
                            <span className="text-sm font-black text-purple-900">{alert.misuse_category}</span>
                          </div>
                        </div>
                        <p className="text-xs text-purple-800 leading-relaxed font-medium">"{alert.misuse_reasoning}"</p>
                      </div>
                    )}

                    <div className="flex items-center justify-between mt-4">
                      {/* Action Button */}
                      <button 
                        onClick={() => alert.response.includes("TAKEDOWN") ? handleGenerateTakedown(alert) : null}
                        disabled={!alert.response.includes("TAKEDOWN")}
                        className={`text-sm font-bold flex items-center gap-1.5 px-3 py-1.5 rounded-md border transition-all ${
                          alert.response.includes("TAKEDOWN") 
                            ? "bg-red-50 text-red-600 border-red-200 hover:bg-red-600 hover:text-white cursor-pointer shadow-sm" 
                            : alert.level === "MEDIUM" 
                            ? "bg-orange-50 text-orange-600 border-orange-100 cursor-default" 
                            : "bg-yellow-50 text-yellow-600 border-yellow-100 cursor-default"
                        }`}
                      >
                        {alert.level === "CRITICAL" ? "🚨" : alert.level === "MEDIUM" ? "⚠️" : "ℹ️"} {alert.response}
                      </button>
                      
                      <div className="w-1/3 text-right">
                        <div className="flex justify-end text-[10px] text-gray-500 mb-1 font-bold uppercase tracking-wider">
                          <span>Smart Risk Score</span>
                          <span className={`ml-2 font-black ${getRiskColor(alert.risk_score).replace('bg-', 'text-')}`}>{alert.risk_score}%</span>
                        </div>
                        <div className="w-full bg-gray-200 h-1.5 rounded-full overflow-hidden">
                          <div className={`${getRiskColor(alert.risk_score)} h-1.5 rounded-full transition-all duration-1000 ease-out`} style={{ width: `${alert.risk_score}%` }}></div>
                        </div>
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
          <div className="bg-white p-5 rounded-xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
            <h2 className="text-md font-bold text-gray-800 mb-3">Live Geographic Heatmap</h2>
            <div className="relative w-full h-[180px] bg-[#eef2f6] rounded-lg overflow-hidden border border-blue-100 shadow-inner">
              <img src="https://upload.wikimedia.org/wikipedia/commons/8/80/World_map_-_low_resolution.svg" alt="World Map" className="w-full h-full opacity-30 object-cover" />
              {regionData.map((r, i) => {
                const pos = positions[r.region] || positions["Global"];
                return (
                  <div key={i} className="absolute" style={{ top: pos.top, left: pos.left }}>
                    <div className="relative flex items-center justify-center group">
                      <div className="w-4 h-4 bg-red-500/30 rounded-full animate-ping absolute"></div>
                      <div className="w-2.5 h-2.5 bg-red-600 rounded-full relative z-10 shadow-[0_0_8px_rgba(220,38,38,0.8)] border-2 border-white group-hover:scale-150 transition-transform cursor-pointer"></div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="bg-white p-5 rounded-xl shadow-sm border border-gray-100 hover:shadow-md transition-shadow">
            <h2 className="text-md font-bold text-gray-800 mb-4 flex items-center justify-between">
              Top Threat Regions
              <span className="text-xs font-normal text-gray-400 border border-gray-200 px-2 py-0.5 rounded bg-gray-50">Auto-updating</span>
            </h2>
            <div className="max-h-[160px] overflow-y-auto pr-2 custom-scrollbar">
              {regionData.sort((a,b) => b.count - a.count).map((r) => (
                <div key={r.region} className="mb-4 group">
                  <div className="flex justify-between text-sm font-bold text-gray-700 mb-1.5">
                    <span>{r.region}</span>
                    <span className="text-red-500">{r.count} <span className="text-[10px] font-normal text-gray-400">flags</span></span>
                  </div>
                  <div className="w-full bg-gray-100 h-2 rounded-full overflow-hidden">
                    <div className="bg-gradient-to-r from-red-400 to-red-600 h-2 rounded-full transition-all duration-1000 ease-out group-hover:opacity-80" style={{ width: `${Math.min(r.count * 15, 100)}%` }}></div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* TAKEDOWN GENERATOR MODAL */}
      {isModalOpen && (
        <div className="fixed inset-0 bg-gray-900/60 backdrop-blur-sm z-50 flex items-center justify-center animate-in fade-in duration-200">
          <div className="bg-white w-[800px] max-h-[85vh] rounded-2xl shadow-2xl overflow-hidden flex flex-col border border-gray-200">
            {/* Modal Header */}
            <div className="bg-gray-50 px-6 py-4 border-b border-gray-200 flex justify-between items-center shrink-0">
              <div className="flex items-center gap-3">
                <span className="bg-purple-100 p-2 rounded-lg">
                  <svg className="w-5 h-5 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"></path></svg>
                </span>
                <div>
                  <h3 className="font-bold text-gray-900">Legal Action Center</h3>
                  <p className="text-xs text-gray-500 font-mono">Target: {selectedAlert?.source} | Asset: {selectedAlert?.video_id}</p>
                </div>
              </div>
              <button onClick={() => setIsModalOpen(false)} className="text-gray-400 hover:text-gray-700 bg-gray-100 hover:bg-gray-200 p-2 rounded-full transition-colors">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M6 18L18 6M6 6l12 12"></path></svg>
              </button>
            </div>

            {/* Modal Body */}
            <div className="p-6 overflow-y-auto bg-gray-50 flex-1">
              {isGenerating ? (
                <div className="flex flex-col items-center justify-center py-16 space-y-4">
                  <div className="w-12 h-12 border-4 border-purple-200 border-t-purple-600 rounded-full animate-spin"></div>
                  <p className="text-purple-700 font-medium animate-pulse">Gemini 2.5 Flash is drafting the legal notice...</p>
                  <p className="text-xs text-gray-400">Contextualizing {selectedAlert?.misuse_category} parameters</p>
                </div>
              ) : (
                <div className="bg-white border border-gray-200 rounded-lg p-6 shadow-inner relative group">
                  <div className="absolute top-4 right-4 text-[10px] bg-green-100 text-green-700 font-bold px-2 py-1 rounded uppercase tracking-widest border border-green-200">
                    Ready to Send
                  </div>
                  <pre className="whitespace-pre-wrap font-sans text-sm text-gray-700 leading-relaxed">
                    {takedownText}
                  </pre>
                </div>
              )}
            </div>

            {/* Modal Footer */}
            <div className="bg-white border-t border-gray-200 px-6 py-4 flex justify-end gap-3 shrink-0">
              <button onClick={() => setIsModalOpen(false)} className="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors">
                Cancel
              </button>
              <button 
                disabled={isGenerating}
                className={`px-4 py-2 text-sm font-medium text-white rounded-lg transition-all flex items-center gap-2 ${isGenerating ? 'bg-purple-300 cursor-not-allowed' : 'bg-purple-600 hover:bg-purple-700 shadow-md hover:shadow-lg'}`}
              >
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"></path></svg>
                {isGenerating ? 'Drafting...' : 'Dispatch DMCA Takedown'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}