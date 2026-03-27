"use client";

import { useEffect, useState } from "react";
import { collection, onSnapshot, query, orderBy } from "firebase/firestore";
import { db } from "../firebase";

export default function Dashboard() {
  const [alerts, setAlerts] = useState<any[]>([]);

  useEffect(() => {
    const q = query(collection(db, "alerts"), orderBy("timestamp", "desc"));
    
    const unsubscribe = onSnapshot(q, (snapshot) => {
      const liveAlerts = snapshot.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
      }));
      setAlerts(liveAlerts);
    });

    return () => unsubscribe();
  }, []);

  return (
    <div className="min-h-screen bg-gray-900 text-white p-10">
      <h1 className="text-4xl font-bold mb-8 text-blue-400">NetraX Live Monitoring</h1>
      
      <div className="bg-gray-800 rounded-lg p-6 shadow-lg">
        <h2 className="text-2xl font-semibold mb-4 text-red-400">🚨 Live Piracy Alerts</h2>
        
        {alerts.length === 0 ? (
          <p className="text-gray-400">Scanning streams... No piracy detected yet.</p>
        ) : (
          <div className="space-y-4">
            {alerts.map((alert) => (
              <div key={alert.id} className="border-l-4 border-red-500 bg-gray-700 p-4 rounded">
                <div className="flex justify-between">
                  <span className="font-bold text-lg">{alert.source || "Simulated Stream"}</span>
                  <span className="text-sm text-gray-400">{new Date(alert.timestamp).toLocaleTimeString()}</span>
                </div>
                <div className="mt-2 text-gray-300">
                  Video ID: <span className="text-white">{alert.video_id}</span>
                </div>
                <div className="mt-2 text-green-400 font-bold">
                  Confidence: {alert.confidence}%
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}