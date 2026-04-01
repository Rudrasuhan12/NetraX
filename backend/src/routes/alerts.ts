import express from "express";
import admin from "firebase-admin";

const router = express.Router();

if (!admin.apps.length) {
  const serviceAccount = require("../../serviceAccountKey.json");

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

const db = admin.firestore();

// Fetch Alerts Feed
router.get("/alerts", async (req, res) => {
  try {
    const snapshot = await db
      .collection("alerts")
      .orderBy("timestamp", "desc")
      .limit(20)
      .get();

    const alerts = snapshot.docs.map((doc: any) => {
      const data = doc.data();

      let formattedTimestamp = data.timestamp;

      if (formattedTimestamp && formattedTimestamp._seconds) {
        formattedTimestamp = new Date(
          formattedTimestamp._seconds * 1000
        ).toISOString();
      }

      return {
        id: doc.id,
        video_id: data.video_id || "unknown",
        confidence: data.confidence || 0,
        embedding_score: data.embedding_score || null,  // 🔥 NEW: Pass the AI score to the frontend
        risk_score: data.risk_score || 0,
        region: data.region || "unknown",
        status: data.status || "UNKNOWN",
        source: data.source || "unknown",
        response: data.response || "No action",   
        level: data.level || "UNKNOWN",           
        timestamp: formattedTimestamp,
      };
    });

    res.json(alerts);
  } catch (error) {
    console.error("❌ Error fetching alerts:", error);
    res.status(500).json({ error: "Failed to fetch alerts" });
  }
});

// 🔥 NEW: Fetch Propagation Tracking Data for the UI Graph
router.get("/propagation", async (req, res) => {
  try {
    const snapshot = await db
      .collection("propagation_links")
      .orderBy("timestamp", "desc")
      .limit(10)
      .get();

    const links = snapshot.docs.map((doc: any) => {
      const data = doc.data();
      return {
        id: doc.id,
        parent_id: data.parent_id || "unknown",
        child_id: data.child_id || "unknown",
        similarity: data.similarity || 0,
      };
    });

    res.json(links);
  } catch (error) {
    console.error("❌ Error fetching propagation links:", error);
    res.status(500).json({ error: "Failed to fetch propagation links" });
  }
});

export default router;