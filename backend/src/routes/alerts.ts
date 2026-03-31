import express from "express";
import admin from "firebase-admin";

const router = express.Router();

// ✅ Initialize Firebase (only once)
if (!admin.apps.length) {
  const serviceAccount = require("../../serviceAccountKey.json");

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

const db = admin.firestore();

// ✅ GET ALERTS (Clean + Sorted + Fixed Timestamp)
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

      // 🔥 Fix Firestore Timestamp object
      if (formattedTimestamp && formattedTimestamp._seconds) {
        formattedTimestamp = new Date(
          formattedTimestamp._seconds * 1000
        ).toISOString();
      }

      return {
        id: doc.id,
        video_id: data.video_id || "unknown",
        confidence: data.confidence || 0,
        risk_score: data.risk_score || 0,
        region: data.region || "unknown",
        status: data.status || "UNKNOWN",
        source: data.source || "unknown",
        timestamp: formattedTimestamp,
      };
    });

    res.json(alerts);
  } catch (error) {
    console.error("❌ Error fetching alerts:", error);
    res.status(500).json({ error: "Failed to fetch alerts" });
  }
});

export default router;