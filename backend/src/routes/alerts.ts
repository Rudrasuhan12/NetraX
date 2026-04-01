import express from "express";
import admin from "firebase-admin";
import path from "path";
import { GoogleGenAI } from "@google/genai"; // 🔥 NEW: Unified Google Gen AI SDK

const router = express.Router();

// Load your existing Google Cloud Service Account
const serviceAccount = require("../../serviceAccountKey.json");

// Tell the Google Cloud SDK exactly where to find your credentials
process.env.GOOGLE_APPLICATION_CREDENTIALS = path.resolve(__dirname, "../../serviceAccountKey.json");

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

const db = admin.firestore();

// 🔥 NEW: Initialize the unified GenAI SDK for Vertex AI using your GCP Project ID
const ai = new GoogleGenAI({
  vertexai: true,
  project: serviceAccount.project_id,
  location: "us-central1", // Standard GCP region
});

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
        formattedTimestamp = new Date(formattedTimestamp._seconds * 1000).toISOString();
      }

      return {
        id: doc.id,
        video_id: data.video_id || "unknown",
        confidence: data.confidence || 0,
        embedding_score: data.embedding_score || null,  
        misuse_category: data.misuse_category || null,    
        misuse_reasoning: data.misuse_reasoning || null,  
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

// Fetch Propagation Tracking Data
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

// 🔥 NEW: Enterprise Vertex AI Takedown Generator (Updated SDK)
router.post("/generate-takedown", async (req, res) => {
  try {
    const { video_id, source, misuse_category, misuse_reasoning } = req.body;

    const prompt = `
      You are an expert legal AI representing "NetraX Broadcasting Corporation".
      Draft a formal, concise DMCA Takedown Notice addressed to the legal department of ${source}.
      
      Here are the specifics of the copyright infringement:
      - Video ID/Asset Tracker: ${video_id}
      - Infringement Classification: ${misuse_category}
      - Forensic AI Analysis: ${misuse_reasoning}

      Write the letter so it is ready to be sent immediately. Make it sound highly professional, citing the Digital Millennium Copyright Act. Emphasize that the AI analysis confirms this is not fair use.
    `;

    console.log(`🤖 Generating Takedown via Vertex AI for ${source} (${video_id})...`);
    
    // Use the new SDK generation method with Gemini 2.5 Flash
    const response = await ai.models.generateContent({
        model: "gemini-2.5-flash", 
        contents: prompt
    });
    
    const takedownText = response.text || "Failed to generate text.";

    console.log("✅ Takedown Notice Generated Successfully!");

    res.json({ takedown_notice: takedownText });
  } catch (error) {
    console.error("❌ Error generating takedown via Vertex AI:", error);
    res.status(500).json({ error: "Failed to generate takedown notice." });
  }
});

export default router;