import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import http from "http";
import { Server, Socket } from "socket.io";
import alertsRoute from "./routes/alerts";
import uploadRoute from "./routes/upload";
import ingestRoute from "./routes/ingest";
import * as admin from "firebase-admin";

dotenv.config();

const app = express();
const server = http.createServer(app);

// Socket.IO WebSocket server
const io = new Server(server, {
  cors: {
    origin: process.env.FRONTEND_URL || "http://localhost:3000",
    methods: ["GET", "POST"]
  }
});

// Enable CORS so your Vercel frontend can talk to your Render backend
app.use(cors());
app.use(express.json());

// Health check endpoint for Render to verify the server is live
app.get("/", (req, res) => {
  res.send("NetraX Backend Running 🚀");
});

// Routes
app.use("/api", uploadRoute);
app.use("/api", ingestRoute);
app.use("/api", alertsRoute);

// Socket.IO connection handler
io.on('connection', (socket: Socket) => {
  console.log(`🔌 Client connected: ${socket.id}`);
  
  socket.on('disconnect', () => {
    console.log(`❌ Client disconnected: ${socket.id}`);
  });
  
  // Allow clients to request live alert updates
  socket.on('subscribe-alerts', () => {
    console.log(`👂 Client subscribed to alerts: ${socket.id}`);
    socket.join('alerts');
  });
});

// Listen to Firestore alerts collection for real-time changes
const db = admin.firestore();

// Set up real-time listener for new alerts
db.collection("alerts")
  .orderBy("timestamp", "desc")
  .limit(1)
  .onSnapshot((snapshot) => {
    snapshot.docChanges().forEach((change) => {
      if (change.type === "added") {
        const alert = change.doc.data();
        
        console.log(`📨 New alert detected: ${alert.video_id}`);
        
        // Convert Firestore timestamp to JS Date (handle various formats)
        let timestampDate: Date;
        if (alert.timestamp) {
          if (typeof alert.timestamp.toDate === 'function') {
            // Firestore Timestamp object
            timestampDate = alert.timestamp.toDate();
          } else if (alert.timestamp instanceof Date) {
            // Already a Date object
            timestampDate = alert.timestamp;
          } else if (typeof alert.timestamp === 'string') {
            // String date
            timestampDate = new Date(alert.timestamp);
          } else if (alert.timestamp._seconds) {
            // Firestore Timestamp-like object with _seconds
            timestampDate = new Date(alert.timestamp._seconds * 1000);
          } else {
            timestampDate = new Date();
          }
        } else {
          timestampDate = new Date();
        }
        
        // Broadcast to all connected clients
        io.to('alerts').emit('new-alert', {
          id: change.doc.id,
          ...alert,
          timestamp: timestampDate
        });
        
        // Send email for CRITICAL alerts
        if (alert.level === "CRITICAL") {
          sendCriticalAlert({
            ...alert,
            timestamp: timestampDate
          });
        }
      }
    });
  }, (error) => {
    console.error("❌ Firestore listener error:", error);
  });

// Helper function to send critical alert emails
async function sendCriticalAlert(alert: any) {
  // Implement email notifications
  console.log(`🚨 CRITICAL alert would be emailed: ${alert.video_id}`);
  
  // Email implementation goes here (using nodemailer)
  // See email.ts module for implementation
}

// Dynamic Port Assignment for Cloud Deployment
const PORT = process.env.PORT || 5000;

server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`   WebSocket enabled for real-time alerts`);
});