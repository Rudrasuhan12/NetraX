import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import alertsRoute from "./routes/alerts";
import uploadRoute from "./routes/upload";

dotenv.config();

const app = express();

// Enable CORS so your Vercel frontend can talk to your Render backend
app.use(cors());
app.use(express.json());

// Health check endpoint for Render to verify the server is live
app.get("/", (req, res) => {
  res.send("NetraX Backend Running 🚀");
});

// Routes
app.use("/api", uploadRoute);
app.use("/api", alertsRoute);

// Dynamic Port Assignment for Cloud Deployment
const PORT = process.env.PORT || 5000;

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});