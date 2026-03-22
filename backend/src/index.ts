import express from "express";
import cors from "cors";
import dotenv from "dotenv";

import uploadRoute from "./routes/upload";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());


app.get("/", (req, res) => {
  res.send("NetraX Backend Running 🚀");
});

app.use("/api", uploadRoute);
const PORT = process.env.PORT || 5000;

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});