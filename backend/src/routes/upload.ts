import express from "express";
import multer from "multer";
import { Storage } from "@google-cloud/storage";

const router = express.Router();

// ✅ memory storage
const upload = multer({ storage: multer.memoryStorage() });

// GCP storage
const storage = new Storage();
const bucketName = "netrax-video-storage";

router.post("/upload", upload.single("video"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).send("No file uploaded");
    }

    const bucket = storage.bucket(bucketName);

    const blob = bucket.file(Date.now() + "-" + req.file.originalname);

    const stream = blob.createWriteStream({
      resumable: false,
    });

    stream.on("finish", () => {
      res.send("✅ Uploaded to GCP!");
    });

    stream.on("error", (err) => {
      console.error(err);
      res.status(500).send("Upload failed");
    });

    stream.end(req.file.buffer);
  } catch (err) {
    console.error(err);
    res.status(500).send("Server error");
  }
});

export default router;