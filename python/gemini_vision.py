"""
Gemini Vision API Integration for Real Image Analysis
Uses Google Generative AI (Gemini 2.0 Flash) with actual image data
"""
import base64
import json
import logging
import os
import cv2
import numpy as np
from google import genai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Gemini client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "serviceAccountKey.json"

client = genai.Client(
    vertexai=True,
    project="bwai-solution-challenge",
    location="us-central1"
)


def encode_frame_to_base64(frame_array):
    """
    Convert numpy array frame to base64 JPEG for API transmission
    """
    try:
        success, buffer = cv2.imencode('.jpg', frame_array, [cv2.IMWRITE_JPEG_QUALITY, 85])
        if success:
            return base64.b64encode(buffer).decode('utf-8')
        else:
            logger.error("Failed to encode frame to JPEG")
            return None
    except Exception as e:
        logger.error(f"Error encoding frame: {e}")
        return None


def analyze_frame_with_gemini_vision(frame_array, video_id):
    """
    Gemini 2.0 Flash Vision Analysis
    Analyzes actual frame image for misuse classification
    
    Returns:
        {
            "category": "Raw Broadcast Piracy" | "Meme/Fan Edit" | "Deepfake/AI Alteration" | "Fair Use News",
            "reasoning": "explanation of the classification",
            "confidence": float (0-1),
            "visual_indicators": list of detected visual elements
        }
    """
    
    if frame_array is None:
        logger.error("Invalid frame array")
        return {
            "category": "UNKNOWN",
            "reasoning": "Unable to analyze null frame",
            "confidence": 0.0
        }
    
    try:
        # Encode frame to base64
        logger.info("🖼️ Encoding frame for Gemini Vision API...")
        frame_base64 = encode_frame_to_base64(frame_array)
        
        if not frame_base64:
            logger.error("Failed to encode frame")
            return {
                "category": "UNKNOWN",
                "reasoning": "Frame encoding failed",
                "confidence": 0.0
            }
        
        # Construct the prompt
        prompt = """You are a digital forensics AI specializing in copyright violation detection.
        
Analyze this video frame and classify the type of misuse into ONE of these exact categories:

1. **Raw Broadcast Piracy** - Unedited, direct stream capture or full replay of official broadcast/match. Minimal editing.
2. **Meme/Fan Edit** - Heavily edited content with text overlays, stickers, filters, fast cuts, background music added. Transformative.
3. **Deepfake/AI Alteration** - Faces synthetically swapped, morphed faces, AI-generated alterations, unnatural facial movements.
4. **Fair Use News** - Clip used for commentary, news reporting, picture-in-picture with host commentary, educational use.

RESPOND ONLY in this exact JSON format:
{
    "category": "one of the four options above",
    "reasoning": "brief explanation (1-2 sentences) of why you classified it this way",
    "confidence": 0.0 to 1.0,
    "visual_indicators": ["list", "of", "detected", "elements"]
}

Do NOT respond with any other text. Only valid JSON."""

        logger.info("📤 Sending frame to Gemini 2.0 Flash Vision API...")
        
        # Call Gemini 2.0 with vision capability
        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=[
                prompt,
                {
                    "inline_data": {
                        "mime_type": "image/jpeg",
                        "data": frame_base64
                    }
                }
            ]
        )
        
        logger.info("✅ Received response from Gemini Vision API")
        
        # Parse the response
        response_text = response.text.strip()
        
        # Handle markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text[7:]  # Remove ```json
        if response_text.startswith("```"):
            response_text = response_text[3:]  # Remove ```
        if response_text.endswith("```"):
            response_text = response_text[:-3]  # Remove trailing ```
        
        response_text = response_text.strip()
        
        logger.info(f"📝 Raw response: {response_text[:200]}...")
        
        analysis = json.loads(response_text)
        
        # Validate required fields
        if "category" not in analysis:
            logger.error("Missing 'category' field in Gemini response")
            return {
                "category": "UNKNOWN",
                "reasoning": "Invalid Gemini response format",
                "confidence": 0.0
            }
        
        logger.info(f"✅ Classification: {analysis['category']}")
        logger.info(f"📊 Confidence: {analysis.get('confidence', 0.0)}")
        
        return analysis
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ Failed to parse Gemini response as JSON: {e}")
        logger.error(f"Response was: {response_text if 'response_text' in locals() else 'N/A'}")
        # Return fallback based on response content
        return {
            "category": "Raw Broadcast Piracy",
            "reasoning": "Unable to parse detailed analysis, defaulting to high-risk classification",
            "confidence": 0.7
        }
        
    except Exception as e:
        logger.error(f"❌ Gemini Vision API error: {e}")
        return {
            "category": "UNKNOWN",
            "reasoning": f"API error: {str(e)}",
            "confidence": 0.0
        }


def analyze_frame_with_gemini_metadata(video_id, source, embedding_score):
    """
    Fallback: Analyze based on metadata if vision API fails
    Uses text-based analysis instead of image vision
    """
    try:
        logger.info("📝 Using metadata-based Gemini analysis (fallback)...")
        
        prompt = f"""Analyze this metadata of a suspected copyright violation and classify it.

Video ID: {video_id}
Source: {source}
AI Similarity Score: {embedding_score}%

Classify into ONE category:
1. Raw Broadcast Piracy
2. Meme/Fan Edit
3. Deepfake/AI Alteration
4. Fair Use News

Respond ONLY in JSON format:
{{"category": "...", "reasoning": "...", "confidence": 0.0}}"""

        response = client.models.generate_content(
            model="gemini-2.0-flash-exp",
            contents=prompt
        )
        
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        analysis = json.loads(response_text.strip())
        return analysis
        
    except Exception as e:
        logger.error(f"Metadata analysis failed: {e}")
        return {
            "category": "Raw Broadcast Piracy",
            "reasoning": "Fallback due to API error",
            "confidence": 0.6
        }
