"""
Twitch API - Real-Time Sports Clip Scraper
Monitors Twitch for sports clips and publishes to Pub/Sub
"""

import os
import json
import time
import base64
import requests
import logging
from datetime import datetime, timedelta
import numpy as np
from PIL import Image
from io import BytesIO

from google.cloud import pubsub_v1
from publisher import publisher, topic_path
from hash import generate_hash
from db import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Twitch API credentials
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_ACCESS_TOKEN = os.getenv("TWITCH_ACCESS_TOKEN")

# Sports category IDs on Twitch
SPORTS_CATEGORIES = {
    "esports": "509658",
    "sports": "1469308723"
}

# Top sports streamers/broadcasters
SPORTS_CHANNELS = [
    "ESL_CSGO",
    "ESGN_LCS",
    "OWL",
    "VictorsFury",
    "esl_sc2"
]


def get_twitch_token():
    """
    Get OAuth token for Twitch API
    Uses Client Credentials flow
    """
    try:
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": os.getenv("TWITCH_CLIENT_SECRET"),
            "grant_type": "client_credentials"
        }
        response = requests.post(url, params=params, timeout=10)
        
        if response.status_code == 200:
            token = response.json()["access_token"]
            logger.info("✅ Twitch token obtained")
            return token
        else:
            logger.error(f"Failed to get Twitch token: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error getting Twitch token: {e}")
        return None


def get_twitch_headers(access_token):
    """Generate Twitch API headers"""
    return {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


def download_and_hash_clip_thumbnail(thumbnail_url):
    """
    Download clip thumbnail and generate perceptual hash
    """
    try:
        response = requests.get(thumbnail_url, timeout=5)
        if response.status_code != 200:
            logger.warning(f"Failed to download clip thumbnail")
            return None
        
        # Convert image bytes to array
        img = Image.open(BytesIO(response.content))
        img_array = np.array(img)
        
        # Generate hash
        hash_value = generate_hash(img_array)
        
        # Encode as base64 for Gemini Vision
        img_bytes = BytesIO()
        img.save(img_bytes, format='JPEG')
        img_base64 = base64.b64encode(img_bytes.getvalue()).decode('utf-8')
        
        return {
            "hash": hash_value,
            "image_base64": img_base64
        }
        
    except Exception as e:
        logger.warning(f"Error processing clip thumbnail: {e}")
        return None


def scrape_twitch_clips():
    """
    Fetch top sports clips from Twitch
    """
    access_token = get_twitch_token()
    if not access_token:
        logger.error("Failed to get Twitch access token")
        return
    
    headers = get_twitch_headers(access_token)
    
    logger.info("🎮 Starting Twitch Clips Scraper...")
    logger.info(f"Monitoring {len(SPORTS_CATEGORIES)} sports categories")
    
    try:
        for category_name, category_id in SPORTS_CATEGORIES.items():
            logger.info(f"\n📺 Fetching {category_name} clips (ID: {category_id})...")
            
            try:
                # Get trending clips in sports category
                url = "https://api.twitch.tv/helix/clips"
                
                # Calculate date from 7 days ago
                started_at = (datetime.utcnow() - timedelta(days=7)).isoformat() + "Z"
                
                params = {
                    "category_id": category_id,
                    "first": 10,
                    "started_at": started_at,
                    "ended_at": datetime.utcnow().isoformat() + "Z"
                }
                
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if response.status_code != 200:
                    logger.error(f"Twitch API error: {response.status_code}")
                    continue
                
                clips = response.json().get("data", [])
                
                if not clips:
                    logger.info(f"   No clips found for {category_name}")
                    continue
                
                logger.info(f"   Found {len(clips)} clips")
                
                for clip in clips:
                    clip_id = clip["id"]
                    title = clip["title"]
                    broadcaster_name = clip["broadcaster_name"]
                    creator_name = clip["creator_name"]
                    thumbnail_url = clip["thumbnail_url"]
                    view_count = clip["view_count"]
                    created_at = clip["created_at"]
                    
                    logger.info(f"   🎥 Found clip: {title[:40]}... ({view_count:,} views)")
                    
                    # Download and hash thumbnail
                    hash_data = download_and_hash_clip_thumbnail(thumbnail_url)
                    
                    if not hash_data:
                        logger.warning(f"   Skipping {clip_id} - hash generation failed")
                        continue
                    
                    # Prepare Pub/Sub message
                    payload = {
                        "hash": hash_data["hash"],
                        "video_id": f"twitch_clip_{clip_id}",
                        "source": f"Twitch Clip ({broadcaster_name})",
                        "platform": "twitch",
                        "broadcaster": broadcaster_name,
                        "creator": creator_name,
                        "title": title,
                        "view_count": view_count,
                        "created_at": created_at,
                        "thumbnail_url": thumbnail_url,
                        "frame_base64": hash_data["image_base64"],
                        "twitch_url": f"https://twitch.tv/clip/{clip_id}",
                        "metadata": {
                            "category": category_name,
                            "clip_id": clip_id,
                            "broadcaster_name": broadcaster_name
                        }
                    }
                    
                    # Publish to Pub/Sub
                    try:
                        future = publisher.publish(
                            topic_path,
                            data=json.dumps(payload).encode('utf-8')
                        )
                        message_id = future.result()
                        logger.info(f"   ✅ Published: {clip_id} (Message ID: {message_id})")
                        
                        time.sleep(1)  # Rate limiting
                        
                    except Exception as e:
                        logger.error(f"   ❌ Failed to publish {clip_id}: {e}")
                
            except Exception as e:
                logger.error(f"   ❌ Error fetching {category_name} clips: {e}")
                continue
    
    except Exception as e:
        logger.error(f"❌ Scraper error: {e}")


def continuous_twitch_monitor(interval=600):
    """
    Continuously monitor Twitch clips at specified interval (default: 10 minutes)
    """
    logger.info(f"🚀 Starting continuous Twitch monitor (interval: {interval}s)")
    
    while True:
        try:
            scrape_twitch_clips()
            logger.info(f"⏳ Next scan in {interval} seconds...")
            time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("🛑 Twitch monitor stopped")
            break
        except Exception as e:
            logger.error(f"Monitor error: {e}")
            time.sleep(10)  # Wait before retrying


if __name__ == "__main__":
    # Check for API credentials
    if not TWITCH_CLIENT_ID or not TWITCH_ACCESS_TOKEN:
        logger.error("❌ Twitch credentials not set in environment variables")
        logger.info("Set them with:")
        logger.info("  export TWITCH_CLIENT_ID=your_client_id")
        logger.info("  export TWITCH_CLIENT_SECRET=your_client_secret")
        logger.info("  export TWITCH_ACCESS_TOKEN=your_access_token")
        exit(1)
    
    # Run scraper
    continuous_twitch_monitor(interval=600)  # Scan every 10 minutes
