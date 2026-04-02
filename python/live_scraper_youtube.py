"""
YouTube Data API - Real-Time YouTube Shorts Scraper
Monitors sports channels for new Shorts and publishes to Pub/Sub
"""

import os
import json
import time
import base64
import requests
import logging
from datetime import datetime
import cv2
import numpy as np
from PIL import Image
from io import BytesIO

from google.cloud import pubsub_v1
from publisher import publisher, topic_path
from hash import generate_hash
from db import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# YouTube API credentials
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # Get from environment
CHANNEL_IDS = {
    "espn": "UCsrZ3LvIq3JHEqEGJBuE6OQ",  # Official ESPN
    "sports_center": "UCEgdi0XIXXZ-qJOFPf4JSKw",  # SportsCenter
    "nba": "UCJB8UH7cM3r5YKHgXOZBwpA",  # NBA Official
    "nfl": "UCDVYQ4Zhbm3S7__I47EB23A",  # NFL Official
    "mlb": "UCSqv-ssCOvDvVP1dJHe4d_g"  # MLB Official
}


def get_youtube_service():
    """Initialize YouTube API client"""
    try:
        from googleapiclient.discovery import build
        service = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        logger.info("✅ YouTube API service initialized")
        return service
    except Exception as e:
        logger.error(f"❌ Failed to initialize YouTube API: {e}")
        return None


def download_and_hash_thumbnail(thumbnail_url):
    """
    Download video thumbnail and generate perceptual hash
    """
    try:
        response = requests.get(thumbnail_url, timeout=5)
        if response.status_code != 200:
            logger.warning(f"Failed to download thumbnail: {thumbnail_url}")
            return None
        
        # Convert image bytes to array
        img = Image.open(BytesIO(response.content))
        img_array = np.array(img)
        
        # Generate hash
        hash_value = generate_hash(img_array)
        
        # Also encode as base64 for Gemini Vision
        img_bytes = BytesIO()
        img.save(img_bytes, format='JPEG')
        img_base64 = base64.b64encode(img_bytes.getvalue()).decode('utf-8')
        
        return {
            "hash": hash_value,
            "image_base64": img_base64,
            "url": thumbnail_url
        }
        
    except Exception as e:
        logger.warning(f"Error processing thumbnail: {e}")
        return None


def scrape_youtube_shorts():
    """
    Fetch and monitor YouTube Shorts from sports channels
    """
    youtube = get_youtube_service()
    if not youtube:
        logger.error("YouTube service not available")
        return
    
    logger.info("🎬 Starting YouTube Shorts Scraper...")
    logger.info(f"Monitoring {len(CHANNEL_IDS)} sports channels")
    
    try:
        for channel_name, channel_id in CHANNEL_IDS.items():
            logger.info(f"\n📺 Fetching from {channel_name} (ID: {channel_id})...")
            
            try:
                # Search for recent Shorts
                request = youtube.search().list(
                    part='snippet',
                    channelId=channel_id,
                    maxResults=5,
                    order='date',
                    type='video',
                    videoDuration='short',  # Filter for Shorts (< 60 seconds)
                    publishedAfter=datetime.now().isoformat() + "Z"  # Last upload
                )
                
                response = request.execute()
                
                if 'items' not in response or len(response['items']) == 0:
                    logger.info(f"   No new Shorts found")
                    continue
                
                for item in response['items']:
                    video_id = item['id']['videoId']
                    title = item['snippet']['title']
                    channel_title = item['snippet']['channelTitle']
                    thumbnail_url = item['snippet']['thumbnails']['high']['url']
                    published_at = item['snippet']['publishedAt']
                    
                    logger.info(f"   🎥 Found Short: {title[:40]}...")
                    
                    # Download and hash thumbnail
                    hash_data = download_and_hash_thumbnail(thumbnail_url)
                    
                    if not hash_data:
                        logger.warning(f"   Skipping {video_id} - hash generation failed")
                        continue
                    
                    # Prepare Pub/Sub message
                    payload = {
                        "hash": hash_data["hash"],
                        "video_id": f"youtube_shorts_{video_id}",
                        "source": f"YouTube Shorts ({channel_title})",
                        "platform": "youtube",
                        "channel": channel_name,
                        "title": title,
                        "published_at": published_at,
                        "thumbnail_url": thumbnail_url,
                        "frame_base64": hash_data["image_base64"],
                        "youtube_url": f"https://youtube.com/shorts/{video_id}",
                        "metadata": {
                            "channel_id": channel_id,
                            "video_id": video_id,
                            "channel_title": channel_title
                        }
                    }
                    
                    # Publish to Pub/Sub
                    try:
                        future = publisher.publish(
                            topic_path,
                            data=json.dumps(payload).encode('utf-8')
                        )
                        message_id = future.result()
                        logger.info(f"   ✅ Published: {video_id} (Message ID: {message_id})")
                        
                        time.sleep(1)  # Rate limiting
                        
                    except Exception as e:
                        logger.error(f"   ❌ Failed to publish {video_id}: {e}")
                        
            except Exception as e:
                logger.error(f"   ❌ Error fetching from {channel_name}: {e}")
                continue
    
    except Exception as e:
        logger.error(f"❌ Scraper error: {e}")


def continuous_youtube_monitor(interval=300):
    """
    Continuously monitor YouTube Shorts at specified interval (default: 5 minutes)
    """
    logger.info(f"🚀 Starting continuous YouTube monitor (interval: {interval}s)")
    
    while True:
        try:
            scrape_youtube_shorts()
            logger.info(f"⏳ Next scan in {interval} seconds...")
            time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("🛑 YouTube monitor stopped")
            break
        except Exception as e:
            logger.error(f"Monitor error: {e}")
            time.sleep(10)  # Wait before retrying


if __name__ == "__main__":
    # Check for API key
    if not YOUTUBE_API_KEY:
        logger.error("❌ YOUTUBE_API_KEY not set in environment variables")
        logger.info("Set it with: export YOUTUBE_API_KEY=your_api_key")
        exit(1)
    
    # Run scraper
    continuous_youtube_monitor(interval=300)  # Scan every 5 minutes
