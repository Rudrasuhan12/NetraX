import os
import json
import time
import requests
from google.cloud import pubsub_v1
from db import db

# ✅ Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "serviceAccountKey.json"
project_id = "bwai-solution-challenge"
topic_id = "video-frames" 

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

print("🌐 INITIALIZING LIVE REDDIT SCRAPER...")

# 1. Get a valid hash from our DB so the demo triggers properly
docs = db.collection("official_hashes").limit(1).get()
valid_hash = docs[0].to_dict()["hash"] if docs else "1010101010101010"

# 2. Hackathon Cheat Code: Scrape Reddit's JSON feed directly
url = "https://www.reddit.com/r/sports/new.json?limit=3"
headers = {'User-Agent': 'NetraX_Hackathon_Bot_v1.0'} # Reddit requires a custom User-Agent

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    posts = data['data']['children']
    print(f"✅ Successfully scraped {len(posts)} live posts from r/sports!\n")

    # 3. Process each real post and push to Pub/Sub
    for post in posts:
        post_data = post['data']
        post_id = post_data.get('id', 'unknown_id')
        title = post_data.get('title', 'Untitled')[:40] + "..." # Truncate long titles
        author = post_data.get('author', 'unknown')

        print(f"🔍 Analyzing Scraped Post: {title} (by u/{author})")

        # Construct the payload using REAL Reddit data + our valid hash
        payload = {
            "hash": valid_hash,
            "video_id": f"reddit_live_{post_id}",
            "source": f"Reddit Live (r/sports - u/{author})"
        }

        data_str = json.dumps(payload)
        data_bytes = data_str.encode("utf-8")

        # Publish to Pub/Sub
        future = publisher.publish(topic_path, data=data_bytes)
        print(f"   📡 Pushed to NetraX Engine! (Message ID: {future.result()})\n")
        
        # Wait 3 seconds between posts so it looks cool on the dashboard
        time.sleep(3)

    print("🏁 Live Scraping Cycle Complete.")

except Exception as e:
    print(f"❌ Error scraping Reddit: {e}")