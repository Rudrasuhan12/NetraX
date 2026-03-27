import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceAccountKey.json"
from google.cloud import pubsub_v1
import json

project_id = "bwai-solution-challenge"
topic_id = "video-frames"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def publish_frame(hash_value):
    data = json.dumps({"hash": hash_value}).encode("utf-8")
    
    future = publisher.publish(topic_path, data)
    future.result()   # 🔥 VERY IMPORTANT (wait for send)

    print("📤 Message sent!")