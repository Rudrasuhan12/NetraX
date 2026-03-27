import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceAccountKey.json"
from google.cloud import pubsub_v1
import json
from fetch_hashes import fetch_hashes
from compare import hamming_distance
from alert_store import store_alert  

project_id = "bwai-solution-challenge"
subscription_id = "video-frames-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

stored_hashes = fetch_hashes()

def calculate_confidence(distance):
    return max(0, 100 - distance)


def callback(message):
    print("📩 Message received!")

    data = json.loads(message.data.decode("utf-8"))
    incoming_hash = data["hash"]

    print("🔍 Processing hash...")

    match_found = False

    for stored_hash in stored_hashes:
        distance = hamming_distance(incoming_hash, stored_hash)

        if distance < 10:
            confidence = calculate_confidence(distance)

            print(f"🚨 MATCH DETECTED (Confidence: {confidence}%)")

            store_alert("match1", confidence)

            match_found = True
            break

    if not match_found:
        print("❌ NOT DETECTED")

    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print("📡 Listening for messages...")

with subscriber:
    try:
        streaming_pull_future.result(timeout=None)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()