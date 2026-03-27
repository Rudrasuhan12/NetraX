import os
import json
from google.cloud import pubsub_v1
from db import db

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../serviceAccountKey.json"

project_id = "bwai-solution-challenge"
subscription_id = "video-frames-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def hamming_distance(hash1, hash2):
    return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

def calculate_confidence(distance):
    return max(0, 100 - distance)

def fetch_all_hashes():
    """Fetches both official and pirated hashes for comparison."""
    official_hashes_ref = db.collection("official_hashes").stream()
    pirated_hashes_ref = db.collection("pirated_hashes").stream()

    all_hashes = {}
    for doc in official_hashes_ref:
        data = doc.to_dict()
        all_hashes[data["hash"]] = {"type": "official", "video_id": data["video_id"]}
    
    for doc in pirated_hashes_ref:
        data = doc.to_dict()
        all_hashes[data["hash"]] = {"type": "pirated", "doc_id": doc.id}

    return all_hashes

def callback(message):
    print(f"📩 Message received: {message.message_id}")
    message.ack()

    try:
        data = json.loads(message.data.decode("utf-8"))
        incoming_hash = data["hash"]
        incoming_video_id = data.get("video_id", "simulated_stream_1")

        all_hashes = fetch_all_hashes()

        best_match = None
        min_distance = 11

        for stored_hash, info in all_hashes.items():
            distance = hamming_distance(incoming_hash, stored_hash)
            if distance < min_distance:
                min_distance = distance
                best_match = info
                best_match['hash'] = stored_hash

        if min_distance < 10:
            confidence = calculate_confidence(min_distance)
            print(f"🚨 MATCH DETECTED (Confidence: {confidence}%)")
            timestamp, new_pirated_hash_doc = db.collection("pirated_hashes").add({
                "hash": incoming_hash,
                "video_id": incoming_video_id,
                "timestamp": firestore.SERVER_TIMESTAMP,
            })

            if best_match['type'] == 'pirated':
                db.collection("propagation_links").add({
                    "parent_id": best_match['doc_id'],
                    "child_id": new_pirated_hash_doc.id,
                    "similarity": confidence,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                print(f"🔗 Propagation link created from {best_match['doc_id']} to {new_pirated_hash_doc.id}.")

            db.collection("alerts").add({
                "video_id": incoming_video_id,
                "confidence": confidence,
                "timestamp": firestore.SERVER_TIMESTAMP,
                "source": "Simulated YouTube Stream"
            })

        else:
            print("❌ No significant match found.")

    except Exception as e:
        print(f"An error occurred: {e}")


from firebase_admin import firestore

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"📡 Listening for messages on {subscription_path}...")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()