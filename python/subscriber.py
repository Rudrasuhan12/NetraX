import os
import json
import random
from google.cloud import pubsub_v1
from db import db
from firebase_admin import firestore

# ✅ Correct credentials path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "serviceAccountKey.json"

project_id = "bwai-solution-challenge"
subscription_id = "video-frames-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


# ------------------ UTIL FUNCTIONS ------------------ #

def hamming_distance(hash1, hash2):
    return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))


def calculate_confidence(distance):
    return max(0, 100 - distance)


def fetch_all_hashes():
    official_hashes_ref = db.collection("official_hashes").stream()
    pirated_hashes_ref = db.collection("pirated_hashes").stream()

    all_hashes = {}

    for doc in official_hashes_ref:
        data = doc.to_dict()
        all_hashes[data["hash"]] = {
            "type": "official",
            "video_id": data["video_id"]
        }

    for doc in pirated_hashes_ref:
        data = doc.to_dict()
        all_hashes[data["hash"]] = {
            "type": "pirated",
            "doc_id": doc.id,
            "video_id": data.get("video_id", "unknown")
        }

    return all_hashes


def calculate_risk_score(confidence, match_type):
    risk = confidence
    if match_type == "pirated":
        risk += 20
    return min(risk, 100)


def get_fake_region():
    regions = ["India", "US", "Brazil", "UK", "Indonesia"]
    return random.choice(regions)


# ------------------ CALLBACK ------------------ #

def callback(message):
    print(f"\n📩 Message received: {message.message_id}")

    try:
        data = json.loads(message.data.decode("utf-8"))

        incoming_hash = data["hash"]
        incoming_video_id = data.get("video_id", "simulated_stream")

        print("🔍 Processing hash...")

        all_hashes = fetch_all_hashes()

        best_match = None
        min_distance = 11

        # 🔍 Find best match
        for stored_hash, info in all_hashes.items():
            distance = hamming_distance(incoming_hash, stored_hash)

            if distance < min_distance:
                min_distance = distance
                best_match = info

        # ---------------- MATCH FOUND ---------------- #
        if min_distance < 10 and best_match:
            confidence = calculate_confidence(min_distance)
            match_type = best_match["type"]

            risk_score = calculate_risk_score(confidence, match_type)
            region = get_fake_region()

            print("🚨 MATCH DETECTED")
            print(f"   ➤ Confidence: {confidence}%")
            print(f"   ➤ Risk Score: {risk_score}%")
            print(f"   ➤ Region: {region}")

            # ✅ FIXED: Use document() instead of add()
            pirated_doc_ref = db.collection("pirated_hashes").document()
            pirated_doc_ref.set({
                "hash": incoming_hash,
                "video_id": incoming_video_id,
                "timestamp": firestore.SERVER_TIMESTAMP
            })

            # ✅ Propagation tracking (safe)
            if match_type == "pirated" and "doc_id" in best_match:
                db.collection("propagation_links").add({
                    "parent_id": best_match["doc_id"],
                    "child_id": pirated_doc_ref.id,
                    "similarity": confidence,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })
                print("🔗 Propagation link created")

            # ✅ Store alert (MAIN FIX)
            print("📦 Storing alert in Firestore...")

            db.collection("alerts").add({
                "video_id": incoming_video_id,
                "confidence": confidence,
                "risk_score": risk_score,
                "region": region,
                "status": "MATCH",
                "timestamp": firestore.SERVER_TIMESTAMP,
                "source": "Simulated YouTube Stream"
            })

            print("✅ Alert stored successfully")

        else:
            print("❌ NO MATCH DETECTED")

        # ✅ ACK message
        message.ack()

    except Exception as e:
        print(f"❌ Error: {e}")
        message.ack()   # prevent infinite retry


# ------------------ START SUBSCRIBER ------------------ #

streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback
)

print(f"📡 Listening on: {subscription_path}...")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("🛑 Subscriber stopped")