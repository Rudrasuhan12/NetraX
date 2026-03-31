import os
import json
import random
from google.cloud import pubsub_v1
from db import db
from firebase_admin import firestore

# ✅ Credentials
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


def calculate_risk_score(confidence, match_type):
    risk = confidence
    if match_type == "pirated":
        risk += 20
    return min(risk, 100)


def get_fake_region():
    return random.choice(["India", "US", "Brazil", "UK", "Indonesia"])


def generate_response(confidence, risk_score, video_id):
    if risk_score > 80:
        return {"level": "HIGH", "action": "🚨 IMMEDIATE TAKEDOWN REQUIRED"}
    elif risk_score > 60:
        return {"level": "MEDIUM", "action": "⚠️ MONITOR AND FLAG"}
    else:
        return {"level": "LOW", "action": "ℹ️ LOG ONLY"}


def fetch_all_hashes():
    official = db.collection("official_hashes").stream()
    pirated = db.collection("pirated_hashes").stream()

    all_hashes = {}

    for doc in official:
        data = doc.to_dict()
        all_hashes[data["hash"]] = {
            "type": "official",
            "video_id": data["video_id"]
        }

    for doc in pirated:
        data = doc.to_dict()
        all_hashes[data["hash"]] = {
            "type": "pirated",
            "doc_id": doc.id,
            "video_id": data.get("video_id", "unknown")
        }

    return all_hashes


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

            # 🔥 Auto response
            response = generate_response(confidence, risk_score, incoming_video_id)

            print("🚨 MATCH DETECTED")
            print(f"   ➤ Confidence: {confidence}%")
            print(f"   ➤ Risk Score: {risk_score}%")
            print(f"   ➤ Region: {region}")
            print(f"📢 {response['action']}")

            # ✅ Store pirated hash
            pirated_doc = db.collection("pirated_hashes").document()
            pirated_doc.set({
                "hash": incoming_hash,
                "video_id": incoming_video_id,
                "timestamp": firestore.SERVER_TIMESTAMP
            })

            # ✅ Propagation tracking
            if match_type == "pirated" and "doc_id" in best_match:
                db.collection("propagation_links").add({
                    "parent_id": best_match["doc_id"],
                    "child_id": pirated_doc.id,
                    "similarity": confidence,
                    "timestamp": firestore.SERVER_TIMESTAMP
                })

            # ✅ Store alert with response + evidence
            db.collection("alerts").add({
                "video_id": incoming_video_id,
                "confidence": confidence,
                "risk_score": risk_score,
                "region": region,
                "status": "MATCH",
                "response": response["action"],   # 🔥 NEW
                "level": response["level"],       # 🔥 NEW
                "evidence": {                     # 🔥 NEW
                    "hash": incoming_hash,
                    "distance": min_distance
                },
                "timestamp": firestore.SERVER_TIMESTAMP,
                "source": "Simulated YouTube Stream"
            })

            print("✅ Alert stored")

        else:
            print("❌ NO MATCH DETECTED")

        message.ack()

    except Exception as e:
        print(f"❌ Error: {e}")
        message.ack()


# ------------------ START ------------------ #

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"📡 Listening on: {subscription_path}...")

with subscriber:
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("🛑 Subscriber stopped")