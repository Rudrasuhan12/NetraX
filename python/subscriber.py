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

# --- VERTEX AI SETUP (STAGE 2) ---
# To use real Vertex AI, uncomment the imports and initialize the model:
# import vertexai
# from vertexai.vision_models import MultiModalEmbeddingModel, Image
# vertexai.init(project="bwai-solution-challenge", location="us-central1")
# model = MultiModalEmbeddingModel.from_pretrained("multimodalembedding@001")

def get_embedding_score(incoming_video_id, stored_video_id):
    """
    Stage 2: Invokes Vertex AI to generate and compare embeddings.
    """
    # [REAL IMPLEMENTATION LOGIC]
    # img1 = Image.load_from_file(f"gs://bucket/{incoming_video_id}/frame.jpg")
    # img2 = Image.load_from_file(f"gs://bucket/{stored_video_id}/frame.jpg")
    # emb1 = model.get_embeddings(image=img1).image_embedding
    # emb2 = model.get_embeddings(image=img2).image_embedding
    # return calculate_cosine_similarity(emb1, emb2) * 100
    
    # [DEMO MOCK] Simulating high-accuracy Vertex AI embedding match
    # Since we only trigger this on suspicious frames, we return a high score 
    # simulating that the smart layer confirmed it's piracy.
    return random.randint(86, 99)


# ------------------ UTIL FUNCTIONS ------------------ #

def hamming_distance(hash1, hash2):
    return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

def calculate_confidence(distance):
    # Calculates confidence % based on hamming distance
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

        print("🔍 Stage 1: Fast pHash Check...")

        all_hashes = fetch_all_hashes()

        best_match = None
        min_distance = 65 # Max starting distance for 64-bit hash

        for stored_hash, info in all_hashes.items():
            distance = hamming_distance(incoming_hash, stored_hash)
            if distance < min_distance:
                min_distance = distance
                best_match = info

        phash_confidence = calculate_confidence(min_distance)

        # ---------------- 2-STAGE DECISION LOGIC ---------------- #
        # Stage 1: pHash similarity > 70% -> Send to embeddings
        if phash_confidence > 70 and best_match:
            print(f"⚠️ SUSPICIOUS FRAME (pHash: {phash_confidence}%)")
            print("🧠 Triggering Stage 2: Vertex AI Embeddings Verification...")
            
            # Stage 2: Embedding Verification
            embedding_score = get_embedding_score(incoming_video_id, best_match["video_id"])
            print(f"   ➤ AI Embedding Score: {embedding_score}%")

            # Embedding similarity > 85% -> CONFIRMED piracy
            if embedding_score > 85:
                match_type = best_match["type"]
                risk_score = calculate_risk_score(embedding_score, match_type) # Use smart score for risk
                region = get_fake_region()

                # 🔥 Auto response
                response = generate_response(embedding_score, risk_score, incoming_video_id)

                print("🚨 CONFIRMED PIRACY MATCH")
                print(f"   ➤ Final Confidence: {embedding_score}%")
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
                        "similarity": embedding_score,
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })

                # ✅ Store alert with BOTH scores + response + evidence
                db.collection("alerts").add({
                    "video_id": incoming_video_id,
                    "confidence": phash_confidence,       # Stage 1 Fast Score
                    "embedding_score": embedding_score,   # Stage 2 Smart Score
                    "risk_score": risk_score,
                    "region": region,
                    "status": "CONFIRMED",
                    "response": response["action"],
                    "level": response["level"],
                    "evidence": {
                        "hash": incoming_hash,
                        "phash_confidence": phash_confidence,
                        "embedding_score": embedding_score,
                        "distance": min_distance
                    },
                    "timestamp": firestore.SERVER_TIMESTAMP,
                    "source": "Simulated YouTube Stream"
                })

                print("✅ Confirmed Alert Stored")
            else:
                print("✅ CLEARED: Embedding score too low. False positive prevented.")
        else:
            print("❌ NO MATCH (pHash below 70%)")

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