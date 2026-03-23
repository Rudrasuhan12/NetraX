from db import db

def store_hash(video_id, frame_id, hash_value, timestamp):
    db.collection("official_hashes").add({
        "video_id": video_id,
        "frame_id": frame_id,
        "hash": hash_value,
        "timestamp": timestamp
    })