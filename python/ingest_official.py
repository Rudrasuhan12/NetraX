from extract_frames import extract_frames
from hash import generate_hash
from store import store_hash
import datetime

def ingest_video(video_path, video_id):
    print(f"🎬 Ingesting OFFICIAL video: {video_id}...")
    frames = extract_frames(video_path)

    for frame_id, frame in frames:
        hash_value = generate_hash(frame)
        timestamp = datetime.datetime.utcnow().isoformat()

        store_hash(video_id, frame_id, hash_value, timestamp)
        print(f"✅ Saved hash for frame {frame_id}")

    print("🎉 Official video fully ingested into database!")

if __name__ == "__main__":
    video_path = input("Enter official video path (e.g., test.mp4): ")
    video_id = input("Enter a name for this video (e.g., official_match_1): ")
    ingest_video(video_path, video_id)