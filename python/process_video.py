from extract_frames import extract_frames
from hash import generate_hash
from store import store_hash

def process_video(video_path, video_id):
    frames = extract_frames(video_path)

    for frame_id, frame in frames:
        hash_value = generate_hash(frame)
        timestamp = frame_id * 2  # since interval = 2 sec

        store_hash(video_id, frame_id, hash_value, timestamp)

    print("✅ Video processed and hashes stored!")

# Example usage
process_video("test.mp4", "match1")