from extract_frames import extract_frames
from hash import generate_hash
from publisher import publish_frame

def process_video(video_path):
    print("📡 Sending frames to Pub/Sub...")

    frames = extract_frames(video_path)

    for _, frame in frames:
        hash_value = generate_hash(frame)
        publish_frame(hash_value)

    print("✅ Done sending frames")

if __name__ == "__main__":
    video_path = input("Enter video name: ")
    process_video(video_path)