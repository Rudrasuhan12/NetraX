"""
Unified Video Upload Processor
Automatically detects piracy by comparing against:
1. Official content in Firestore (local database)
2. YouTube external sources
3. Reddit external sources
"""
import os
import sys
import io as io_module

# Load environment variables from .env.local
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), '../.env.local')
    print(f"Looking for .env.local at: {env_path}")
    load_dotenv(env_path, override=True)  # override=True ensures new values are used
    print("env.local loaded in upload_processor")
except ImportError:
    # If dotenv not installed, continue with system env vars
    print("dotenv not installed")
    pass

# Fix Windows Unicode encoding issues
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

from extract_frames import extract_frames
from hash import generate_hash
from external_source_checker import check_all_external_sources
import datetime
from datetime import timezone
from google.cloud import firestore
import imagehash
import base64
import json
from PIL import Image
import hashlib

# Initialize Firestore
db = firestore.Client()
LOCAL_MATCH_DISTANCE_THRESHOLD = int(os.getenv("LOCAL_MATCH_DISTANCE_THRESHOLD", "18"))
LOCAL_MATCH_SAMPLE_SIZE = int(os.getenv("LOCAL_MATCH_SAMPLE_SIZE", "2000"))
LOCAL_MATCH_FRAME_LIMIT = int(os.getenv("LOCAL_MATCH_FRAME_LIMIT", "10"))


def hash_distance(hash1: str, hash2: str) -> int:
    """Bit-level hamming distance for pHash values."""
    if not hash1 or not hash2:
        return 64
    try:
        return imagehash.hex_to_hash(hash1) - imagehash.hex_to_hash(hash2)
    except Exception:
        return 64


def build_asset_id(source_platform: str, source_url: str, video_id: str) -> str:
    seed = f"{source_platform}|{source_url}|{video_id}"
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
    return f"asset_{source_platform}_{digest}"


def graph_fields(video_id: str, source_platform: str, source_url: str, parent_asset_id: str = "") -> dict:
    return {
        "asset_id": build_asset_id(source_platform, source_url or video_id, video_id),
        "source_platform": source_platform,
        "source_url": source_url,
        "parent_asset_id": parent_asset_id or None,
    }

def process_uploaded_video(video_path):
    """
    Process uploaded video and automatically detect piracy
    Compares against ALL official content in database
    """
    print(f"\n{'='*60}")
    print(f"🎬 NetraX Video Analysis - Processing Upload")
    print(f"{'='*60}")
    print(f"📁 File: {os.path.basename(video_path)}")
    
    if not os.path.exists(video_path):
        print(f"❌ Error: Video file not found at {video_path}")
        return False
    
    try:
        # Step 1: Extract frames
        print(f"\n[1/5] 🎞️  Extracting video frames...")
        frames = extract_frames(video_path)
        total_frames = len(frames)
        print(f"      ✅ Extracted {total_frames} frames")
        
        # Step 1.5: Check External Sources (YouTube/Reddit/X/Instagram/Facebook/TikTok)
        print(f"\n[2/5] 🌐 Checking External Sources (YouTube/Reddit/X/Instagram/Facebook/TikTok)...")
        upload_id = f"upload_{int(datetime.datetime.now().timestamp())}"
        upload_context = {
            "upload_title": os.path.basename(video_path),
            "uploaded_at": datetime.datetime.now(timezone.utc)
        }
        external_results = check_all_external_sources(frames, upload_id, upload_context)
        youtube_matches = external_results.get("youtube_matches", [])
        reddit_matches = external_results.get("reddit_matches", [])
        x_matches = external_results.get("x_matches", [])
        instagram_matches = external_results.get("instagram_matches", [])
        facebook_matches = external_results.get("facebook_matches", [])
        tiktok_matches = external_results.get("tiktok_matches", [])
        all_external_matches = youtube_matches + reddit_matches + x_matches + instagram_matches + facebook_matches + tiktok_matches
        
        # Step 2: Check database for official content
        print(f"\n[3/5] 🔍 Checking local Firestore database...")
        official_count = db.collection("official_hashes").limit(1).stream()
        has_official_content = len(list(official_count)) > 0
        
        if not has_official_content:
            print(f"      ⚠️  No official content found in database")
            print(f"      💡 Add official sports videos first using ingest_official.py")
            print(f"      ℹ️  For demo: Re-upload this video to create reference")
            
            # Store as official for future comparisons
            print(f"\n[4/5] 💾 Storing as official reference content...")
            for frame_id, frame in frames[:10]:  # Store first 10 frames
                hash_value = generate_hash(frame)
                db.collection("official_hashes").add({
                    "video_id": f"uploaded_{int(datetime.datetime.now().timestamp())}",
                    "frame_id": frame_id,
                    "hash": hash_value,
                    "timestamp": datetime.datetime.now(timezone.utc).isoformat(),
                    "source": "judge_upload"
                })
            print(f"      ✅ Stored {min(10, total_frames)} reference hashes")
            
            # Still check external sources even if no local database
            if external_results.get("external_piracy_detected"):
                print(f"\n[4/5] 🌐 External Piracy Detected!")
                print(f"      📺 YouTube matches: {len(youtube_matches)}")
                print(f"      🤖 Reddit matches: {len(reddit_matches)}")
                print(f"      🐦 X matches: {len(x_matches)}")
                print(f"      📸 Instagram matches: {len(instagram_matches)}")
                print(f"      📘 Facebook matches: {len(facebook_matches)}")
                print(f"      🎵 TikTok matches: {len(tiktok_matches)}")
                
                # Create external match alerts
                for ext_match in all_external_matches[:8]:
                    create_external_source_alert(ext_match, upload_id)
                
                print(f"\n[5/5] 📢 Status: EXTERNAL PIRACY DETECTED")
            else:
                print(f"\n[4/5] 🌐 No external piracy detected")
                print(f"[5/5] 📢 Status: No content found on YouTube/Reddit")
            
            return True
        
        print(f"      ✅ Official content database available")
        
        # Step 3: Detect matches in local database
        print(f"\n[4/5] 🔬 Analyzing for local database matches...")
        matches_found = []
        frames_analyzed = 0
        
        # Analyze more frames for local matching reliability
        frame_limit = min(max(LOCAL_MATCH_FRAME_LIMIT, 1), len(frames))
        sampled_official_hashes = []
        for official_doc in db.collection("official_hashes").limit(max(LOCAL_MATCH_SAMPLE_SIZE, 1)).stream():
            official_data = official_doc.to_dict()
            official_hash = official_data.get("hash")
            if official_hash:
                sampled_official_hashes.append(official_data)

        print(f"      ℹ️  Loaded {len(sampled_official_hashes)} official hashes for approximate matching")
        print(f"      ℹ️  Local frame limit: {frame_limit}, distance threshold: {LOCAL_MATCH_DISTANCE_THRESHOLD}")

        for frame_id, frame in frames[:frame_limit]:
            frames_analyzed += 1
            hash_value = generate_hash(frame)
            
            # First attempt exact hash lookup
            matching_docs = db.collection("official_hashes").where(filter=firestore.FieldFilter("hash", "==", hash_value)).limit(1).stream()
            exact_match = next(matching_docs, None)

            if exact_match:
                match_data = exact_match.to_dict()
                matches_found.append({
                    "frame_id": frame_id,
                    "hash": hash_value,
                    "official_video_id": match_data.get("video_id"),
                    "distance": 0,
                    "frame": frame
                })
                print(f"      🚨 LOCAL MATCH FOUND on frame {frame_id}")
                print(f"         Official Video: {match_data.get('video_id')}")
                continue

            # Fallback: approximate pHash match against sampled official hashes
            best_match = None
            best_distance = 65
            for official in sampled_official_hashes:
                dist = hash_distance(hash_value, official.get("hash"))
                if dist < best_distance:
                    best_distance = dist
                    best_match = official

            if best_match and best_distance <= LOCAL_MATCH_DISTANCE_THRESHOLD:
                matches_found.append({
                    "frame_id": frame_id,
                    "hash": hash_value,
                    "official_video_id": best_match.get("video_id"),
                    "distance": best_distance,
                    "frame": frame
                })
                print(f"      🚨 LOCAL APPROX MATCH FOUND on frame {frame_id}")
                print(f"         Official Video: {best_match.get('video_id')} (distance={best_distance})")
        
        # Check if external matches were found
        if external_results.get("external_piracy_detected"):
            print(f"\n      🌐 EXTERNAL SOURCES MATCHED!")
            print(f"      📺 YouTube: {len(youtube_matches)} matches")
            print(f"      🤖 Reddit: {len(reddit_matches)} matches")
            print(f"      🐦 X: {len(x_matches)} matches")
            print(f"      📸 Instagram: {len(instagram_matches)} matches")
            print(f"      📘 Facebook: {len(facebook_matches)} matches")
            print(f"      🎵 TikTok: {len(tiktok_matches)} matches")
            
            # Create external alerts
            for ext_match in all_external_matches[:8]:
                create_external_source_alert(ext_match, upload_id)
        
        if not matches_found and not external_results.get("external_piracy_detected"):
            print(f"      ✅ No matches detected (local or external)")
            print(f"      ℹ️  Analyzed {frames_analyzed} frames")
            print(f"\n[5/5] 📢 Status: NO PIRACY DETECTED")
            
            # CREATE ALERT FOR CLEAN UPLOAD (so it appears on dashboard)
            clean_alert = {
                "video_id": upload_id,
                "filename": os.path.basename(video_path),
                "source": "Upload",
                "url": os.path.basename(video_path),
                "timestamp": datetime.datetime.now(timezone.utc).isoformat(),
                "confidence": 100,
                "risk_score": 0,
                "misuse_category": "Clean Content",
                "misuse_reasoning": "No piracy detected - verified content",
                "alert_type": "CLEAN",
                "status": "verified_clean",
                **graph_fields(upload_id, "upload", os.path.basename(video_path))
            }
            db.collection("piracy_alerts").add(clean_alert)
            print(f"      📊 Clean upload alert created")
            return True
        
        # If only external matches, create alerts and return
        if not matches_found and external_results.get("external_piracy_detected"):
            print(f"\n[5/5] 📢 Status: EXTERNAL PIRACY DETECTED (No local matches)")
            return True
        
        # Step 4: Create detailed alerts with AI analysis (for local matches)
        print(f"\n[4/4] 🤖 Running AI analysis on {len(matches_found)} matches...")
        
        for match in matches_found:
            try:
                # Convert frame to base64
                pil_img = Image.fromarray(match["frame"])
                buffer = io_module.BytesIO()
                pil_img.save(buffer, format='JPEG')
                frame_base64 = base64.b64encode(buffer.getvalue()).decode()
                
                # Import AI modules
                from gemini_vision import analyze_frame_with_gemini_vision
                
                # Create payload
                video_id = upload_id
                payload = {
                    "video_id": video_id,
                    "frame_id": match["frame_id"],
                    "frame_data": frame_base64,
                    "original_hash": match["hash"],
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                    "source": "Judge Upload Detection",
                    "url": os.path.basename(video_path),
                    "matched_official_id": match["official_video_id"]
                }
                
                print(f"      🧠 Analyzing frame {match['frame_id']} with Gemini Vision...")
                
                # Run Gemini Vision
                gemini_result = analyze_frame_with_gemini_vision(frame_base64, payload)
                
                if gemini_result:
                    # Calculate risk score
                    base_score = gemini_result.get('embedding_score', 85)
                    misuse_category = gemini_result.get('misuse_category', 'Unknown')
                    
                    # Apply multipliers based on classification
                    multiplier = 1.0
                    if gemini_result.get('is_deepfake'):
                        multiplier = 1.3
                    elif misuse_category == "Raw Piracy":
                        multiplier = 1.15
                    elif misuse_category == "Meme":
                        multiplier = 0.8
                    elif misuse_category == "Fair Use":
                        multiplier = 0.5
                    
                    risk_score = min(base_score * multiplier, 100)
                    
                    # Determine alert level
                    if risk_score > 85:
                        alert_type = "HIGH_RISK"
                        alert_emoji = "🔴"
                    elif risk_score > 50:
                        alert_type = "MEDIUM_RISK"
                        alert_emoji = "🟡"
                    else:
                        alert_type = "LOW_RISK"
                        alert_emoji = "🟢"
                    
                    # Store alert to Firestore
                    alert_data = {
                        "video_id": video_id,
                        "source": "Judge Upload",
                        "url": os.path.basename(video_path),
                        "timestamp": firestore.SERVER_TIMESTAMP,
                        "confidence": base_score,
                        "risk_score": risk_score,
                        "misuse_category": misuse_category,
                        "misuse_reasoning": gemini_result.get('misuse_reasoning', 'Detected from uploaded video'),
                        "matched_official_id": match["official_video_id"],
                        "alert_type": alert_type,
                        "is_deepfake": gemini_result.get('is_deepfake', False),
                        **graph_fields(video_id, "upload", os.path.basename(video_path), f"asset_official_{match['official_video_id']}")
                    }
                    
                    alert_ref = db.collection("piracy_alerts").add(alert_data)
                    
                    print(f"      ✅ Alert created: {alert_ref[1].id}")
                    print(f"         {alert_emoji} Risk Score: {risk_score:.1f}%")
                    print(f"         🏷️  Category: {misuse_category}")
                    print(f"         📝 Reasoning: {gemini_result.get('misuse_reasoning', 'N/A')[:60]}...")
                    
                else:
                    print(f"      ⚠️  AI analysis unavailable, using basic detection")
                    # Store basic alert without AI analysis
                    alert_data = {
                        "video_id": video_id,
                        "source": "Judge Upload",
                        "url": os.path.basename(video_path),
                        "timestamp": firestore.SERVER_TIMESTAMP,
                        "confidence": 100,
                        "risk_score": 90,
                        "misuse_category": "Suspected Piracy",
                        "misuse_reasoning": "Hash match detected",
                        "matched_official_id": match["official_video_id"],
                        "alert_type": "HIGH_RISK",
                        **graph_fields(video_id, "upload", os.path.basename(video_path), f"asset_official_{match['official_video_id']}")
                    }
                    db.collection("piracy_alerts").add(alert_data)
                    
            except Exception as e:
                print(f"      ⚠️  Error during AI analysis: {e}")
                # Continue with next match
        
        print(f"\n{'='*60}")
        print(f"🎉 Analysis Complete!")
        print(f"📊 Summary:")
        print(f"   • Frames analyzed: {frames_analyzed}")
        print(f"   • Local matches found: {len(matches_found)}")
        print(f"   • YouTube matches found: {len(youtube_matches)}")
        print(f"   • Reddit matches found: {len(reddit_matches)}")
        print(f"   • X/Instagram/Facebook/TikTok matches found: {len(x_matches) + len(instagram_matches) + len(facebook_matches) + len(tiktok_matches)}")
        print(f"   • Total external sources checked: YouTube + Reddit + X + Instagram + Facebook + TikTok")
        print(f"   • Alerts created: {len(matches_found) + len(all_external_matches)}")
        print(f"   • Status: {'PIRACY DETECTED ⚠️' if (matches_found or external_results.get('external_piracy_detected')) else 'No issues found ✅'}")
        print(f"{'='*60}")
        print(f"📱 Check your NetraX dashboard for real-time alerts!")
        print(f"{'='*60}\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Processing Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def create_external_source_alert(match_data: dict, video_id: str):
    """
    Create an alert for external source matches (YouTube/Reddit/X/Instagram/Facebook/TikTok)
    
    Args:
        match_data: Match result from external_source_checker
        video_id: ID of uploaded video
    """
    try:
        platform = match_data.get("platform", "Unknown")
        raw_similarity = match_data.get("multi_signal_score", match_data.get("similarity", 0))
        try:
            similarity = float(raw_similarity or 0)
        except (TypeError, ValueError):
            similarity = 0.0
        # Normalize scores that are in 0-1 range to 0-100.
        if 0 < similarity <= 1:
            similarity *= 100.0
        
        # Determine alert level based on similarity
        if similarity > 85:
            alert_type = "HIGH_RISK"
        elif similarity > 70:
            alert_type = "MEDIUM_RISK"
        else:
            alert_type = "LOW_RISK"
        # Dynamic risk score to avoid a fixed floor for all low-risk matches.
        risk_score = max(30, min(100, round(similarity)))
        embedding_score = match_data.get("embedding_similarity")
        if embedding_score is None:
            embedding_score = match_data.get("vector_similarity")
        if embedding_score is None:
            embedding_score = similarity
        
        # Create alert
        alert_data = {
            "video_id": video_id,
            "source": f"External ({platform})",
            "platform": platform,
            "url": match_data.get("url", ""),
            "timestamp": firestore.SERVER_TIMESTAMP,
            "confidence": similarity,
            "similarity_score": similarity,
            "risk_score": risk_score,
            "misuse_category": "Unauthorized Reupload",
            "misuse_reasoning": f"{platform} content reused - {similarity:.1f}% similarity match detected",
            "alert_type": alert_type,
            "lineage_score": match_data.get("multi_signal_score", similarity),
            "hash_similarity": match_data.get("hash_similarity"),
            "title_similarity": match_data.get("title_similarity"),
            "temporal_similarity": match_data.get("temporal_similarity"),
            "embedding_similarity": match_data.get("embedding_similarity"),
            "embedding_score": embedding_score,
            "external_metadata": {
                "title": match_data.get("title", ""),
                "author": match_data.get("author") or match_data.get("channel", ""),
                "location": match_data.get("subreddit") or match_data.get("channel", "Unknown"),
                "thumbnail_url": match_data.get("thumbnail_url") or match_data.get("media_url", ""),
                "detected_at": match_data.get("detected_at", "")
            },
            **graph_fields(
                video_id,
                str(match_data.get("source_platform", platform)).lower(),
                match_data.get("source_url") or match_data.get("url", ""),
                match_data.get("parent_asset_id", "")
            )
        }
        
        db.collection("piracy_alerts").add(alert_data)
        
        emoji = "🔴" if risk_score > 85 else "🟡" if risk_score > 50 else "🟢"
        print(f"      ✅ External Alert Created ({platform})")
        print(f"         {emoji} Risk Score: {risk_score}%")
        print(f"         📍 Location: {alert_data['external_metadata']['location']}")
        print(f"         🎬 Title: {match_data.get('title', '')[:50]}")
        print(f"         🔗 URL: {match_data.get('url', '')}")
        
    except Exception as e:
        print(f"      ⚠️  Error creating external alert: {e}")


if __name__ == "__main__":
    # Get video path from environment or command line
    video_path = os.environ.get("VIDEO_PATH")
    
    if not video_path and len(sys.argv) > 1:
        video_path = sys.argv[1]
    
    if not video_path:
        print("❌ Error: No video path provided")
        print("Usage:")
        print("  - Set VIDEO_PATH environment variable")
        print("  - Or: python upload_processor.py <video_path>")
        sys.exit(1)
    
    success = process_uploaded_video(video_path)
    sys.exit(0 if success else 1)
