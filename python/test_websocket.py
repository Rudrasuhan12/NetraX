from db import db
from firebase_admin import firestore
import time

print("⏱️  WebSocket Speed Test")
print("=" * 50)
print("\n🚨 Adding CRITICAL alert to Firestore...")
print("👀 Watch your dashboard - it should appear INSTANTLY!")
print()

start_time = time.time()

db.collection("alerts").add({
    "video_id": f"websocket_speed_test_{int(start_time)}",
    "source": "WebSocket Real-Time Test",
    "confidence": 95,
    "embedding_score": 92,
    "misuse_category": "Raw Broadcast Piracy",
    "misuse_reasoning": "Testing WebSocket instant delivery (<100ms)",
    "risk_score": 88,
    "region": "Test Region",
    "status": "CLASSIFIED",
    "response": "🚨 AUTO-GENERATE DMCA TAKEDOWN",
    "level": "CRITICAL",
    "timestamp": firestore.SERVER_TIMESTAMP
})

print(f"✅ Alert added at {time.strftime('%H:%M:%S')}")
print()
print("🔍 CHECK YOUR DASHBOARD NOW:")
print("   ✅ INSTANT (<100ms) = WebSocket working")
print("   ❌ DELAYED (2-3s) = Only polling, WebSocket failed")
print()
input("Press Enter to exit...")
