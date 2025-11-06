import random
import time
from datetime import datetime, timezone
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# MongoDB connection
DB_NAME = os.environ.get('DB_NAME')
COLLECTION_NAME = 'videoLogs'
MONGO_URI = F"mongodb+srv://{os.environ.get('DB_USER')}_db_user:{os.environ.get('DB_PASSWORD')}@nestjs-challenge.ora4ujs.mongodb.net/?appName=nestjs-challenge"

print(os.environ.get('DB_USER'))

client = MongoClient(MONGO_URI)
db = client[os.environ.get('DB_NAME')]
collection = db['videoLogs']

# Random data generators
DEVICE_TYPES = ["mobile", "desktop", "tablet", "smart_tv"]
QUALITIES = ["360p", "480p", "720p", "1080p", "4k"]


def generate_document():
    """Generate a random document"""
    video_duration = random.randint(60, 3600)
    return {
        "video_id": f"video_{random.randint(10000, 99999)}",
        "session_id": f"session_{random.randint(100000, 999999)}",
        "watched_seconds": random.randint(0, video_duration),
        "video_duration_seconds": video_duration,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "device_type": random.choice(DEVICE_TYPES),
        "quality": random.choice(QUALITIES)
    }


def insert_operation():
    """Insert a random document"""
    doc = generate_document()
    result = collection.insert_one(doc)
    print(f"INSERT: {result.inserted_id}")


def update_operation():
    """Update a random document"""
    # Find a random document
    docs = list(collection.find().limit(10))
    if not docs:
        print("UPDATE: No documents to update")
        return

    target = random.choice(docs)
    new_watched = random.randint(0, target["video_duration_seconds"])

    result = collection.update_one(
        {"_id": target["_id"]},
        {"$set": {
            "watched_seconds": new_watched,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }}
    )
    print(f"UPDATE: Modified {result.modified_count} document(s)")


def delete_operation():
    """Delete a random document"""
    docs = list(collection.find().limit(10))
    if not docs:
        print("DELETE: No documents to delete")
        return

    target = random.choice(docs)
    result = collection.delete_one({"_id": target["_id"]})
    print(f"DELETE: Deleted {result.deleted_count} document(s)")


def main():
    operations = [insert_operation, update_operation, delete_operation]
    weights = [0.5, 0.3, 0.2]  # 50% insert, 30% update, 20% delete

    print(f"Connected to MongoDB: {DB_NAME}.{COLLECTION_NAME}")
    print("Starting random operations...")

    while True:
        # Choose random operation
        operation = random.choices(operations, weights=weights)[0]
        operation()

        # Wait random time (1-10 seconds)
        sleep_time = random.uniform(0.1, 1)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()