import uuid
import time
import os
import subprocess
from datetime import datetime
from pymongo import MongoClient, errors

# ----------------------------
# Configuration
# ----------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_PATH = "/tmp/mongo-data"
REPLICA_SET = "rs0"

# ----------------------------
# Helper Functions
# ----------------------------
def start_mongo():
    """Start MongoDB as a subprocess if not already running."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1000)
        client.admin.command("ping")
        print("MongoDB already running. ✅")
    except errors.ServerSelectionTimeoutError:
        print("🚀 Starting MongoDB...")
        os.makedirs(DB_PATH, exist_ok=True)
        subprocess.Popen([
            "mongod",
            "--dbpath", DB_PATH,
            "--replSet", REPLICA_SET,
            "--bind_ip", "localhost",
            "--quiet"
        ])
        print("Waiting a few seconds for MongoDB to start...")
        time.sleep(5)

def wait_for_primary(client, timeout=30):
    """Wait until MongoDB becomes PRIMARY."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            status = client.admin.command("replSetGetStatus")
            if status["myState"] == 1:
                print("✅ MongoDB PRIMARY is ready.")
                return True
        except Exception:
            pass
        print("⏳ Waiting for PRIMARY...")
        time.sleep(1)
    print("❌ Timed out waiting for PRIMARY.")
    return False

def init_replica_set():
    """Initialize the replica set if it is not already initialized."""
    client = MongoClient(MONGO_URI)
    try:
        client.admin.command("replSetGetStatus")
        print("Replica set already initialized. ✅")
        return
    except errors.OperationFailure:
        print("⚙️ Initializing replica set...")

    config = {
        "_id": REPLICA_SET,
        "members": [{"_id": 0, "host": "localhost:27017"}]
    }
    client.admin.command("replSetInitiate", config)
    print("Replica set initiation sent.")
    wait_for_primary(client)

def seed_orders():
    """Insert initial orders and start background updates."""
    # Use replicaSet parameter for change stream support
    client = MongoClient(f"{MONGO_URI}/?replicaSet={REPLICA_SET}", serverSelectionTimeoutMS=5000)
    db = client["demo_db"]
    collection = db["orders"]

    # Drop existing collection to start fresh
    collection.drop()

    print("Inserting sample orders...")
    for i in range(5):
        order = {
            "order_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "status": "CREATED",
            "amount": 10.0 + i * 5,
            "currency": "USD",
            "last_updated": datetime.utcnow()
        }
        collection.insert_one(order)
        print(f"  → Inserted order {order['order_id']}")

    print("Starting status flipper loop...")
    while True:
        time.sleep(10)
        order = collection.aggregate([{"$sample": {"size": 1}}]).next()
        new_status = "COMPLETED" if order["status"] == "CREATED" else "CREATED"
        collection.update_one(
            {"_id": order["_id"]},
            {"$set": {"status": new_status, "last_updated": datetime.utcnow()}}
        )
        print(f"  → Updated order {order['order_id']} to {new_status}")

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    start_mongo()
    init_replica_set()
    seed_orders()
