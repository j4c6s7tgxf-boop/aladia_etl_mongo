# and added proper operation type normalization

import argparse
import json
import time
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from kafka_producer import Producer
from bson import json_util

parser = argparse.ArgumentParser()
parser.add_argument("--kafka", default="localhost:9092")
parser.add_argument("--topic", default="cdc_orders")
parser.add_argument("--mongo", default="mongodb://localhost:27017")
args = parser.parse_args()


def get_mongo_client():
    """Attempt to connect to MongoDB with retries."""
    while True:
        try:
            client = MongoClient(args.mongo, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            print("Connected to MongoDB.")
            return client
        except Exception as e:
            print("MongoDB not ready, retrying in 2s:", e)
            time.sleep(2)


client = get_mongo_client()
db = client.demo_db
orders = db.orders

producer = Producer(bootstrap_servers=args.kafka)


def normalize_op_type(op_type):
    """Normalize MongoDB operationType to single character."""
    if op_type == "insert":
        return "I"
    elif op_type == "update":
        return "U"
    elif op_type == "replace":
        return "R"
    elif op_type == "delete":
        return "D"
    else:
        return "U"


def run():
    """Main CDC loop."""
    print("Starting Change Stream listener...")

    try:
        with orders.watch(full_document="updateLookup") as stream:
            for change in stream:
                full_doc = change.get("fullDocument", {})
                
                evt = {
                    "change_id": str(change.get("_id")),
                    "op": normalize_op_type(change.get("operationType")),
                    "order_id": str(change.get("documentKey", {}).get("_id")),
                    "payload": {
                        "order_id": str(full_doc.get("order_id", "")),
                        "user_id": str(full_doc.get("user_id", "")),
                        "status": str(full_doc.get("status", "")),
                        "amount": str(full_doc.get("amount", 0)),
                        "currency": str(full_doc.get("currency", "USD")),
                    },
                    "evt_ts": str(change.get("clusterTime")),
                }

                # Safe JSON encoding of BSON objects
                encoded = json.loads(json_util.dumps(evt))

                producer.send(
                    args.topic,
                    encoded,
                    key=str(evt["order_id"])
                )

                print(f"[CDC] Published {evt['op']} event for order {evt['order_id']}")

    except PyMongoError as e:
        print("MongoDB Change Stream error:", e)
        raise


if __name__ == "__main__":
    while True:
        try:
            run()
        except Exception as e:
            print("CDC crashed, retrying in 2s:", e)
            time.sleep(2)
