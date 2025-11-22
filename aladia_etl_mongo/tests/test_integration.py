"""
Integration test for the ETL pipeline.
Run with: python -m pytest tests/test_integration.py -s
"""

import subprocess
import time
import duckdb
import os
from pymongo import MongoClient


def test_full_pipeline():
    """Test the full pipeline from MongoDB to DuckDB."""
    
    # Ensure analytics DB is clean
    if os.path.exists('analytics.duckdb'):
        os.remove('analytics.duckdb')
    
    # Wait for Kafka to be ready
    time.sleep(10)
    
    # Insert test data
    client = MongoClient("mongodb://localhost:27017/?replicaSet=rs0", serverSelectionTimeoutMS=5000)
    db = client["demo_db"]
    collection = db["orders"]
    
    test_order = {
        "order_id": "test-order-123",
        "user_id": "user-456",
        "status": "CREATED",
        "amount": 500.0,
        "currency": "USD"
    }
    
    collection.insert_one(test_order)
    print("[TEST] Inserted test order")
    
    # Wait for CDC to process
    time.sleep(5)
    
    # Check if data reached DuckDB
    con = duckdb.connect('analytics.duckdb')
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM orders_analytics WHERE order_id = 'test-order-123'"
        ).fetchall()
        
        assert result[0][0] > 0, "No data found in analytics warehouse"
        print(f"[TEST] Pipeline Success! Found {result[0][0]} events in warehouse")
        
    except Exception as e:
        print(f"[TEST] Failed: {e}")
        raise


if __name__ == "__main__":
    test_full_pipeline()
