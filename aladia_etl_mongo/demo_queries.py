"""
Demo script to run queries on the analytics warehouse (DuckDB).
Shows how to query the processed CDC data in a real-world scenario.
"""

import duckdb
import sys

def run_demo_queries():
    """Run sample analytics queries on the warehouse."""
    
    con = duckdb.connect('analytics.duckdb')
    
    print("\n=== ETL Pipeline Analytics Demo ===\n")
    
    # Query 1: Count of events by operation type
    print("1. Event counts by operation type (I=Insert, U=Update, D=Delete, R=Replace):")
    result = con.execute("""
        SELECT op, COUNT(*) as count
        FROM orders_analytics
        GROUP BY op
        ORDER BY op
    """).fetchall()
    for op, count in result:
        print(f"   {op}: {count}")
    
    # Query 2: High-value orders
    print("\n2. High-value orders (amount >= 1000):")
    result = con.execute("""
        SELECT order_id, amount, op, evt_ts
        FROM orders_analytics
        WHERE is_high_value = true
        ORDER BY evt_ts DESC
    """).fetchall()
    if result:
        for order_id, amount, op, evt_ts in result:
            print(f"   Order {order_id}: ${amount} ({op}) at {evt_ts}")
    else:
        print("   No high-value orders found yet.")
    
    # Query 3: Latest state per order (deduplication)
    print("\n3. Latest state per order (deduplication via change_id):")
    result = con.execute("""
        SELECT order_id, op, amount, processed_at
        FROM orders_analytics
        WHERE (order_id, evt_ts) IN (
            SELECT order_id, MAX(evt_ts) FROM orders_analytics GROUP BY order_id
        )
        LIMIT 10
    """).fetchall()
    for order_id, op, amount, processed_at in result:
        print(f"   Order {order_id}: {op} | ${amount} | {processed_at}")
    
    # Query 4: Processing latency
    print("\n4. Processing latency (time from CDC to warehouse load):")
    result = con.execute("""
        SELECT 
            ROUND(AVG(EXTRACT(EPOCH FROM (loaded_at - CAST(evt_ts AS TIMESTAMP)))), 2) as avg_latency_sec
        FROM orders_analytics
    """).fetchall()
    if result and result[0][0]:
        print(f"   Average latency: {result[0][0]} seconds")
    
    # Query 5: Total data processed
    print("\n5. Total data statistics:")
    result = con.execute("""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT order_id) as unique_orders,
            COALESCE(SUM(amount), 0) as total_amount
        FROM orders_analytics
    """).fetchall()
    total_events, unique_orders, total_amount = result[0]
    print(f"   Total events: {total_events}")
    print(f"   Unique orders: {unique_orders}")
    print(f"   Total amount: ${total_amount}")
    
    print("\n=== Demo Complete ===\n")

if __name__ == "__main__":
    try:
        run_demo_queries()
    except Exception as e:
        print(f"Error running demo queries: {e}")
        sys.exit(1)
