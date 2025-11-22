#!/bin/bash

# Real-Time ETL Pipeline Runner
# Orchestrates all components for a smooth demo

set -e

echo "=========================================="
echo "  Aladia Real-Time ETL Pipeline"
echo "=========================================="
echo ""

# Step 1: Start infrastructure
echo "[1/5] Starting Docker infrastructure..."
docker-compose up -d
echo "Waiting 30 seconds for services to stabilize..."
sleep 30

# Step 2: Initialize MongoDB
echo ""
echo "[2/5] Initializing MongoDB and seeding data..."
python3 init_mongo.py &
INIT_PID=$!
sleep 10

# Step 3: Start CDC
echo ""
echo "[3/5] Starting MongoDB CDC listener..."
python3 cdc_mongo.py --kafka localhost:9092 --topic cdc_orders &
CDC_PID=$!
sleep 5

# Step 4: Start Spark Streaming
echo ""
echo "[4/5] Starting Spark Streaming job..."
python3 spark_job.py \
  --kafka-bootstrap localhost:9092 \
  --topic cdc_orders \
  --checkpoint /tmp/spark-checkpoint \
  --output /tmp/spark_output &
SPARK_PID=$!
sleep 5

# Step 5: Start Sink Loader
echo ""
echo "[5/5] Starting DuckDB sink loader..."
python3 sink_writer.py &
SINK_PID=$!
sleep 5

echo ""
echo "=========================================="
echo "  Pipeline Running! (PIDs: $CDC_PID, $SPARK_PID, $SINK_PID)"
echo "=========================================="
echo ""
echo "To run demo queries:"
echo "  python3 demo_queries.py"
echo ""
echo "To stop the pipeline:"
echo "  kill $CDC_PID $SPARK_PID $SINK_PID $INIT_PID"
echo ""

# Wait for Ctrl+C
trap "echo 'Stopping pipeline...'; kill $CDC_PID $SPARK_PID $SINK_PID $INIT_PID 2>/dev/null; docker-compose down" EXIT
wait
