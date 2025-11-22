# Aladia Real-Time ETL Pipeline with MongoDB CDC

A fully functional, production-grade ETL pipeline demonstrating real-time data ingestion, transformation, and analytics using MongoDB Change Streams, Apache Kafka, PySpark, and DuckDB.

## Features

- **Real-Time CDC**: MongoDB Change Streams for low-latency change capture
- **Exactly-Once Semantics**: End-to-end deduplication and ordering guarantees
- **Fault Tolerance**: Checkpointing and recovery across all components
- **Schema Evolution**: Graceful handling of missing/malformed data
- **Enrichment**: Data transformation (amount parsing, high-value flagging)
- **Scalability**: Architecture supports 10x volume growth

## Architecture

\`\`\`
MongoDB (CDC) → Kafka (Queue) → Spark (Transform) → Parquet (Stage) → DuckDB (Warehouse)
\`\`\`

See `ARCHITECTURE.md` for detailed diagrams and design decisions.

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- 4GB+ available RAM

### Setup & Run

1. **Start Infrastructure**
\`\`\`bash
docker-compose up -d
\`\`\`
Wait 30 seconds for services to stabilize.

2. **Seed MongoDB & Start CDC**
\`\`\`bash
python3 init_mongo.py &
# Runs in background: inserts 5 sample orders, then updates randomly every 10s
\`\`\`

3. **Start Spark Streaming Job** (in a new terminal)
\`\`\`bash
python3 spark_job.py \
  --kafka-bootstrap localhost:9092 \
  --topic cdc_orders \
  --checkpoint /tmp/spark-checkpoint \
  --output /tmp/spark_output
\`\`\`

4. **Start DuckDB Sink Loader** (in another terminal)
\`\`\`bash
python3 sink_writer.py
\`\`\`

5. **Run Demo Queries** (when data flows)
\`\`\`bash
python3 demo_queries.py
\`\`\`

### Expected Output

\`\`\`
=== ETL Pipeline Analytics Demo ===

1. Event counts by operation type:
   I: 5
   U: 15
   
2. High-value orders (amount >= 1000):
   No high-value orders found yet.
   
3. Latest state per order:
   Order abc123: U | $25.0 | 2024-01-15 12:34:56
   ...
\`\`\`

## Data Flow & Guarantees

### Operation Types
- `I` = Insert (new order)
- `U` = Update (status/amount change)
- `D` = Delete (order cancelled)
- `R` = Replace (full document replacement)

### End-to-End Guarantees
1. **Ordering**: Per-order maintained via Kafka partitioning by `order_id`
2. **Exactly-Once**: Spark checkpoints + DuckDB primary key deduplication
3. **Latency**: < 1 second typical (CDC → Kafka → Spark → DuckDB)
4. **Durability**: Kafka persists all events; Spark checkpoints track progress

### Failure Recovery
- **CDC Resumes**: Uses MongoDB resume tokens
- **Spark Recovers**: Reads checkpoint on restart
- **Deduplication**: DuckDB upsert prevents duplicates

## Design Decisions & Trade-Offs

### Why MongoDB Change Streams?
✅ **Pros**: Low-latency, native support, no external tool
❌ **Cons**: MongoDB-specific, not universal CDC approach

### Why Kafka?
✅ **Pros**: Durable, scalable, industry-standard
❌ **Cons**: Extra component; adds complexity

### Why Spark?
✅ **Pros**: Fault-tolerant, exactly-once semantics, scalable
❌ **Cons**: Higher resource overhead than simpler tools

### Why DuckDB?
✅ **Pros**: SQL interface, file-based, easy testing
❌ **Cons**: Single-process; scale to Snowflake/BigQuery for production

## Scalability: What Breaks at 10x Volume?

**Priority Order**:
1. **DuckDB** (single-file limitation) → Migrate to cloud warehouse
2. **MongoDB single replica set** → Shard the orders collection
3. **Kafka single broker** → Add brokers, increase partitions
4. **Spark cluster size** → Add executors/nodes

See `ARCHITECTURE.md` scaling section for details.

## Production Readiness Checklist

- [x] Exactly-once semantics
- [x] Error handling (malformed data)
- [x] Checkpointing/recovery
- [x] Schema validation
- [ ] Monitoring/alerting (recommend Prometheus + Grafana)
- [ ] Data quality tests (recommend Great Expectations)
- [ ] Secrets management (use Vault/AWS Secrets Manager)
- [ ] CI/CD pipeline (GitHub Actions, etc.)
- [ ] Rate limiting & backpressure
- [ ] SLA tracking & metrics

## File Structure

\`\`\`
aladia-etl-mongo/
├── docker-compose.yml        # Infrastructure setup
├── init_mongo.py            # Seed MongoDB with sample data
├── cdc_mongo.py             # Change stream listener → Kafka
├── kafka_producer.py        # Kafka producer wrapper
├── spark_job.py             # PySpark Structured Streaming
├── sink_writer.py           # Parquet → DuckDB loader
├── demo_queries.py          # Analytics query examples
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── ARCHITECTURE.md         # Detailed design rationale
\`\`\`

## Testing

\`\`\`bash
# Unit test: CDC event format
python3 -m pytest tests/test_cdc_format.py

# Integration test: end-to-end pipeline (requires Docker)
python3 -m pytest tests/test_integration.py -s

# Load test: simulate high volume
python3 tests/load_test.py --events 10000 --rate 100/sec
\`\`\`

## Monitoring & Debugging

### Check Kafka topic contents
\`\`\`bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc_orders \
  --from-beginning
\`\`\`

### View DuckDB warehouse
\`\`\`bash
sqlite3 analytics.duckdb
> SELECT COUNT(*) FROM orders_analytics;
\`\`\`

### Spark job logs
\`\`\`bash
tail -f /tmp/spark_output/*.log
\`\`\`

## Key Metrics

| Metric | Target | Impact |
|--------|--------|--------|
| CDC → Kafka latency | < 100ms | Event freshness |
| Spark processing latency | < 1s | Analytics timeliness |
| DuckDB query time (1M rows) | < 500ms | User experience |
| Kafka consumer lag | 0 (steady-state) | Data freshness |

## Further Reading

- [MongoDB Change Streams Docs](https://www.mongodb.com/docs/manual/changeStreams/)
- [Apache Kafka Design](https://kafka.apache.org/documentation/#design)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [DuckDB SQL](https://duckdb.org/docs/sql/introduction)
- [Exactly-Once Semantics](https://kafka.apache.org/documentation/#semantics)

## Support

For issues or questions:
1. Check logs in component terminals
2. Review `ARCHITECTURE.md` for design rationale
3. See `demo_queries.py` for example analytics
4. File an issue with logs and reproduction steps

---

**Built for Aladia Interview Challenge** | MongoDB CDC variant | PySpark ETL
\`\`\`
