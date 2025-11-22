# Design Rationale & Trade-Off Analysis

## Challenge Requirements Addressed

### 1. Real-Time ETL Pipeline ✓
- MongoDB change streams capture inserts/updates/deletes in milliseconds
- Kafka ensures events reach consumers without loss
- Spark Structured Streaming processes events as they arrive

### 2. CDC Implementation ✓
**Choice: MongoDB Change Streams (native)**
- Pro: Low latency, no external tool overhead, works with replica sets
- Con: MongoDB-specific (not universal), requires replica set mode
- Alternative considered: Debezium (more portable, but heavier)

### 3. Message Queue ✓
**Choice: Kafka**
- Pro: Industry-standard, partitioning by order_id preserves order, durable
- Con: Adds infrastructure complexity
- Alternative considered: Redis Streams (simpler, but less durable)

### 4. Data Processing ✓
**Choice: Apache Spark Structured Streaming**
- Pro: Exactly-once semantics via checkpointing, fault tolerance, scales
- Con: Higher resource overhead than simple consumers
- Alternative considered: Flink (similar, but higher learning curve)

### 5. Data Warehouse ✓
**Choice: DuckDB (local development), Snowflake/BigQuery (production)**
- Pro: DuckDB is lightweight and file-based for testing
- Con: Single-process limitation; real warehouse needed for 10x scale
- Deduplication: Primary key on change_id ensures exactly-once

---

## Architectural Decisions

### Order Preservation
**Problem**: Events for the same order must maintain strict ordering
**Solution**: Kafka partitioning by `order_id` (key)
**Trade-off**: Ordering only per-key; allows parallelism across orders

### Exactly-Once Semantics
**Problem**: How to prevent duplicates across failures?
**Solution**: Three-layer approach:
1. Kafka: `acks=all` + persists to disk
2. Spark: Checkpoints + idempotent output
3. DuckDB: Primary key constraint on change_id
**Trade-off**: Adds latency (~100-200ms), but guarantees correctness

### Error Handling
**Problem**: Malformed data breaks the pipeline
**Solution**: 
- JSON parsing with PERMISSIVE mode
- UDFs with try-catch blocks
- Null checks on critical fields
**Trade-off**: Silent failures on malformed data (logged); could alert instead

### Schema Evolution
**Problem**: MongoDB changes document structure over time
**Solution**: Flexible Map types in Spark, UDFs handle missing fields
**Trade-off**: Type safety sacrificed for flexibility; recommend schema registry for production

---

## Scalability Bottleneck Analysis

### Current Setup (1x volume)
- Single MongoDB replica set ✓
- Single Kafka broker ✓
- Spark driver + local workers ✓
- DuckDB single file ✓

### At 10x Volume Growth

**Breaking Points (priority)**:

1. **DuckDB** (FIRST BREAK)
   - Single-process OLAP database
   - Limit: ~100GB per file
   - Fix: Migrate to cloud warehouse (Snowflake, BigQuery, Redshift)

2. **MongoDB** (SECOND BREAK)
   - Single replica set limit
   - Limit: ~2TB per server
   - Fix: Implement sharding on order_id

3. **Kafka** (THIRD BREAK)
   - Single broker bottleneck
   - Limit: Network throughput (~100MB/sec)
   - Fix: Add brokers, increase partitions (10x = 10 partitions)

4. **Spark** (FINAL BREAK)
   - Local cluster exhaustion
   - Limit: CPU/memory of single machine
   - Fix: Distributed cluster (16+ cores)

### Scaling Action Plan

| Volume | MongoDB | Kafka | Spark | Warehouse |
|--------|---------|-------|-------|-----------|
| 1x | Replica set | 1 broker, 1 part | Local | DuckDB |
| 3x | Replica set | 1 broker, 3 parts | 4 cores | DuckDB |
| 10x | Sharded (3) | 3 brokers, 10 parts | Distributed cluster | Snowflake |

---

## Why Not Other Approaches?

### Why not polling-based CDC?
- Latency: Every 1-5 seconds (vs. milliseconds for streams)
- Load: Repeated full-table scans drain resources
- Complexity: Handle state, watermarks, missed windows

### Why not RabbitMQ instead of Kafka?
- Kafka is optimized for streaming data & replay
- RabbitMQ better for task queues (fire-and-forget)
- Kafka's partitioning preserves order per key

### Why not just S3/Parquet for the warehouse?
- Query latency: 10-30s (vs. <1s for columnar DB)
- Schema versioning: Harder to manage
- ACID guarantees: None (eventually consistent)

### Why not Lambda architecture (batch + stream)?
- Complexity: Two code paths to maintain
- Lambda best for scenarios requiring accuracy + speed
- Here, speed already achieved via Spark streaming

---

## Monitoring & Observability

### Key Metrics to Track
1. **CDC lag**: Time from MongoDB write to Kafka publish (target: <100ms)
2. **Kafka lag**: Consumer offset vs. broker offset (target: 0)
3. **Spark latency**: Event arrival to Parquet write (target: <1s)
4. **DuckDB query time**: Analytic queries P95 (target: <500ms)
5. **Data quality**: % malformed events (target: <0.1%)

### Recommended Stack
- **Metrics**: Prometheus
- **Dashboards**: Grafana
- **Alerting**: PagerDuty (lag > 5s)
- **Logging**: ELK Stack

---

## Security Considerations

### Production Hardening
1. **Credentials**: Use Vault or AWS Secrets Manager (no hardcoded keys)
2. **Network**: VPC isolation, TLS for all connections
3. **Access Control**: RBAC for Kafka topics, MongoDB collections
4. **Data Privacy**: Mask PII before publishing (order_id, user_id)
5. **Audit Logging**: Track all schema changes, deletions

### Current Development Setup
- All connections localhost (safe for demo)
- No authentication (not needed for Docker containers)
- Logging to stdout (sufficient for debugging)

---

## Testing Strategy

### Unit Tests (Component-level)
- CDC event normalization
- Spark UDF logic (amount parsing)
- Sink deduplication logic

### Integration Tests (End-to-end)
- Insert → CDC → Kafka → Spark → DuckDB flow
- Verify exactly-once (no duplicates)
- Test failure recovery (restart components mid-stream)

### Load Tests
- Simulate 1000 events/sec through pipeline
- Measure latency distribution (p50, p95, p99)
- Identify resource bottlenecks

---

## Future Enhancements

1. **Schema Registry**: Confluent Schema Registry for versioning
2. **Streaming Analytics**: Real-time dashboards (Superset, Metabase)
3. **Data Quality**: Great Expectations for anomaly detection
4. **Cost Optimization**: Tiered storage (hot/warm/cold data)
5. **Advanced CDC**: Debezium for database-agnostic approach
