# Real-Time ETL Pipeline Architecture

## System Overview

\`\`\`
┌─────────────────┐
│   MongoDB       │
│   (Source DB)   │
│   with Replica  │
│      Set        │
└────────┬────────┘
         │ CDC Events
         ▼
┌─────────────────┐
│  Change Stream  │ (Low-latency native CDC)
│   Listener      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Topic     │ (Decoupling & Durability)
│  cdc_orders     │ Partitioned by order_id
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Spark Streaming │ (ETL: Transform & Enrich)
│ PySpark Job     │ • Parse JSON
│                 │ • Handle schema evolution
│                 │ • UDF: amount parsing
│                 │ • Flag high-value orders
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Parquet Files  │ (Intermediate Storage)
│  /tmp/spark_    │
│    output/      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    DuckDB       │ (Analytics Warehouse)
│   analytics.db  │ Upsert-based dedup
└─────────────────┘
\`\`\`

## Component Design Rationale

### 1. Source Database: MongoDB
- **Why**: Document-oriented, native change streams support
- **Trade-offs**: No need for external polling; built-in support for replica sets
- **Scaling**: Horizontally scalable via sharding

### 2. CDC: MongoDB Change Streams
- **Why**: 
  - Native support (no external tool needed)
  - Low latency, event-driven
  - Works with single-node replica sets
- **Trade-offs**: Tied to MongoDB; not language-agnostic like Debezium
- **Exactly-Once**: Resume tokens enable recovery from failures

### 3. Message Queue: Apache Kafka
- **Why**:
  - Durability: persists events on disk
  - Decoupling: producers/consumers independent
  - Scalability: partitioning by `order_id` preserves order per order
- **Delivery Semantics**: 
  - `acks=all`: wait for all replicas (safer)
  - Partitioning ensures per-key ordering
  - Max retries for transient failures
- **Scaling**: Add brokers + replication factor

### 4. Stream Processing: Apache Spark Structured Streaming
- **Why**:
  - Fault-tolerant: checkpointing + recovery
  - Exactly-once semantics with idempotent writes
  - Scalable: distributes work across nodes
- **Features**:
  - JSON parsing with error tolerance
  - Watermarking for late data
  - UDFs for custom logic (amount parsing)
- **Output**: Append-only to Parquet (idempotent)

### 5. Analytics Sink: DuckDB
- **Why**:
  - Lightweight SQL OLAP database
  - File-based (portable)
  - Upsert support for deduplication
- **Production Alternative**: BigQuery, Snowflake, Redshift
- **Deduplication**: Primary key on `change_id` ensures exactly-once

## Delivery Semantics

### End-to-End Guarantee: Exactly-Once
1. **Kafka Producer**: `acks=all` + retries ensure durable write
2. **Spark Consumer**: Checkpoints track offset progress
3. **Parquet Output**: Idempotent append (same data, no duplicates)
4. **DuckDB Sink**: Primary key constraint de-duplicates

### Failure Scenarios
- **CDC fails**: Resume token restarts from last checkpoint
- **Kafka broker fails**: Replication (RF=1 locally, higher in prod)
- **Spark job fails**: Checkpoint recovery prevents data loss
- **Network partition**: Timeout + retry strategy

## Scalability Analysis

### 10x Volume Growth Impact

**Component Bottlenecks (in order)**:
1. **MongoDB** → Sharding needed; CDC scales with shards
2. **Kafka** → Add brokers; increase partition count
3. **Spark** → More executors; larger cluster
4. **DuckDB** → **Would break first** (single-process); migrate to BigQuery/Snowflake

### Scaling Actions

| Component | 1x Volume | 10x Volume |
|-----------|-----------|-----------|
| MongoDB | 1 replica set | Sharded cluster (3 shards) |
| Kafka | 1 broker, 1 partition | 3 brokers, 10 partitions |
| Spark | Local/small cluster | Distributed cluster (16+ cores) |
| DuckDB | Single file | Move to cloud warehouse (Snowflake/BigQuery) |

## Edge Cases Handled

1. **Malformed JSON**: PERMISSIVE mode + null checks
2. **Missing fields**: UDF defensive coding (try-except)
3. **Type mismatches**: Safe type conversion in UDFs
4. **Duplicate events**: DuckDB primary key constraint
5. **Late arrivals**: Spark checkpointing handles recovery
6. **Schema evolution**: Flexible JSON parsing

## Production Deployment Recommendations

1. **Schema Registry**: Integrate Confluent Schema Registry for versioning
2. **Monitoring**: Add Prometheus metrics + Grafana dashboards
3. **Alerting**: Set up alerts on lag, error rates, late data
4. **Data Quality**: Add expectations framework (Great Expectations)
5. **Testing**: Unit tests on UDFs, integration tests on pipeline
6. **Governance**: Data lineage tracking, PII masking
