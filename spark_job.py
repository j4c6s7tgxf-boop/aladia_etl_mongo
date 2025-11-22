import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType
import logging

# ----------------------------
# Logging setup
# ----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------
# Arguments
# ----------------------------
parser = argparse.ArgumentParser()
parser.add_argument('--kafka-bootstrap', default='localhost:9092', help='Kafka bootstrap servers')
parser.add_argument('--topic', default='cdc_orders', help='Kafka topic to subscribe')
parser.add_argument('--checkpoint', default='/tmp/spark-checkpoint', help='Spark checkpoint location')
parser.add_argument('--output', default='/tmp/spark_output', help='Parquet output path')
args = parser.parse_args()

# ----------------------------
# Spark session
# ----------------------------
spark = SparkSession.builder \
    .appName('mongo-cdc-spark') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Schema for Kafka JSON payload - UPDATED to match CDC structure
# ----------------------------
schema = StructType([
    StructField('change_id', StringType()),
    StructField('op', StringType()),
    StructField('order_id', StringType()),
    StructField('payload', MapType(StringType(), StringType())),
    StructField('evt_ts', StringType())
])

# ----------------------------
# Read from Kafka
# ----------------------------
raw = (spark.readStream.format('kafka')
       .option('kafka.bootstrap.servers', args.kafka_bootstrap)
       .option('subscribe', args.topic)
       .option('startingOffsets', 'earliest')
       .load())

# Convert value to string and parse JSON
value_str = raw.selectExpr('CAST(value AS STRING) as value', 'CAST(key AS STRING) as key')

json_df = value_str.select(from_json('value', schema, {"mode": "PERMISSIVE"}).alias('data'), 'key')

# Flatten nested JSON
flat = json_df.select(
    col('data.change_id'),
    col('data.op'),
    col('data.order_id'),
    col('data.payload'),
    col('data.evt_ts')
)

# ----------------------------
# Filter out malformed rows - enhanced filtering
# ----------------------------
clean = flat.filter(
    (col('change_id').isNotNull()) & 
    (col('order_id').isNotNull()) &
    (col('op').isNotNull()) &
    (col('op').isin(['I', 'U', 'R', 'D']))
)

# ----------------------------
# UDF to safely parse 'amount' from payload
# ----------------------------
def to_amount(payload):
    try:
        if payload is None:
            return None
        v = payload.get('amount')
        if v is None:
            return None
        return float(v)
    except (ValueError, TypeError, AttributeError):
        return None

to_amount_udf = udf(to_amount, 'double')

# ----------------------------
# Enrichment with error handling
# ----------------------------
enriched = clean.withColumn('amount', to_amount_udf(col('payload'))) \
                .withColumn('is_high_value', when(col('amount') >= 1000.0, True).otherwise(False)) \
                .withColumn('processed_at', current_timestamp()) \
                .select(
                    col('change_id'),
                    col('op'),
                    col('order_id'),
                    col('amount'),
                    col('is_high_value'),
                    col('evt_ts'),
                    col('processed_at')
                )

# ----------------------------
# Write to Parquet sink with exactly-once semantics
# ----------------------------
query = (enriched.writeStream
         .format('parquet')
         .option('path', args.output)
         .option('checkpointLocation', args.checkpoint)
         .outputMode('append')
         .start())

logger.info(f"[INFO] Spark Structured Streaming started. Writing to {args.output}")
query.awaitTermination()
