import duckdb
import glob
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

con = duckdb.connect('analytics.duckdb')

# Create table with primary key for upsert semantics
con.execute('''
    CREATE TABLE IF NOT EXISTS orders_analytics (
        change_id VARCHAR PRIMARY KEY,
        op VARCHAR,
        order_id VARCHAR,
        amount DOUBLE,
        is_high_value BOOLEAN,
        evt_ts VARCHAR,
        processed_at VARCHAR,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')

parquet_dir = '/tmp/spark_output'
logger.info(f'Watching parquet directory: {parquet_dir}')
processed = set()

while True:
    files = glob.glob(os.path.join(parquet_dir, '*.parquet'))
    for p in files:
        if p in processed:
            continue
        try:
            con.execute(
                """
                INSERT OR REPLACE INTO orders_analytics 
                SELECT change_id, op, order_id, amount, is_high_value, evt_ts, processed_at, CURRENT_TIMESTAMP
                FROM read_parquet(?)
                """,
                (p,),
            )
            processed.add(p)
            logger.info(f'Loaded {p} into analytics warehouse')
        except Exception as e:
            logger.error(f'Error loading {p}: {e}')
    time.sleep(2)
