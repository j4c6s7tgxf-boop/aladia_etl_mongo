from kafka import KafkaProducer
import json
import time

class Producer:
    def __init__(self, bootstrap_servers='localhost:9092', request_timeout_ms=30000):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=request_timeout_ms,
            acks='all',
            max_retries=3
        )

    def send(self, topic, value, key=None):
        """Send message to Kafka with error handling."""
        k = key.encode('utf-8') if key else None
        future = self.producer.send(topic, value=value, key=k)
        try:
            result = future.get(timeout=10)
            return result
        except Exception as e:
            print("Error sending message:", e)
            time.sleep(1)
            self.producer.flush()
            raise
