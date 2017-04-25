# -*- coding: utf-8 -*-
import time
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="10.25.115.53:9092,10.252.0.76:9092,10.44.184.245:9092",
    compression_type='gzip',
    retries=3,
    api_version=(0, 10),  # 支持自定义timestamp
)

topic_name = str("test_future")

future = producer.send(topic_name, value="Test"*100, timestamp_ms=time.time()*1000)

producer.flush()

record_metadata = future.get(timeout=10)

# Successful result returns assigned partition and offset
print (record_metadata.topic)
print (record_metadata.partition)
print (record_metadata.offset)