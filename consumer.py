from kafka import KafkaConsumer
import json

print("Consumer started...")

consumer = KafkaConsumer(
    "sales",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest", 
    enable_auto_commit=True,
    group_id="sales-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)


for message in consumer:
    print("Received:", message.value)