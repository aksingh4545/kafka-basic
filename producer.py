from kafka import KafkaProducer
import pandas as pd
import json
import time

print("Producer started...")

df = pd.read_csv("data/ecommerce_dataset_updated.csv")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic_name = "sales"


for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, message)
    print("Sent:", message)
    time.sleep(0.2)  

producer.flush()
print("Producer finished.")