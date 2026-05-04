"""
03_kafka_producer.py — Stream tin đăng BĐS vào Kafka topic 'realestate-stream'.
Simulate real-time: mỗi giây publish 1-3 tin đăng mới từ dataset đã scrape.
"""
import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC   = "realestate-stream"
CSV_PATH      = "/data/realestate_cleaned.csv"
DELAY_SECONDS = 0.3   # ~3 tin/giây

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)
print("Kafka producer connected")


def load_listings():
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], errors="ignore")
    df = df.dropna(subset=["listing_id"])
    listings = []
    for _, row in df.iterrows():
        record = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        listings.append(record)
    print(f"Loaded {len(listings)} listings từ {CSV_PATH}")
    return listings


listings = load_listings()
pass_num  = 0

while True:
    pass_num += 1
    random.shuffle(listings)
    print(f"\n--- Pass {pass_num}: streaming {len(listings)} listings ---")

    for i, listing in enumerate(listings):
        producer.send(
            topic=KAFKA_TOPIC,
            key=str(listing.get("listing_id", i)),
            value=listing,
        )
        if (i + 1) % 200 == 0:
            producer.flush()
            print(f"  sent {i + 1}/{len(listings)}")
        time.sleep(DELAY_SECONDS)

    producer.flush()
    print(f"Pass {pass_num} complete. Sleeping 15s...")
    time.sleep(15)
