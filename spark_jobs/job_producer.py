from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("data/raw/Uncleaned_DS_jobs.csv")

for _, row in df.iterrows():
    job = row.to_dict()
    producer.send("job_postings", job)
    print("Sent job:", job.get("Job Title", "Unknown"))
    time.sleep(2)