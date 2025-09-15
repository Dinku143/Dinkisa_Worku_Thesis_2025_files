#!/usr/bin/env python3
# kafka_to_mongo.py
#
# Consumes from Kafka topic `processed_transactions` and writes to MongoDB.
# - Exactly-once at sink: _id = transaction_id + upsert
# - Server-side ingest_ts via update-with-pipeline (MongoDB 4.2+), with safe fallback
# - Commits Kafka offsets only AFTER successful Mongo write
# - Micro-batching with size/time triggers

import os, json, time, signal, sys
from typing import List
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
from pymongo.errors import OperationFailure

# -----------------------
# Config (override via env if needed)
# -----------------------
TOPIC             = os.getenv("KAFKA_TOPIC", "processed_transactions")  # underscore!
BOOTSTRAP         = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID          = os.getenv("KAFKA_GROUP", "mongo-writer")

MONGODB_URI       = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME           = os.getenv("MONGODB_DB", "fraud_detection")
COLLECTION        = os.getenv("MONGODB_COLL", "processed_transactions")

BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))   # max ops per flush
FLUSH_MS          = int(os.getenv("FLUSH_MS",  "100"))    # max added latency (ms)
MAX_POLL_RECORDS  = int(os.getenv("MAX_POLL",  "500"))

# -----------------------
# Kafka consumer
# -----------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,                         # commit after DB ack
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    max_poll_records=MAX_POLL_RECORDS,
    request_timeout_ms=30000,
    session_timeout_ms=20000,
    heartbeat_interval_ms=3000,
)
print(f"[INFO] Subscribed to '{TOPIC}' as group '{GROUP_ID}'.")

# -----------------------
# MongoDB client & indexes
# -----------------------
mongo = MongoClient(MONGODB_URI)
col = mongo[DB_NAME][COLLECTION]
# _id unique index exists by default; don't recreate it.
col.create_index([("card_id", 1), ("ingest_ts", 1)])
col.create_index([("producer_ts_ms", 1)])
print(f"[INFO] MongoDB connected: db='{DB_NAME}', coll='{COLLECTION}'. Indexes ensured.")

# -----------------------
# Batching + pipeline support flag
# -----------------------
docs_batch: List[dict] = []
last_flush = time.time()
total_written = 0
running = True
USE_PIPELINE = True  # try server-side $$NOW first; auto-fallback if not supported

def build_ops_pipeline(batch: List[dict]) -> List[UpdateOne]:
    ops: List[UpdateOne] = []
    for d in batch:
        txid = d["transaction_id"]
        ops.append(UpdateOne(
            {"_id": txid},
            [
                {"$set": {
                    "_id": txid,
                    "producer_ts_ms": d.get("producer_ts_ms"),
                    "flink_proc_ts_ms": d.get("flink_proc_ts_ms"),
                    "card_id": d.get("card_id"),
                    "user_id": d.get("user_id"),
                    "location": d.get("location"),
                    "transaction_value": d.get("transaction_value"),
                    "spending_limit": d.get("spending_limit"),
                    "ground_truth_anomaly": d.get("ground_truth_anomaly"),
                    "anomaly": d.get("anomaly"),
                    # SERVER timestamp only if missing (first insert)
                    "ingest_ts": {"$ifNull": ["$ingest_ts", "$$NOW"]}
                }}
            ],
            upsert=True
        ))
    return ops

def build_ops_classic(batch: List[dict]) -> List[UpdateOne]:
    """Fallback for MongoDB <4.2 (no pipeline updates). Uses client UTC time."""
    ops: List[UpdateOne] = []
    now_dt = datetime.now(timezone.utc)
    for d in batch:
        txid = d["transaction_id"]
        ops.append(UpdateOne(
            {"_id": txid},
            {
                "$setOnInsert": {**d, "_id": txid, "ingest_ts": now_dt},
                "$set": {
                    "producer_ts_ms": d.get("producer_ts_ms"),
                    "flink_proc_ts_ms": d.get("flink_proc_ts_ms"),
                    "card_id": d.get("card_id"),
                    "user_id": d.get("user_id"),
                    "location": d.get("location"),
                    "transaction_value": d.get("transaction_value"),
                    "spending_limit": d.get("spending_limit"),

                    "ground_truth_anomaly": d.get("ground_truth_anomaly"),
                
                    "anomaly": d.get("anomaly"),
                }
            },
            upsert=True
        ))
    return ops

def flush():
    """Flush current batch to MongoDB, then commit Kafka offsets."""
    global docs_batch, last_flush, total_written, USE_PIPELINE
    if not docs_batch:
        return
    try:
        ops = build_ops_pipeline(docs_batch) if USE_PIPELINE else build_ops_classic(docs_batch)
        res = col.bulk_write(ops, ordered=False)
        consumer.commit()  # commit offsets only after DB success
        total_written += (res.upserted_count + res.modified_count)
        print(f"[INFO] Flushed {len(docs_batch)} docs "
              f"(upserts={res.upserted_count}, updates={res.modified_count}); total={total_written}.")
        docs_batch = []
        last_flush = time.time()
    except OperationFailure as e:
        msg = str(e)
        # If pipeline update unsupported or mis-specified, fall back automatically
        if USE_PIPELINE and ("pipeline" in msg.lower() or "unrecognized" in msg.lower() or e.code in (40324, 9, 72)):
            print("[WARN] Pipeline update not supported here; falling back to classic upsert with client timestamp.")
            USE_PIPELINE = False
            # Retry once with classic ops
            ops = build_ops_classic(docs_batch)
            res = col.bulk_write(ops, ordered=False)
            consumer.commit()
            total_written += (res.upserted_count + res.modified_count)
            print(f"[INFO] Flushed (fallback) {len(docs_batch)} docs; total={total_written}.")
            docs_batch = []
            last_flush = time.time()
        else:
            print(f"[ERROR] Mongo flush failed; NOT committing offsets. Reason: {e}")
            time.sleep(0.5)
            # Keep docs_batch for retry on next loop
    except Exception as e:
        print(f"[ERROR] Mongo flush failed; NOT committing offsets. Reason: {e}")
        time.sleep(0.5)

def shutdown(*_):
    """Graceful shutdown: flush, close, exit."""
    global running
    running = False
    try: flush()
    except: pass
    try: consumer.close()
    except: pass
    try: mongo.close()
    except: pass
    print("[INFO] Clean shutdown.")
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

print("[INFO] Waiting for messages… (Ctrl-C to stop)")

# -----------------------
# Consume loop
# -----------------------
while running:
    recs = consumer.poll(timeout_ms=1000, max_records=MAX_POLL_RECORDS)
    got_msgs = False

    for tp, msgs in recs.items():
        for m in msgs:
            d = m.value
            got_msgs = True

            txid = d.get("transaction_id")
            if not txid:
                continue  # skip malformed

            # Back-compat: legacy 'timestamp' → 'producer_ts_ms'
            if "producer_ts_ms" not in d and "timestamp" in d:
                d["producer_ts_ms"] = d["timestamp"]

            docs_batch.append(d)

    now = time.time()
    if docs_batch and (len(docs_batch) >= BATCH_SIZE or (now - last_flush) * 1000 >= FLUSH_MS):
        flush()
    elif (not got_msgs) and (now - last_flush) * 1000 >= FLUSH_MS:
        flush()

# Safety fallback
shutdown()


# This script consumes messages from a Kafka topic and writes them to a MongoDB collection.
#which mongosh - path for MongoDB shell
#/usr/bin/mongod
#/usr/bin/mongosh


