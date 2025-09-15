#!/usr/bin/env python3
"""
Transaction Simulator (home-location realism)
Thesis: “NoSQL Databases in Stream Data Processing:
        Use-Case Anomaly Detection in Payment Card Transactions”

Generates realistic payment-card transactions, injects three anomaly types,
and publishes each record to an Apache Kafka topic.

- Normal tx: jitter around a per-card home location (reduces spatial FPs).
- location_change: baseline event + large jump for the same card.
- frequent_transaction: rapid burst (>=5) for the same card.
- Regular path randomly injects only high_value anomalies.

Author: Worku Dinkisa Demeke (revised)
"""

import json
import os
import random
import time
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple

from faker import Faker            # pip install Faker
from kafka import KafkaProducer    # pip install kafka-python
import uuid

# ------------------------------------------------------------------ #
# (1) CONFIGURATION — EDIT TO MATCH YOUR ENVIRONMENT                 #
# ------------------------------------------------------------------ #
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC             = "transactions"

NUM_CARDS        = 10_000
NUM_USERS        = 2_000
SPENDING_LIMIT   = Decimal("10000")            # per-card limit

# Overall anomaly rate used to *schedule* anomaly sequences per loop
ANOMALY_FRACTION = 0.05                        # ~5% cycles inject an anomaly class

# For visibility while testing (set SIM_VERBOSE=0 to silence)
VERBOSE = os.getenv("SIM_VERBOSE", "1") == "1"

# -------- Home-location realism (for normal behavior) --------
HOME_SIGMA_DEG   = 0.10   # ≈ 11 km std dev around home
JUMP_DEG_MIN     = 10.0   # ≈ 1,100 km minimum jump for location_change
JUMP_DEG_MAX     = 25.0   # ≈ 2,800 km maximum jump for location_change

# -------- Sequences to make rules detectable --------
FREQ_MIN_BURST   = 5
FREQ_MAX_BURST   = 8

# In the regular path, randomly inject ONLY high_value anomalies
RANDOM_ONLY_HIGH_VALUE = True

# ------------------------------------------------------------------ #
# (2) JSON helper for Decimal values                                 #
# ------------------------------------------------------------------ #
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

# ------------------------------------------------------------------ #
# (3) Faker and in-memory maps                                       #
# ------------------------------------------------------------------ #
faker = Faker()
card_user_map = {
    f"card_{i}": f"user_{random.randint(1, NUM_USERS)}"
    for i in range(NUM_CARDS)
}

# Per-card home (lat, lon)
def _gen_home() -> Tuple[float, float]:
    return (float(faker.latitude()), float(faker.longitude()))

home_loc_map: Dict[str, Tuple[float, float]] = {
    card: _gen_home() for card in card_user_map.keys()
}

# ------------------------------------------------------------------ #
# (4) Geo helpers                                                     #
# ------------------------------------------------------------------ #
def _clamp_lat(x: float) -> float:
    return max(-89.9, min(89.9, x))

def _wrap_lon(x: float) -> float:
    if x < -180.0 or x > 180.0:
        x = ((x + 180.0) % 360.0) - 180.0
    return x

def _jitter_near(lat: float, lon: float, sigma_deg: float = HOME_SIGMA_DEG) -> Tuple[float, float]:
    jl = random.gauss(0.0, sigma_deg)
    jg = random.gauss(0.0, sigma_deg)
    return _clamp_lat(lat + jl), _wrap_lon(lon + jg)

# ------------------------------------------------------------------ #
# (5) Build a single transaction (optionally force anomaly/location) #
# ------------------------------------------------------------------ #
def build_transaction(
    card_id: str,
    force_anomaly: Optional[str] = None,      # "high_value" | "frequent_transaction" | "location_change" | None
    loc: Optional[Tuple[float, float]] = None # (lat, lon) base location if you want to pin it
) -> Dict[str, Any]:
    user_id = card_user_map[card_id]

    # Choose a base location: provided 'loc' or jitter near card's home
    if loc is not None:
        base_lat, base_lon = float(loc[0]), float(loc[1])
    else:
        home_lat, home_lon = home_loc_map[card_id]
        base_lat, base_lon = _jitter_near(home_lat, home_lon)

    latitude, longitude = base_lat, base_lon
    value   = Decimal(f"{random.uniform(1, 1000):.2f}")
    anomaly = None

    # Decide anomaly
    if force_anomaly is not None:
        anomaly = force_anomaly
    else:
        # In the regular path, only allow high_value as random anomaly (to avoid unlabeled stateful ones)
        if RANDOM_ONLY_HIGH_VALUE and random.random() < ANOMALY_FRACTION:
            anomaly = "high_value"

    # Mutate fields based on anomaly
    if anomaly == "high_value":
        upper = float(SPENDING_LIMIT * Decimal("1.5"))
        value = Decimal(f"{random.uniform(float(SPENDING_LIMIT), upper):.2f}")

    elif anomaly == "location_change":
        # Apply a large jump *from the base* (either provided loc or near home)
        dlat = random.uniform(JUMP_DEG_MIN, JUMP_DEG_MAX) * random.choice([-1.0, 1.0])
        dlon = random.uniform(JUMP_DEG_MIN, JUMP_DEG_MAX) * random.choice([-1.0, 1.0])
        latitude  = _clamp_lat(base_lat + dlat)
        longitude = _wrap_lon(base_lon + dlon)

    # Note: frequent_transaction has no field mutation; it's a sequence-level pattern

    return {
        "transaction_id": str(uuid.uuid4()),
        "producer_ts_ms": int(time.time_ns() // 1_000_000),
        "card_id": card_id,
        "user_id": user_id,
        "location": {"latitude": float(latitude), "longitude": float(longitude)},
        "transaction_value": float(value),
        "spending_limit": float(SPENDING_LIMIT),
        "anomaly": anomaly,  # ground truth for evaluation
    }

# ------------------------------------------------------------------ #
# (6) Kafka producer                                                 #
# ------------------------------------------------------------------ #
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, cls=DecimalEncoder).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all"
)

# ------------------------------------------------------------------ #
# (7) Sequence helpers                                               #
# ------------------------------------------------------------------ #
def send_location_change_sequence(card_id: str) -> None:
    """Emit baseline (normal near home) then a large jump for the same card."""
    home_lat, home_lon = home_loc_map[card_id]
    base_loc = _jitter_near(home_lat, home_lon)

    base = build_transaction(card_id, force_anomaly=None, loc=base_loc)  # baseline (normal)
    producer.send(KAFKA_TOPIC, key=card_id, value=base)
    if VERBOSE:
        print("Sent baseline for loc_change:", base)

    time.sleep(random.uniform(0.02, 0.08))  # keep within your Flink LOC_WINDOW

    jumped = build_transaction(card_id, force_anomaly="location_change", loc=base_loc)
    producer.send(KAFKA_TOPIC, key=card_id, value=jumped)
    if VERBOSE:
        print("Sent location_change:", jumped)

def send_frequent_burst(card_id: str) -> None:
    """Emit >=5 events quickly for the same card (first labeled as frequent_transaction)."""
    burst = random.randint(FREQ_MIN_BURST, FREQ_MAX_BURST)

    first = build_transaction(card_id, force_anomaly="frequent_transaction")
    producer.send(KAFKA_TOPIC, key=card_id, value=first)
    if VERBOSE:
        print("Sent frequent_transaction (seed):", first)

    for _ in range(burst - 1):
        time.sleep(random.uniform(0.02, 0.08))
        txn = build_transaction(card_id, force_anomaly=None)
        producer.send(KAFKA_TOPIC, key=card_id, value=txn)
        if VERBOSE:
            print("Sent burst txn:", txn)

# ------------------------------------------------------------------ #
# (8) Main loop                                                      #
# ------------------------------------------------------------------ #
def run_simulator() -> None:
    print("🚀 Transaction simulator started — Ctrl-C to exit.")
    try:
        while True:
            card_id = random.choice(list(card_user_map))

            r = random.random()
            if r < ANOMALY_FRACTION / 3:
                # location_change: baseline + jump
                send_location_change_sequence(card_id)

            elif r < 2 * ANOMALY_FRACTION / 3:
                # frequent_transaction: short burst
                send_frequent_burst(card_id)

            else:
                # regular traffic (mostly normal; may include random high_value if enabled)
                txn = build_transaction(card_id, force_anomaly=None)
                producer.send(KAFKA_TOPIC, key=card_id, value=txn)
                if VERBOSE:
                    print("Sent normal/high_value:", txn)

            # overall pacing
            time.sleep(random.uniform(0.1, 0.3))

    except KeyboardInterrupt:
        print("\n🛑 Simulator stopped.")
    finally:
        producer.flush(5)
        producer.close()

if __name__ == "__main__":
    run_simulator()
