#!/usr/bin/env python3
# fraud_detection_flink.py
#
# Kafka (transactions) -> Flink -> Kafka (processed_transactions)
# Rules: high_value (threshold), frequent_transaction (rolling count), location_change (geo jump)
# Hardened: no-restart (for debugging), robust keyBy, >= comparator, anomaly_type in output,
#           explicit earliest offsets with fresh consumer group, broad error guards.

import json
import time
import math
import os
from typing import List, Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.common.restart_strategy import RestartStrategies  # NEW

# ------------------------------
# Tunables
# ------------------------------
FREQ_WINDOW_SEC   = 60          # processing-time window
FREQ_THRESHOLD    = 6           # >=5 events within window => frequent_transaction
FREQ_MIN_TOTAL_VALUE  = 300.0      # keep or raise to 400 if still many FPs
FREQ_COOLDOWN_SEC     = 90
# ---- Location rule (stricter but still catches jumps) ----
LOC_DISTANCE_KM       = 1500.0     # was 2000 in the precision-first file; 1500 suits home-jitter normals
LOC_WINDOW_MIN        = 15
LOC_COOLDOWN_SEC      = 120
DEBUG_PRINT       = os.getenv("FLINK_DEBUG", "0") == "1"  # FLINK_DEBUG=1 to print to TaskManager logs

# Kafka topics & group
KAFKA_BOOTSTRAP    = "localhost:9092"
SRC_TOPIC          = "transactions"
SINK_TOPIC         = "processed_transactions"
CONSUMER_GROUP     = "flink-fraud-detector-v4"   # fresh group to avoid stale offsets

# ------------------------------
# Utilities
# ------------------------------
def safe_float(x: Optional[object], default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def now_ms() -> int:
    return int(time.time() * 1000)

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon/2)**2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ------------------------------
# Stateful rule engine (per card_id)
# ------------------------------
class RuleEngine(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        self.ts_buffer = runtime_context.get_list_state(
            ListStateDescriptor("ts_buffer_ms", Types.LONG())
        )
        self.last_loc = runtime_context.get_state(
            ValueStateDescriptor("last_loc", Types.TUPLE([Types.FLOAT(), Types.FLOAT(), Types.LONG()]))
        )

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        try:
            # Parse JSON safely
            try:
                d = json.loads(value)
            except Exception:
                return  # skip malformed record

            tv = safe_float(d.get("transaction_value"))
            sl = safe_float(d.get("spending_limit"), default=1e12)  # very large if missing
            loc = d.get("location") or {}
            lat = safe_float(loc.get("latitude"), default=None)
            lon = safe_float(loc.get("longitude"), default=None)
            pt_now = ctx.timer_service().current_processing_time()

            detected: List[str] = []

            # Rule 1: high_value
            if tv > sl:
                detected.append("high_value")

            # Rule 2: frequent_transaction (rolling count)
            if FREQ_THRESHOLD > 0 and FREQ_WINDOW_SEC > 0:
                try:
                    buf_iter = self.ts_buffer.get()
                    buf = list(buf_iter) if buf_iter is not None else []
                except Exception:
                    buf = []
                buf.append(pt_now)
                cutoff = pt_now - (FREQ_WINDOW_SEC * 1000)
                buf = [t for t in buf if t >= cutoff]
                try:
                    self.ts_buffer.update(buf)
                except Exception:
                    # if state update fails, drop counting for this event
                    pass
                if len(buf) >= FREQ_THRESHOLD:        # CRITICAL: '>='
                    detected.append("frequent_transaction")

            # Rule 3: location_change (large jump within window)
            if LOC_DISTANCE_KM > 0 and LOC_WINDOW_MIN > 0 and lat is not None and lon is not None:
                try:
                    last = self.last_loc.value()
                except Exception:
                    last = None
                if last is not None:
                    try:
                        last_lat, last_lon, last_ts = last
                        if (pt_now - last_ts) <= (LOC_WINDOW_MIN * 60 * 1000):
                            dist = haversine_km(lat, lon, float(last_lat), float(last_lon))
                            if dist >= LOC_DISTANCE_KM:
                                detected.append("location_change")
                    except Exception:
                        pass
                try:
                    self.last_loc.update((float(lat), float(lon), pt_now))
                except Exception:
                    pass

            anomaly = bool(detected)

            out = {
                "transaction_id": d.get("transaction_id"),
                "producer_ts_ms": int(safe_float(d.get("producer_ts_ms"), default=now_ms())),
                "flink_proc_ts_ms": now_ms(),
                "card_id": d.get("card_id"),
                "user_id": d.get("user_id"),
                "location": d.get("location"),
                "transaction_value": tv,
                "spending_limit": sl,
                "ground_truth_anomaly": d.get("anomaly") if "anomaly" in d else None,
                "anomaly": anomaly,
                "anomaly_type": detected[0] if detected else None,
                "detected_types": detected
            }

            if DEBUG_PRINT and anomaly:
                print("[DETECT]", json.dumps({"card_id": out["card_id"], "detected": out["detected_types"]}))

            yield json.dumps(out, ensure_ascii=False)

        except Exception as e:
            # swallow any unexpected error to prevent task failure/restart
            if DEBUG_PRINT:
                print("[RULE_ERROR]", str(e))
            return

# ------------------------------
# Main
# ------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    # CRITICAL: fail fast to see the real error (not endless restarting)
    env.set_restart_strategy(RestartStrategies.no_restart())

    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest"  # start at earliest for this new group
    }

    consumer = FlinkKafkaConsumer(
        topics=SRC_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    try:
        consumer.set_start_from_earliest()  # double-ensure earliest
    except Exception:
        pass

    stream = env.add_source(consumer)

    # Robust key selector (never crash on missing/invalid JSON)
    def key_selector(s: str) -> str:
        try:
            return (json.loads(s) or {}).get("card_id", "UNKNOWN")
        except Exception:
            return "UNKNOWN"

    keyed = stream.key_by(key_selector)

    processed = keyed.process(RuleEngine(), output_type=Types.STRING())

    if DEBUG_PRINT:
        processed.print()  # TaskManager logs

    producer = FlinkKafkaProducer(
        topic=SINK_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "transaction.timeout.ms": "900000"
        }
    )
    processed.add_sink(producer)

    env.execute("Fraud Detection (rules hardened)")

if __name__ == "__main__":
    main()
