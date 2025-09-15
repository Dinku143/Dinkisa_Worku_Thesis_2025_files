#!/usr/bin/env python3
"""
Accuracy Evaluation Script (MongoDB Direct)
Thesis: “NoSQL Databases in Stream Data Processing:
        Use-Case Anomaly Detection in Payment Card Transactions”

Evaluates Flink’s anomaly detection output stored in MongoDB
against simulator ground truth.
"""

import pandas as pd
from pymongo import MongoClient
from sklearn.metrics import confusion_matrix, precision_score, recall_score, f1_score

# ------------------------------------------------------------------ #
# (1) Connect to MongoDB and load documents                          #
# ------------------------------------------------------------------ #
client = MongoClient("mongodb://localhost:27017/")
db = client["fraud_detection"]
collection = db["processed_transactions"]

docs = list(collection.find({}, {
    "_id": 0,
    "transaction_id": 1,
    "card_id": 1,
    "user_id": 1,
    "transaction_value": 1,
    "spending_limit": 1,
    "ground_truth_anomaly": 1,
    "anomaly": 1,
    "anomaly_type": 1
}))

df = pd.DataFrame(docs)

if df.empty:
    print("⚠️ No data found in MongoDB collection 'processed_transactions'.")
    exit(1)

# ------------------------------------------------------------------ #
# (2) Convert to labels                                              #
# ------------------------------------------------------------------ #
y_true = df["ground_truth_anomaly"].notnull().astype(int)  # 1 if anomaly injected
y_pred = df["anomaly"].astype(int)                        # 1 if anomaly flagged

# ------------------------------------------------------------------ #
# (3) Confusion matrix and overall metrics                           #
# ------------------------------------------------------------------ #
tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

precision = precision_score(y_true, y_pred, zero_division=0)
recall = recall_score(y_true, y_pred, zero_division=0)  # same as TPR
f1 = f1_score(y_true, y_pred, zero_division=0)
fpr = fp / (fp + tn) if (fp + tn) > 0 else 0
fnr = fn / (tp + fn) if (tp + fn) > 0 else 0

print("=== Overall Metrics ===")
print(f"TP: {tp}, FP: {fp}, TN: {tn}, FN: {fn}")
print(f"Precision: {precision:.4f}")
print(f"Recall (TPR): {recall:.4f}")
print(f"F1-score: {f1:.4f}")
print(f"False Positive Rate (FPR): {fpr:.4f}")
print(f"False Negative Rate (FNR): {fnr:.4f}\n")

# ------------------------------------------------------------------ #
# (4) Metrics per anomaly type                                       #
# ------------------------------------------------------------------ #
print("=== Metrics per Anomaly Type ===")
for anomaly_type in ["high_value", "frequent_transaction", "location_change"]:
    subset = df[df["ground_truth_anomaly"] == anomaly_type]
    if len(subset) == 0:
        continue
    recall_type = recall_score(
        subset["ground_truth_anomaly"].notnull().astype(int),
        subset["anomaly"].astype(int),
        zero_division=0
    )
    detected = subset[subset["anomaly"] == 1]
    print(f"{anomaly_type}: {len(subset)} cases → Recall={recall_type:.4f}, Detected={len(detected)}")

print("\n✅ Evaluation complete (MongoDB direct)")
