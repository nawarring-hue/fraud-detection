"""
processor.py — Pipeline Spark + Kafka (optionnel, Dev avancé)
Lit depuis Kafka 'transactions', score via ML + règles, écrit dans Redis.
Compatible Docker et local Linux/Mac.
"""

import os
import sys
import json
import redis
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from config.settings import REDIS_HOST, REDIS_PORT, FRAUD_SCORE_THRESHOLD

# ── Spark ──────────────────────────────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("DetectionFraude")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ── Modèle ─────────────────────────────────────────────────────────────────────
model       = joblib.load(os.path.join(ROOT, "fraud_model.joblib"))
le_type     = joblib.load(os.path.join(ROOT, "le_type.joblib"))
le_category = joblib.load(os.path.join(ROOT, "le_category.joblib"))
le_currency = joblib.load(os.path.join(ROOT, "le_currency.joblib"))


def predict_score(amount, lat, lon, currency, t_type, category):
    try:
        df = pd.DataFrame([{
            "amount":       amount,
            "latitude":     lat,
            "longitude":    lon,
            "currency_enc": le_currency.transform([currency])[0],
            "type_enc":     le_type.transform([t_type])[0],
            "category_enc": le_category.transform([category])[0],
        }])
        return float(model.predict_proba(df)[0][1])
    except Exception:
        return 0.0


predict_udf = F.udf(predict_score, DoubleType())

# ── Schéma ─────────────────────────────────────────────────────────────────────
schema = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("currency",          StringType(),  True),
    StructField("timestamp",         TimestampType(), True),
    StructField("latitude",          DoubleType(),  True),
    StructField("longitude",         DoubleType(),  True),
    StructField("ip_address",        StringType(),  True),
    StructField("device_fingerprint", StringType(), True),
    StructField("merchant_category", StringType(),  True),
    StructField("transaction_type",  StringType(),  True),
    StructField("country",           StringType(),  True),
])

# ── Lecture Kafka ──────────────────────────────────────────────────────────────
kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

df_kafka = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_host)
            .option("subscribe", "transactions")
            .load())

json_df = (df_kafka
           .selectExpr("CAST(value AS STRING)")
           .select(F.from_json(F.col("value"), schema).alias("d"))
           .select("d.*"))

# ── Règles métier ──────────────────────────────────────────────────────────────
df_scored = (json_df
    .withColumn("hour", F.hour(F.col("timestamp")))
    .withColumn("alertes", F.array(
        F.when((F.col("amount") > 10000) | ((F.col("hour") >= 2) & (F.col("hour") <= 5)), "F1"),
        F.when((F.col("transaction_type") == "TRANSFER") & (F.col("amount") > 5000), "F2"),
        F.when(F.col("ip_address").startswith("127.") | F.col("device_fingerprint").isNull(), "F3"),
        F.when((F.col("transaction_type") == "PAYMENT") & (F.col("merchant_category") == "travel"), "F4"),
        F.when((F.col("transaction_type") == "CASH_OUT") & (F.col("amount") > 2000), "F5"),
        F.when((F.col("amount") >= 900) & (F.col("amount") < 1000), "F6"),
        F.when((F.col("transaction_type") == "TRANSFER") & (F.col("amount") > 3000), "F7"),
    ))
    .withColumn("alertes", F.array_remove(F.col("alertes"), None))
    .withColumn("fraud_score", predict_udf(
        F.col("amount"), F.col("latitude"), F.col("longitude"),
        F.col("currency"), F.col("transaction_type"), F.col("merchant_category")
    ))
    .withColumn("is_fraud", F.when(
        (F.size(F.col("alertes")) > 0) | (F.col("fraud_score") > FRAUD_SCORE_THRESHOLD), 1
    ).otherwise(0))
)


# ── Écriture Redis ─────────────────────────────────────────────────────────────
def send_to_redis(df, batch_id):
    rc = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pipe = rc.pipeline()
    for row in df.collect():
        tx = {
            "transaction_id":    row["transaction_id"],
            "user_id":           row["user_id"],
            "amount":            row["amount"],
            "currency":          row["currency"],
            "country":           row["country"],
            "transaction_type":  row["transaction_type"],
            "merchant_category": row["merchant_category"],
            "fraud_score":       round(row["fraud_score"], 4),
            "fraud_type":        row["alertes"][0] if row["alertes"] else None,
            "is_fraud":          bool(row["is_fraud"]),
            "timestamp":         str(row["timestamp"]),
            "latency_ms":        0,
        }
        pipe.lpush("transactions:live", json.dumps(tx))
        pipe.ltrim("transactions:live", 0, 199)

        if tx["is_fraud"]:
            pipe.lpush("fraud:alerts", json.dumps({
                "alert_id":   row["transaction_id"],
                "user_id":    tx["user_id"],
                "amount":     tx["amount"],
                "currency":   tx["currency"],
                "fraud_type": tx["fraud_type"] or "ML_DETECTED",
                "fraud_score": tx["fraud_score"],
                "country":    tx["country"],
                "timestamp":  tx["timestamp"],
                "latency_ms": 0,
            }))
            pipe.ltrim("fraud:alerts", 0, 499)
            pipe.hincrby("pipeline:stats", "total_frauds", 1)

        pipe.hincrby("pipeline:stats", "total_transactions", 1)
    pipe.execute()


query = (df_scored
         .select("transaction_id", "user_id", "amount", "currency",
                 "country", "transaction_type", "merchant_category",
                 "alertes", "fraud_score", "is_fraud", "timestamp")
         .writeStream
         .foreachBatch(send_to_redis)
         .start())

print("🚀 Pipeline Spark actif — données envoyées vers Redis.")
query.awaitTermination()
