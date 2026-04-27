"""
kafka/producer.py — Producteur Kafka de transactions bancaires
Génère des transactions (normales + frauduleuses), les score via le modèle ML
et les publie sur le topic Kafka 'transactions' à 10 tx/s.
Le processor.py (Spark) consomme ce topic et écrit les résultats dans Redis.
"""

import os
import sys
import json
import time
import uuid
import random
import logging
from datetime import datetime

from confluent_kafka import Producer
from faker import Faker

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import joblib
import pandas as pd

from config.settings import (
    TRANSACTION_TYPES, MERCHANT_CATEGORIES, CURRENCIES,
    COUNTRIES, DEVICES, FRAUD_SCORE_THRESHOLD
)

# ── Logging ────────────────────────────────────────────────────────────────────
os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(ROOT, "logs", "producer.log"), encoding="utf-8"),
    ],
)
logger = logging.getLogger("kafka-producer")

fake = Faker()

# ── Modèle ML ──────────────────────────────────────────────────────────────────
logger.info("🔄 Chargement du modèle ML...")
try:
    model       = joblib.load(os.path.join(ROOT, "fraud_model.joblib"))
    le_type     = joblib.load(os.path.join(ROOT, "le_type.joblib"))
    le_category = joblib.load(os.path.join(ROOT, "le_category.joblib"))
    le_currency = joblib.load(os.path.join(ROOT, "le_currency.joblib"))
    ML_AVAILABLE = True
    logger.info("✅ Modèle ML chargé")
except Exception as e:
    logger.warning(f"⚠️  Modèle non disponible ({e}) — score=0 par défaut")
    ML_AVAILABLE = False


def ml_score(amount, latitude, longitude, currency, transaction_type, merchant_category):
    """Retourne la probabilité de fraude du modèle Random Forest."""
    if not ML_AVAILABLE:
        return 0.0
    try:
        df = pd.DataFrame([{
            "amount":       amount,
            "latitude":     latitude,
            "longitude":    longitude,
            "currency_enc": le_currency.transform([currency])[0],
            "type_enc":     le_type.transform([transaction_type])[0],
            "category_enc": le_category.transform([merchant_category])[0],
        }])
        return float(model.predict_proba(df)[0][1])
    except Exception:
        return 0.0


# ── Génération de transactions ─────────────────────────────────────────────────
def generate_normal(user_id: str) -> dict:
    currency = random.choice(CURRENCIES)
    t_type   = random.choice(TRANSACTION_TYPES)
    category = random.choice(MERCHANT_CATEGORIES)
    amount   = round(random.uniform(5, 400), 2)
    lat      = round(random.uniform(30, 50), 4)
    lon      = round(random.uniform(-5, 20), 4)

    return {
        "transaction_id":    str(uuid.uuid4()),
        "user_id":           user_id,
        "amount":            amount,
        "currency":          currency,
        "timestamp":         datetime.utcnow().isoformat() + "Z",
        "country":           random.choice(COUNTRIES),
        "merchant_category": category,
        "transaction_type":  t_type,
        "ip_address":        fake.ipv4(),
        "device_fingerprint": random.choice(DEVICES[:-1]),
        "latitude":          lat,
        "longitude":         lon,
        "is_fraud":          False,
        "fraud_type":        None,
        "fraud_score":       round(ml_score(amount, lat, lon, currency, t_type, category), 4),
    }


def generate_fraud(user_id: str) -> dict:
    fraud_type = random.choice(["F1", "F2", "F3", "F4", "F5", "F6", "F7"])
    currency   = random.choice(CURRENCIES)
    category   = random.choice(MERCHANT_CATEGORIES)
    lat        = round(random.uniform(30, 50), 4)
    lon        = round(random.uniform(-5, 20), 4)

    if fraud_type == "F1":
        amount, t_type = round(random.uniform(8_000, 15_000), 2), random.choice(TRANSACTION_TYPES)
    elif fraud_type == "F2":
        amount, t_type = round(random.uniform(5_001, 12_000), 2), "TRANSFER"
    elif fraud_type == "F3":
        amount, t_type = round(random.uniform(100, 800), 2), random.choice(TRANSACTION_TYPES)
    elif fraud_type == "F4":
        amount, t_type, category = round(random.uniform(100, 800), 2), "PAYMENT", "travel"
    elif fraud_type == "F5":
        amount, t_type = round(random.uniform(2_001, 5_000), 2), "CASH_OUT"
    elif fraud_type == "F6":
        amount, t_type = round(random.uniform(900, 999), 2), random.choice(TRANSACTION_TYPES)
    else:  # F7
        amount, t_type = round(random.uniform(3_001, 8_000), 2), "TRANSFER"

    device = "Unknown_Device" if fraud_type == "F3" else random.choice(DEVICES)
    ip     = "127.0.0.1"     if fraud_type == "F3" else fake.ipv4()

    return {
        "transaction_id":    str(uuid.uuid4()),
        "user_id":           user_id,
        "amount":            amount,
        "currency":          currency,
        "timestamp":         datetime.utcnow().isoformat() + "Z",
        "country":           random.choice(COUNTRIES),
        "merchant_category": category,
        "transaction_type":  t_type,
        "ip_address":        ip,
        "device_fingerprint": device,
        "latitude":          lat,
        "longitude":         lon,
        "is_fraud":          True,
        "fraud_type":        fraud_type,
        "fraud_score":       round(ml_score(amount, lat, lon, currency, t_type, category), 4),
    }


# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC        = "transactions"

sent_ok  = 0
sent_err = 0

def delivery_report(err, msg):
    global sent_ok, sent_err
    if err is None:
        sent_ok += 1
        if sent_ok % 100 == 0:
            logger.info(f"📨 {sent_ok} messages livrés | erreurs: {sent_err}")
    else:
        sent_err += 1
        logger.error(f"❌ Erreur livraison: {err}")


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers":  KAFKA_BROKER,
        "client.id":          "fraud-detection-producer",
        "acks":               "1",
        "retries":            5,
        "retry.backoff.ms":   200,
        "linger.ms":          5,
        "batch.size":         16384,
        "compression.type":   "snappy",
    })


# ── Boucle principale ──────────────────────────────────────────────────────────
def run():
    logger.info("🚀 Producteur Kafka démarré")
    logger.info(f"   Broker  : {KAFKA_BROKER}")
    logger.info(f"   Topic   : {TOPIC}")
    logger.info(f"   Débit   : 10 tx/s | 5% fraudes")
    logger.info("-" * 60)

    producer = build_producer()
    users    = [f"USER_{i:04d}" for i in range(1, 101)]
    count    = 0

    try:
        while True:
            user_id = random.choice(users)
            tx = (generate_fraud(user_id)
                  if random.random() < 0.05
                  else generate_normal(user_id))

            payload = json.dumps(tx, ensure_ascii=False).encode("utf-8")
            producer.produce(
                TOPIC,
                key=tx["user_id"].encode("utf-8"),
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)
            count += 1

            tag = f"[FRAUDE {tx['fraud_type']}]" if tx["is_fraud"] else ""
            logger.debug(
                f"→ {tx['transaction_id'][:8]} | {tx['user_id']} | "
                f"{tx['amount']:>9.2f} {tx['currency']} | {tx['transaction_type']:10s} "
                f"| score={tx['fraud_score']:.3f} {tag}"
            )

            time.sleep(0.1)  # 10 tx/s

    except KeyboardInterrupt:
        logger.info("⛔ Arrêt demandé")
    finally:
        producer.flush()
        logger.info(f"\n📊 Résumé final — envoyés: {sent_ok} | erreurs: {sent_err}")


if __name__ == "__main__":
    run()
