"""
Simulateur de transactions bancaires avec scoring ML réel.
- Génère des transactions normales et frauduleuses
- Charge le modèle XGBoost + encodeurs depuis la racine du projet
- Pousse dans Redis (transactions:live + fraud:alerts + pipeline:stats)
"""

import redis
import json
import random
import time
import uuid
import os
import sys
from datetime import datetime
from faker import Faker

# ── Chemin racine du projet pour charger les modèles ──────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import joblib
import pandas as pd

from config.settings import (
    REDIS_HOST, REDIS_PORT,
    TRANSACTION_TYPES, MERCHANT_CATEGORIES, CURRENCIES,
    COUNTRIES, DEVICES,
    FRAUD_SCORE_THRESHOLD
)

fake = Faker()

# ── Connexion Redis ────────────────────────────────────────────────────────────
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# ── Chargement modèle ML ───────────────────────────────────────────────────────
print("🔄 Chargement du modèle ML...")
try:
    model       = joblib.load(os.path.join(ROOT, "fraud_model.joblib"))
    le_type     = joblib.load(os.path.join(ROOT, "le_type.joblib"))
    le_category = joblib.load(os.path.join(ROOT, "le_category.joblib"))
    le_currency = joblib.load(os.path.join(ROOT, "le_currency.joblib"))
    print("✅ Modèle ML chargé avec succès")
    ML_AVAILABLE = True
except Exception as e:
    print(f"⚠️  Modèle ML non disponible ({e}) — scoring par règles uniquement")
    ML_AVAILABLE = False


def ml_score(amount, latitude, longitude, currency, transaction_type, merchant_category):
    """Retourne le score de fraude du modèle (0-1)."""
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


# ── Règles métier → type de fraude ────────────────────────────────────────────
def detect_fraud_type(tx):
    """Applique les règles métier et renvoie le code fraude ou None."""
    hour = datetime.fromisoformat(tx["timestamp"]).hour
    amount   = tx["amount"]
    t_type   = tx["transaction_type"]
    category = tx["merchant_category"]
    device   = tx["device_fingerprint"]
    ip       = tx["ip_address"]

    if amount > 10_000 or (2 <= hour <= 5):
        return "F1"
    if t_type == "TRANSFER" and amount > 5_000:
        return "F2"
    if ip.startswith("127.") or device == "Unknown_Device":
        return "F3"
    if t_type == "PAYMENT" and category == "travel":
        return "F4"
    if t_type == "CASH_OUT" and amount > 2_000:
        return "F5"
    if 900 <= amount < 1_000:
        return "F6"
    if t_type == "TRANSFER" and amount > 3_000:
        return "F7"
    return None


# ── Génération de transactions ─────────────────────────────────────────────────
def generate_normal_transaction(user_id):
    currency = random.choice(CURRENCIES)
    t_type   = random.choice(TRANSACTION_TYPES)
    category = random.choice(MERCHANT_CATEGORIES)
    amount   = round(random.uniform(5, 400), 2)
    lat      = round(random.uniform(30, 50), 4)
    lon      = round(random.uniform(-5, 20), 4)
    ts       = datetime.now().isoformat()

    score = ml_score(amount, lat, lon, currency, t_type, category)

    return {
        "transaction_id":    str(uuid.uuid4()),
        "user_id":           user_id,
        "amount":            amount,
        "currency":          currency,
        "timestamp":         ts,
        "country":           random.choice(COUNTRIES),
        "merchant_category": category,
        "transaction_type":  t_type,
        "ip_address":        fake.ipv4(),
        "device_fingerprint": random.choice(DEVICES[:-1]),   # jamais Unknown ici
        "latitude":          lat,
        "longitude":         lon,
        "is_fraud":          False,
        "fraud_type":        None,
        "fraud_score":       round(score, 4),
        "latency_ms":        round(random.uniform(50, 300), 1),
    }


def generate_fraud_transaction(user_id):
    """Génère une transaction avec caractéristiques frauduleuses."""
    fraud_type = random.choice(["F1", "F2", "F3", "F4", "F5", "F6", "F7"])
    currency   = random.choice(CURRENCIES)
    t_type     = random.choice(TRANSACTION_TYPES)
    category   = random.choice(MERCHANT_CATEGORIES)
    lat        = round(random.uniform(30, 50), 4)
    lon        = round(random.uniform(-5, 20), 4)
    ts         = datetime.now().isoformat()

    # Montant frauduleux selon le type
    if fraud_type == "F1":
        amount = round(random.uniform(8_000, 15_000), 2)
        country = random.choice(["CN", "BR", "US"])
    elif fraud_type == "F2":
        amount, t_type = round(random.uniform(5_001, 12_000), 2), "TRANSFER"
        country = random.choice(COUNTRIES)
    elif fraud_type == "F3":
        amount  = round(random.uniform(100, 800), 2)
        country = random.choice(COUNTRIES)
    elif fraud_type == "F5":
        amount, t_type = round(random.uniform(2_001, 5_000), 2), "CASH_OUT"
        country = random.choice(COUNTRIES)
    elif fraud_type == "F6":
        amount  = round(random.uniform(900, 999), 2)
        country = random.choice(COUNTRIES)
    elif fraud_type == "F7":
        amount, t_type = round(random.uniform(3_001, 8_000), 2), "TRANSFER"
        country = random.choice(COUNTRIES)
    else:   # F4
        amount, category = round(random.uniform(100, 800), 2), "travel"
        country = random.choice(COUNTRIES)

    device = "Unknown_Device" if fraud_type == "F3" else random.choice(DEVICES)
    ip     = "127.0.0.1" if fraud_type == "F3" else fake.ipv4()

    score = ml_score(amount, lat, lon, currency, t_type, category)

    return {
        "transaction_id":    str(uuid.uuid4()),
        "user_id":           user_id,
        "amount":            amount,
        "currency":          currency,
        "timestamp":         ts,
        "country":           country,
        "merchant_category": category,
        "transaction_type":  t_type,
        "ip_address":        ip,
        "device_fingerprint": device,
        "latitude":          lat,
        "longitude":         lon,
        "is_fraud":          True,
        "fraud_type":        fraud_type,
        "fraud_score":       round(score, 4),
        "latency_ms":        round(random.uniform(50, 300), 1),
    }


# ── Push vers Redis ────────────────────────────────────────────────────────────
def push_transaction(tx):
    """Pousse la transaction dans Redis (transactions:live + alertes + stats)."""
    t_start = time.time()

    # Vérification règles métier (peut confirmer ou créer une alerte même sans ML)
    rule_type = detect_fraud_type(tx)
    if rule_type and not tx["fraud_type"]:
        tx["fraud_type"] = rule_type
        tx["is_fraud"]   = True

    pipe = r.pipeline()
    pipe.lpush("transactions:live", json.dumps(tx))
    pipe.ltrim("transactions:live", 0, 199)

    if tx["is_fraud"] or tx["fraud_score"] >= FRAUD_SCORE_THRESHOLD:
        tx["is_fraud"]   = True
        alert = {
            "alert_id":    str(uuid.uuid4()),
            "user_id":     tx["user_id"],
            "amount":      tx["amount"],
            "currency":    tx["currency"],
            "fraud_type":  tx["fraud_type"] or "ML_DETECTED",
            "fraud_score": tx["fraud_score"],
            "country":     tx["country"],
            "timestamp":   tx["timestamp"],
            "latency_ms":  round((time.time() - t_start) * 1000 + tx["latency_ms"], 1),
        }
        pipe.lpush("fraud:alerts", json.dumps(alert))
        pipe.ltrim("fraud:alerts", 0, 499)
        pipe.hincrby("pipeline:stats", "total_frauds", 1)

    pipe.hincrby("pipeline:stats", "total_transactions", 1)
    pipe.hset("pipeline:stats", "last_update", datetime.now().isoformat())
    pipe.execute()


# ── Boucle principale ──────────────────────────────────────────────────────────
def run_simulator():
    print("🚀 Simulateur démarré — 10 tx/s | 5% fraudes injectées")
    users = [f"USER_{i:04d}" for i in range(1, 101)]
    count = 0

    while True:
        user_id = random.choice(users)
        tx = (generate_fraud_transaction(user_id)
              if random.random() < 0.05
              else generate_normal_transaction(user_id))

        push_transaction(tx)
        count += 1

        if count % 100 == 0:
            print(f"📊 {count} transactions envoyées | dernière fraude score: {tx['fraud_score']:.3f}")

        time.sleep(0.1)   # 10 tx/s


if __name__ == "__main__":
    run_simulator()
