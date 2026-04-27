import redis
import json
import random
from datetime import datetime
from config.settings import (
    REDIS_HOST, REDIS_PORT,
    COUNTRIES, DEVICES, MERCHANT_CATEGORIES
)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def generate_profile(user_id):
    return {
        "user_id": user_id,
        "avg_amount": round(random.uniform(20, 500), 2),
        "std_amount": round(random.uniform(10, 100), 2),
        "usual_country": random.choice(COUNTRIES),
        "usual_device": random.choice(DEVICES),
        "usual_categories": json.dumps(random.sample(MERCHANT_CATEGORIES, 3)),
        "total_transactions": random.randint(10, 500),
        "account_age_days": random.randint(30, 1825),
        "refund_count_30d": random.randint(0, 2),
        "created_at": datetime.now().isoformat()
    }


def load_profiles(n=100):
    print(f"Chargement de {n} profils clients dans Redis...")
    pipe = r.pipeline()
    for i in range(1, n + 1):
        user_id = f"USER_{i:04d}"
        profile = generate_profile(user_id)
        pipe.hset(f"user:profiles:{user_id}", mapping=profile)
    pipe.execute()
    print(f"✅ {n} profils chargés !")

    r.hset("pipeline:stats", mapping={
        "total_transactions": 0,
        "total_frauds": 0,
        "pipeline_start": datetime.now().isoformat()
    })
    print("✅ Stats initialisées !")


if __name__ == "__main__":
    load_profiles(100)
