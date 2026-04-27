import os

# --- Redis ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# --- Simulation ---
FRAUD_RATE = 0.05
TRANSACTIONS_PER_SECOND = 10

# --- Dashboard ---
DASHBOARD_REFRESH_MS = 500
MAX_ALERTS_DISPLAY = 100

# --- Seuil de fraude ---
FRAUD_SCORE_THRESHOLD = 0.7

# --- Types de fraude ---
FRAUD_TYPES = {
    "F1": "Fraude à la carte",
    "F2": "Fraude par virement",
    "F3": "Usurpation d'identité",
    "F4": "Fraude à l'application",
    "F5": "Fraude au remboursement",
    "F6": "Blanchiment d'argent",
    "F7": "Fraude paiement instantané"
}

# --- Clés Redis ---
REDIS_KEY_PROFILES     = "user:profiles:"
REDIS_KEY_ALERTS       = "fraud:alerts"
REDIS_KEY_STATS        = "pipeline:stats"
REDIS_KEY_TRANSACTIONS = "transactions:live"

# --- Données compatibles avec le modèle ML ---
TRANSACTION_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT"]

MERCHANT_CATEGORIES = [
    "entertainment", "food_dining", "gas_transport", "grocery_net",
    "grocery_pos", "health_fitness", "home", "kids_pets", "misc_net",
    "misc_pos", "personal_care", "shopping_net", "shopping_pos", "travel"
]

CURRENCIES = ["EUR", "MAD", "USD"]

COUNTRIES = ["MA", "FR", "ES", "DE", "IT", "US", "GB", "CN", "BR"]
DEVICES   = ["iPhone_14", "Samsung_S23", "MacBook_Pro", "Windows_PC", "iPad_Air", "Unknown_Device"]
