<<<<<<< HEAD
# 🛡️ Détection de Fraudes Bancaires — Temps Réel

Pipeline de détection de fraudes bancaires combinant règles métier et modèle ML (Random Forest).

---

## 🏗️ Architecture

```
Simulateur → Redis → Dashboard Streamlit
```

- **Simulateur** : génère 10 tx/s (5% frauduleuses), score via le modèle ML réel
- **Redis** : bus de données temps réel (transactions:live, fraud:alerts, pipeline:stats)
- **Dashboard** : visualisation live (KPIs, alertes filtrables, graphiques, export CSV)

---

## 🚀 Démarrage rapide

### Avec Docker (recommandé)

```bash
docker compose up --build
```

Puis ouvrir : http://localhost:8501

### En local (sans Docker)

**Prérequis** : Python 3.10+, Redis en cours d'exécution

```bash
# 1. Installer les dépendances
pip install -r requirements.txt

# 2. Charger les profils utilisateurs dans Redis
python -m data.initial_profiles

# 3. Lancer le simulateur (dans un terminal)
python -m simulator.data_simulator

# 4. Lancer le dashboard (dans un autre terminal)
streamlit run dashboard/app.py
```

---

## 📊 Flux Redis

| Clé               | Type  | Contenu                            |
|-------------------|-------|------------------------------------|
| `transactions:live` | List | 200 dernières transactions (JSON)  |
| `fraud:alerts`    | List  | 500 dernières alertes fraude (JSON)|
| `pipeline:stats`  | Hash  | total_transactions, total_frauds   |
| `user:profiles:*` | Hash  | Profil comportemental par user     |

---

## 🔍 Types de fraude détectés

| Code | Description         | Règle déclenchante                        |
|------|---------------------|-------------------------------------------|
| F1   | Fraude à la carte   | Montant > 10 000 ou heure 2h–5h           |
| F2   | Fraude par virement | TRANSFER > 5 000                          |
| F3   | Usurpation identité | IP 127.x ou device inconnu               |
| F4   | Fraude application  | PAYMENT + catégorie travel               |
| F5   | Fraude remboursement| CASH_OUT > 2 000                          |
| F6   | Blanchiment         | Montant entre 900 et 999                  |
| F7   | Paiement instantané | TRANSFER > 3 000                          |
| ML   | Détecté par modèle  | Score Random Forest > 0.7                 |

---

## 🧠 Modèle ML

- **Algorithme** : Random Forest (fichier `fraud_model.joblib`)
- **Features** : amount, latitude, longitude, currency, transaction_type, merchant_category
- **Sortie** : probabilité de fraude (0–1)
- **Seuil** : configurable dans `config/settings.py` (`FRAUD_SCORE_THRESHOLD = 0.7`)
=======
# fraud-detection
>>>>>>> 0ddab5b88a73b60b5e835714c66367e5f5e9b10c
