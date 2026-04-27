"""
Dashboard Streamlit — Détection de Fraudes en temps réel
Lit depuis Redis : transactions:live | fraud:alerts | pipeline:stats
"""

import streamlit as st
import redis
import json
import pandas as pd
import time
import os
import sys
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from config.settings import (
    REDIS_HOST, REDIS_PORT,
    FRAUD_TYPES, FRAUD_SCORE_THRESHOLD,
    MAX_ALERTS_DISPLAY, DASHBOARD_REFRESH_MS
)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Détection de Fraudes",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
        border-radius: 12px; padding: 20px; color: white; text-align: center;
        margin-bottom: 10px;
    }
    .metric-value { font-size: 2.2rem; font-weight: 700; }
    .metric-label { font-size: 0.85rem; opacity: 0.85; margin-top: 4px; }
    .fraud-badge {
        background: #e74c3c; color: white; padding: 2px 8px;
        border-radius: 10px; font-size: 0.75rem; font-weight: 600;
    }
    .safe-badge {
        background: #27ae60; color: white; padding: 2px 8px;
        border-radius: 10px; font-size: 0.75rem; font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ── Redis ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get_stats(r):
    raw = r.hgetall("pipeline:stats")
    return {
        "total_transactions": int(raw.get("total_transactions", 0)),
        "total_frauds":       int(raw.get("total_frauds", 0)),
        "last_update":        raw.get("last_update", "—"),
        "pipeline_start":     raw.get("pipeline_start", "—"),
    }

def get_alerts(r, limit=MAX_ALERTS_DISPLAY):
    items = r.lrange("fraud:alerts", 0, limit - 1)
    records = []
    for item in items:
        try:
            records.append(json.loads(item))
        except Exception:
            pass
    return pd.DataFrame(records) if records else pd.DataFrame()

def get_transactions(r, limit=50):
    items = r.lrange("transactions:live", 0, limit - 1)
    records = []
    for item in items:
        try:
            records.append(json.loads(item))
        except Exception:
            pass
    return pd.DataFrame(records) if records else pd.DataFrame()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/shield.png", width=60)
    st.title("🛡️ Fraude Monitor")
    st.markdown("---")
    selected_types = st.multiselect(
        "Filtrer par type de fraude",
        options=list(FRAUD_TYPES.keys()),
        format_func=lambda k: f"{k} — {FRAUD_TYPES[k]}",
        default=list(FRAUD_TYPES.keys()),
    )
    min_score = st.slider("Score minimum", 0.0, 1.0, float(FRAUD_SCORE_THRESHOLD), 0.01)
    auto_refresh = st.checkbox("Auto-refresh (2s)", value=True)
    st.markdown("---")
    st.caption("Redis: " + REDIS_HOST)
    st.caption(f"Seuil fraude: {FRAUD_SCORE_THRESHOLD}")

# ── Main ───────────────────────────────────────────────────────────────────────
st.title("🛡️ Dashboard Détection de Fraudes Bancaires")
st.caption(f"Mise à jour: {datetime.now().strftime('%H:%M:%S')}")

r = get_redis()

# Test connexion Redis
try:
    r.ping()
except Exception:
    st.error("❌ Redis non accessible. Vérifiez que Docker est lancé.")
    st.stop()

stats  = get_stats(r)
alerts = get_alerts(r)
txs    = get_transactions(r)

# ── KPIs ───────────────────────────────────────────────────────────────────────
total = stats["total_transactions"]
frauds = stats["total_frauds"]
fraud_rate = (frauds / total * 100) if total > 0 else 0.0
avg_score = alerts["fraud_score"].mean() if not alerts.empty and "fraud_score" in alerts else 0.0

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-value">{total:,}</div>
        <div class="metric-label">Transactions totales</div></div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-value" style="color:#ff6b6b">{frauds:,}</div>
        <div class="metric-label">Fraudes détectées</div></div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-value">{fraud_rate:.2f}%</div>
        <div class="metric-label">Taux de fraude</div></div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="metric-card">
        <div class="metric-value">{avg_score:.3f}</div>
        <div class="metric-label">Score moyen alertes</div></div>""", unsafe_allow_html=True)

st.markdown("---")

# ── Graphiques ─────────────────────────────────────────────────────────────────
col_left, col_right = st.columns([1, 2])

with col_left:
    st.subheader("📊 Répartition par type")
    if not alerts.empty and "fraud_type" in alerts:
        type_counts = (alerts["fraud_type"]
                       .value_counts()
                       .reset_index()
                       .rename(columns={"index": "type", "fraud_type": "count"}))
        type_counts.columns = ["type", "count"]
        type_counts["label"] = type_counts["type"].apply(
            lambda t: f"{t} — {FRAUD_TYPES.get(t, t)}" if pd.notna(t) else "ML_DETECTED"
        )
        fig_pie = px.pie(type_counts, values="count", names="label",
                         color_discrete_sequence=px.colors.sequential.RdBu)
        fig_pie.update_layout(margin=dict(t=10, b=10), height=300,
                              showlegend=True, paper_bgcolor="rgba(0,0,0,0)",
                              plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("En attente de données…")

with col_right:
    st.subheader("📈 Distribution des scores de fraude")
    if not alerts.empty and "fraud_score" in alerts:
        fig_hist = px.histogram(
            alerts, x="fraud_score", nbins=30,
            color_discrete_sequence=["#e74c3c"],
            labels={"fraud_score": "Score de fraude"}
        )
        fig_hist.add_vline(x=FRAUD_SCORE_THRESHOLD, line_dash="dash",
                           line_color="orange", annotation_text="Seuil")
        fig_hist.update_layout(height=300, margin=dict(t=10, b=10),
                               paper_bgcolor="rgba(0,0,0,0)",
                               plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_hist, use_container_width=True)
    else:
        st.info("En attente de données…")

st.markdown("---")

# ── Alertes récentes ───────────────────────────────────────────────────────────
st.subheader("🚨 Alertes de fraude récentes")

if not alerts.empty:
    filtered = alerts.copy()
    if selected_types:
        filtered = filtered[filtered["fraud_type"].isin(selected_types) |
                            filtered["fraud_type"].isna()]
    if "fraud_score" in filtered:
        filtered = filtered[filtered["fraud_score"] >= min_score]

    # Export CSV
    st.download_button(
        "⬇️ Exporter CSV",
        data=filtered.to_csv(index=False),
        file_name=f"alertes_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
    )

    if not filtered.empty:
        display_cols = [c for c in
            ["timestamp", "user_id", "fraud_type", "fraud_score", "amount", "currency", "country"]
            if c in filtered.columns]
        disp = filtered[display_cols].copy()
        if "fraud_score" in disp:
            disp["fraud_score"] = disp["fraud_score"].apply(lambda x: f"{x:.3f}")
        if "amount" in disp:
            disp["amount"] = disp["amount"].apply(lambda x: f"{x:,.2f}")
        st.dataframe(disp.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("Aucune alerte correspondant aux filtres.")
else:
    st.info("⏳ Aucune alerte pour l'instant. Le simulateur tourne-t-il ?")

st.markdown("---")

# ── Flux live ──────────────────────────────────────────────────────────────────
st.subheader("⚡ Flux de transactions en temps réel")

if not txs.empty:
    cols_tx = [c for c in
        ["timestamp", "user_id", "transaction_type", "merchant_category",
         "amount", "currency", "fraud_score", "is_fraud"]
        if c in txs.columns]
    disp_tx = txs[cols_tx].copy()
    if "fraud_score" in disp_tx:
        disp_tx["fraud_score"] = disp_tx["fraud_score"].apply(lambda x: f"{float(x):.3f}")
    if "amount" in disp_tx:
        disp_tx["amount"] = disp_tx["amount"].apply(lambda x: f"{float(x):,.2f}")
    st.dataframe(disp_tx.head(30), use_container_width=True, hide_index=True)
else:
    st.info("⏳ En attente de transactions…")

# ── Auto-refresh ───────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(2)
    st.rerun()
