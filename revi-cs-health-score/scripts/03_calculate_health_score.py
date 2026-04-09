"""
03_calculate_health_score.py
-----------------------------
Loads dim_clients and all raw ReviCX tables, computes the Health Score for
every client according to scoring_rules.yaml, and writes the results to
fct_health_score.
"""

from pathlib import Path
from datetime import date

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
DB_PATH = ROOT / "data" / "revi_cs.db"
RULES_PATH = ROOT / "config" / "scoring_rules.yaml"

REFERENCE_DATE = date(2026, 4, 8)
CURRENT_MONTH = "2026-04"
PREVIOUS_MONTH = "2026-03"
CALCULATED_AT = "2026-04-08"

# ---------------------------------------------------------------------------
# Load config
# ---------------------------------------------------------------------------
with open(RULES_PATH, "r", encoding="utf-8") as fh:
    rules = yaml.safe_load(fh)

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
engine = create_engine(f"sqlite:///{DB_PATH}")

# ---------------------------------------------------------------------------
# Extract all tables we need
# ---------------------------------------------------------------------------
with engine.connect() as conn:
    dim_clients = pd.read_sql(text("SELECT * FROM dim_clients"), conn)

    campaigns = pd.read_sql(
        text("SELECT revi_client_id, sent_at, revenue_attributed, cost FROM raw_revi_campaigns"),
        conn,
    )

    automations = pd.read_sql(
        text("SELECT revi_client_id, automation_type, status FROM raw_revi_automations"),
        conn,
    )

    chat = pd.read_sql(
        text("SELECT revi_client_id, has_human_agent, has_chat_flow, has_ai_enabled FROM raw_revi_chat"),
        conn,
    )

    messages = pd.read_sql(
        text(
            """
            SELECT revi_client_id, year_month, messages_sent, plan_limit
            FROM raw_revi_messages_monthly
            WHERE year_month IN (:cur, :prev)
            """
        ),
        conn,
        params={"cur": CURRENT_MONTH, "prev": PREVIOUS_MONTH},
    )

# ---------------------------------------------------------------------------
# Pre-aggregate: campaigns
# ---------------------------------------------------------------------------
campaigns["sent_at"] = pd.to_datetime(campaigns["sent_at"])

# Last campaign date per client
last_campaign = (
    campaigns.groupby("revi_client_id")["sent_at"]
    .max()
    .reset_index()
    .rename(columns={"sent_at": "last_campaign_at"})
)

# Weighted-average ROI from the last 3 campaigns per client
def compute_roi(grp: pd.DataFrame) -> float:
    last3 = grp.nlargest(3, "sent_at")
    total_cost = last3["cost"].sum()
    if total_cost == 0:
        return 0.0
    return last3["revenue_attributed"].sum() / total_cost

campaign_roi_df = (
    campaigns.groupby("revi_client_id")
    .apply(compute_roi)
    .reset_index()
    .rename(columns={0: "campaign_roi"})
)

# ---------------------------------------------------------------------------
# Pre-aggregate: automations
# ---------------------------------------------------------------------------
active_std = (
    automations[
        (automations["status"] == "active") & (automations["automation_type"] == "standard")
    ]
    .groupby("revi_client_id")
    .size()
    .reset_index(name="active_automations")
)

active_int = (
    automations[
        (automations["status"] == "active") & (automations["automation_type"] == "integration")
    ]
    .groupby("revi_client_id")
    .size()
    .reset_index(name="integration_automations")
)

# ---------------------------------------------------------------------------
# Pre-aggregate: messages
# ---------------------------------------------------------------------------
msg_current = messages[messages["year_month"] == CURRENT_MONTH][
    ["revi_client_id", "messages_sent", "plan_limit"]
].rename(columns={"messages_sent": "msg_current", "plan_limit": "plan_limit"})

msg_previous = messages[messages["year_month"] == PREVIOUS_MONTH][
    ["revi_client_id", "messages_sent"]
].rename(columns={"messages_sent": "msg_previous"})

# ---------------------------------------------------------------------------
# Build master dataframe
# ---------------------------------------------------------------------------
df = dim_clients[["client_id", "cashback_enabled"]].copy()
df = df.rename(columns={"client_id": "revi_client_id"})

# Merge campaigns
df = df.merge(last_campaign, on="revi_client_id", how="left")
df = df.merge(campaign_roi_df, on="revi_client_id", how="left")

# Merge automations
df = df.merge(active_std, on="revi_client_id", how="left")
df = df.merge(active_int, on="revi_client_id", how="left")

# Merge chat
df = df.merge(chat, on="revi_client_id", how="left")

# Merge messages
df = df.merge(msg_current, on="revi_client_id", how="left")
df = df.merge(msg_previous, on="revi_client_id", how="left")

# ---------------------------------------------------------------------------
# Derive intermediate metrics
# ---------------------------------------------------------------------------

# days_since_last_campaign
ref_ts = pd.Timestamp(REFERENCE_DATE)
df["days_since_last_campaign"] = df["last_campaign_at"].apply(
    lambda ts: (ref_ts - ts).days if pd.notna(ts) else 999
)

# campaign_roi — fill missing with 0
df["campaign_roi"] = df["campaign_roi"].fillna(0.0)

# automations counts — fill missing with 0
df["active_automations"] = df["active_automations"].fillna(0).astype(int)
df["integration_automations"] = df["integration_automations"].fillna(0).astype(int)

# chat booleans — fill missing with 0/False
for col in ("has_human_agent", "has_chat_flow", "has_ai_enabled"):
    df[col] = df[col].fillna(0).astype(bool)

# chat_usage_level
def chat_level(row):
    if row["has_human_agent"] and (row["has_chat_flow"] or row["has_ai_enabled"]):
        return "advanced"
    if row["has_human_agent"] or row["has_chat_flow"]:
        return "essential"
    return "none"

df["chat_usage_level"] = df.apply(chat_level, axis=1)

# messages metrics
df["msg_current"] = df["msg_current"].fillna(0).astype(float)
df["msg_previous"] = df["msg_previous"].fillna(0).astype(float)
df["plan_limit"] = df["plan_limit"].fillna(0).astype(float)

df["messages_mom_change"] = df.apply(
    lambda r: ((r["msg_current"] - r["msg_previous"]) / r["msg_previous"]) * 100
    if r["msg_previous"] > 0
    else 0.0,
    axis=1,
)

df["plan_usage_pct"] = df.apply(
    lambda r: (r["msg_current"] / r["plan_limit"]) * 100 if r["plan_limit"] > 0 else 0.0,
    axis=1,
)

# cashback
df["cashback_enabled"] = df["cashback_enabled"].fillna(0).astype(bool)

# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------

def score_recency(days: int) -> int:
    if days <= 7:
        return 5
    if days <= 20:
        return 3
    return 0


def score_roi(roi: float) -> int:
    if roi >= 10:
        return 5
    if roi >= 1:
        return 3
    return 0


def score_automations(count: int) -> int:
    if count >= 3:
        return 5
    if count >= 1:
        return 3
    return 0


def score_integrations(count: int) -> int:
    if count >= 4:
        return 5
    if count == 3:
        return 4
    if count >= 1:
        return 2
    return 0


def score_chat(level: str) -> int:
    if level == "advanced":
        return 5
    if level == "essential":
        return 4
    return 0


def score_volume(mom_change: float, plan_usage: float) -> int:
    # Growing: mom_change > 10 OR plan_usage >= 80
    if mom_change > 10 or plan_usage >= 80:
        return 5
    # Stable: -10 <= mom_change <= 10
    if -10 <= mom_change <= 10:
        return 3
    # Declining or irrelevant: mom_change < -10 OR plan_usage < 20
    return 0


# ---------------------------------------------------------------------------
# Apply scoring
# ---------------------------------------------------------------------------
df["score_recency"] = df["days_since_last_campaign"].apply(score_recency)
df["score_roi"] = df["campaign_roi"].apply(score_roi)
df["score_automations"] = df["active_automations"].apply(score_automations)
df["score_integrations"] = df["integration_automations"].apply(score_integrations)
df["score_chat"] = df["chat_usage_level"].apply(score_chat)
df["score_volume"] = df.apply(
    lambda r: score_volume(r["messages_mom_change"], r["plan_usage_pct"]), axis=1
)
df["bonus_cashback"] = 0  # reserved for future rules

df["total_score"] = (
    df["score_recency"]
    + df["score_roi"]
    + df["score_automations"]
    + df["score_integrations"]
    + df["score_chat"]
    + df["score_volume"]
    + df["bonus_cashback"]
)

# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------
classification = rules["classification"]
green_min = classification["green"]["min_score"]   # 26
yellow_min = classification["yellow"]["min_score"] # 16
yellow_max = classification["yellow"]["max_score"] # 25


def classify(score: int) -> str:
    if score >= green_min:
        return "green"
    if yellow_min <= score <= yellow_max:
        return "yellow"
    return "red"


df["health_status"] = df["total_score"].apply(classify)

# ---------------------------------------------------------------------------
# Build fct_health_score rows
# ---------------------------------------------------------------------------
fct = pd.DataFrame({
    "client_id":                df["revi_client_id"],
    "calculated_at":            CALCULATED_AT,
    "score_recency":            df["score_recency"],
    "score_roi":                df["score_roi"],
    "score_automations":        df["score_automations"],
    "score_integrations":       df["score_integrations"],
    "score_chat":               df["score_chat"],
    "score_volume":             df["score_volume"],
    "bonus_cashback":           df["bonus_cashback"],
    "days_since_last_campaign": df["days_since_last_campaign"],
    "campaign_roi":             df["campaign_roi"],
    "active_automations":       df["active_automations"],
    "integration_automations":  df["integration_automations"],
    "chat_usage_level":         df["chat_usage_level"],
    "messages_sent_current":    df["msg_current"].astype(int),
    "messages_sent_previous":   df["msg_previous"].astype(int),
    "messages_mom_change":      df["messages_mom_change"].round(2),
    "plan_usage_pct":           df["plan_usage_pct"].round(2),
    "total_score":              df["total_score"],
    "health_status":            df["health_status"],
})

# ---------------------------------------------------------------------------
# Write to DB — INSERT OR REPLACE via delete + insert to honour UNIQUE constraint
# ---------------------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(
        text(
            "DELETE FROM fct_health_score WHERE calculated_at = :calc_at"
        ),
        {"calc_at": CALCULATED_AT},
    )
    conn.commit()

fct.to_sql("fct_health_score", engine, if_exists="append", index=False)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
n_total = len(fct)
n_green = (fct["health_status"] == "green").sum()
n_yellow = (fct["health_status"] == "yellow").sum()
n_red = (fct["health_status"] == "red").sum()
avg_score = fct["total_score"].mean()

print(f"Health Score calculado para {n_total} clientes")
print(f"Verdes: {n_green} | Amarelos: {n_yellow} | Vermelhos: {n_red}")
print(f"Score medio: {avg_score:.1f}")
