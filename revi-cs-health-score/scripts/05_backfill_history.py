"""
05_backfill_history.py
----------------------
Simulates 6 months of historical Health Score data (2025-10 through 2026-03).

For each month end-date the script:
  1. Computes all health-score metrics using only data available up to that date.
  2. Applies the same scoring rules as the live pipeline.
  3. Inserts into fct_health_score with calculated_at = last day of the month.
  4. Populates fct_csm_activity_weekly from raw_hubspot_calls.
  5. Populates fct_coverage_monthly per CSM/month.
  6. Populates fct_revenue_retention_monthly from raw_hubspot_deals.

random.seed(42) is used for any stochastic choices.
"""

import random
from calendar import monthrange
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
random.seed(42)

DB_PATH    = Path(__file__).parent.parent / "data" / "revi_cs.db"
RULES_PATH = Path(__file__).parent.parent / "config" / "scoring_rules.yaml"

engine = create_engine(f"sqlite:///{DB_PATH}")

# Months to backfill: (year, month) pairs
MONTHS = [
    (2025, 10),
    (2025, 11),
    (2025, 12),
    (2026,  1),
    (2026,  2),
    (2026,  3),
]

def month_end(year: int, month: int) -> date:
    """Return the last calendar day of the given month."""
    return date(year, month, monthrange(year, month)[1])

def ym_str(year: int, month: int) -> str:
    return f"{year}-{month:02d}"

def prev_month(year: int, month: int):
    if month == 1:
        return year - 1, 12
    return year, month - 1

# ---------------------------------------------------------------------------
# Load scoring rules
# ---------------------------------------------------------------------------
with open(RULES_PATH, encoding="utf-8") as fh:
    cfg = yaml.safe_load(fh)

def score_recency(days):
    if days is None:
        return 0
    if days <= 7:
        return 5
    if days <= 20:
        return 3
    return 0

def score_roi(roi):
    if roi is None:
        return 0
    if roi >= 10:
        return 5
    if roi >= 1:
        return 3
    return 0

def score_automations(n):
    if n is None:
        n = 0
    if n >= 3:
        return 5
    if n >= 1:
        return 3
    return 0

def score_integrations(n):
    if n is None:
        n = 0
    if n >= 4:
        return 5
    if n == 3:
        return 4
    if n >= 1:
        return 2
    return 0

def score_chat(level):
    """
    level: 'advanced', 'essential', or 'none'
    Matches the chat_usage_level stored in fct_health_score.
    """
    if level == "advanced":
        return 5
    if level == "essential":
        return 4
    return 0

def score_volume(mom_change, plan_usage_pct):
    if plan_usage_pct is not None and plan_usage_pct >= 80:
        return 5
    if mom_change is not None:
        if mom_change > 0:
            return 5
        if mom_change >= -10:
            return 3
    return 0

def health_status_from_score(total):
    if total >= 26:
        return "green"
    if total >= 16:
        return "yellow"
    return "red"

# ---------------------------------------------------------------------------
# Load static reference data (not month-dependent)
# ---------------------------------------------------------------------------
with engine.connect() as conn:
    df_clients = pd.read_sql(text("SELECT * FROM dim_clients"), conn)
    df_chat    = pd.read_sql(text("SELECT * FROM raw_revi_chat"), conn)
    df_config  = pd.read_sql(text("SELECT * FROM raw_revi_client_config"), conn)
    df_deals   = pd.read_sql(text("SELECT * FROM raw_hubspot_deals"), conn)
    df_calls   = pd.read_sql(text("SELECT * FROM raw_hubspot_calls"), conn)
    df_campaigns_all = pd.read_sql(text("SELECT * FROM raw_revi_campaigns"), conn)
    df_automations   = pd.read_sql(text("SELECT * FROM raw_revi_automations"), conn)
    df_messages_all  = pd.read_sql(text("SELECT * FROM raw_revi_messages_monthly"), conn)

# Pre-parse datetimes
df_campaigns_all["sent_at"] = pd.to_datetime(df_campaigns_all["sent_at"], errors="coerce")
df_automations["created_at"] = pd.to_datetime(df_automations["created_at"], errors="coerce")
df_calls["call_date"] = pd.to_datetime(df_calls["call_date"], errors="coerce")
df_deals["closed_date"] = pd.to_datetime(df_deals["closed_date"], errors="coerce")

# Build chat_usage_level lookup (static)
def derive_chat_level(row):
    has_human  = bool(row.get("has_human_agent", 0))
    has_flow   = bool(row.get("has_chat_flow", 0))
    has_ai     = bool(row.get("has_ai_enabled", 0))
    if has_human and (has_flow or has_ai):
        return "advanced"
    if has_human or has_flow:
        return "essential"
    return "none"

chat_level_map = {
    row["revi_client_id"]: derive_chat_level(row)
    for _, row in df_chat.iterrows()
}

cashback_map = {
    row["revi_client_id"]: bool(row.get("cashback_enabled", 0))
    for _, row in df_config.iterrows()
}

# ---------------------------------------------------------------------------
# 1–3. Backfill fct_health_score
# ---------------------------------------------------------------------------
score_records = []

for year, month in MONTHS:
    end_date     = month_end(year, month)
    end_dt       = pd.Timestamp(end_date)
    ym           = ym_str(year, month)
    py, pm       = prev_month(year, month)
    prev_ym      = ym_str(py, pm)
    # Window for ROI: this month + 2 prior months
    roi_start_y, roi_start_m = prev_month(py, pm)
    roi_window_start = date(roi_start_y, roi_start_m, 1)

    for _, client in df_clients.iterrows():
        cid = client["client_id"]

        # --- days_since_last_campaign ---
        camp_slice = df_campaigns_all[
            (df_campaigns_all["revi_client_id"] == cid)
            & (df_campaigns_all["sent_at"] <= end_dt)
        ]
        if camp_slice.empty:
            days_since = 999
            campaign_roi_val = None
        else:
            last_sent = camp_slice["sent_at"].max()
            days_since = (end_date - last_sent.date()).days

            # --- campaign_roi (3-month window) ---
            roi_slice = camp_slice[
                camp_slice["sent_at"] >= pd.Timestamp(roi_window_start)
            ]
            total_rev  = roi_slice["revenue_attributed"].sum()
            total_cost = roi_slice["cost"].sum()
            campaign_roi_val = (total_rev / total_cost) if total_cost > 0 else None

        # --- active_automations & integration_automations ---
        auto_slice = df_automations[
            (df_automations["revi_client_id"] == cid)
            & (df_automations["created_at"] <= end_dt)
            & (df_automations["status"] == "active")
        ]
        active_automations      = int((auto_slice["automation_type"] != "integration").sum())
        integration_automations = int((auto_slice["automation_type"] == "integration").sum())

        # --- chat_usage_level (static) ---
        chat_level = chat_level_map.get(cid, "none")

        # --- messages volume ---
        msg_cur_row = df_messages_all[
            (df_messages_all["revi_client_id"] == cid)
            & (df_messages_all["year_month"] == ym)
        ]
        msg_prev_row = df_messages_all[
            (df_messages_all["revi_client_id"] == cid)
            & (df_messages_all["year_month"] == prev_ym)
        ]

        if msg_cur_row.empty:
            msg_current  = None
            plan_limit   = None
            plan_usage   = None
        else:
            msg_current = int(msg_cur_row.iloc[0]["messages_sent"])
            plan_limit  = int(msg_cur_row.iloc[0]["plan_limit"])
            plan_usage  = (msg_current / plan_limit * 100) if plan_limit else None

        msg_previous = (
            int(msg_prev_row.iloc[0]["messages_sent"])
            if not msg_prev_row.empty else None
        )

        if msg_current is not None and msg_previous and msg_previous > 0:
            mom_change = ((msg_current - msg_previous) / msg_previous) * 100
        else:
            mom_change = None

        # --- Scores ---
        s_recency    = score_recency(days_since if days_since < 999 else None)
        s_roi        = score_roi(campaign_roi_val)
        s_automations = score_automations(active_automations)
        s_integrations = score_integrations(integration_automations)
        s_chat       = score_chat(chat_level)
        s_volume     = score_volume(mom_change, plan_usage)
        bonus_cashback = 5 if cashback_map.get(cid, False) else 0

        total_score  = s_recency + s_roi + s_automations + s_integrations + s_chat + s_volume + bonus_cashback
        h_status     = health_status_from_score(total_score)

        score_records.append({
            "client_id":               cid,
            "calculated_at":           str(end_date),
            "score_recency":           s_recency,
            "score_roi":               s_roi,
            "score_automations":       s_automations,
            "score_integrations":      s_integrations,
            "score_chat":              s_chat,
            "score_volume":            s_volume,
            "bonus_cashback":          bonus_cashback,
            "days_since_last_campaign": days_since if days_since < 999 else None,
            "campaign_roi":            campaign_roi_val,
            "active_automations":      active_automations,
            "integration_automations": integration_automations,
            "chat_usage_level":        chat_level,
            "messages_sent_current":   msg_current,
            "messages_sent_previous":  msg_previous,
            "messages_mom_change":     mom_change,
            "plan_usage_pct":          plan_usage,
            "total_score":             total_score,
            "health_status":           h_status,
        })

df_scores = pd.DataFrame(score_records)

# Insert, skipping duplicates via INSERT OR IGNORE
with engine.connect() as conn:
    for _, row in df_scores.iterrows():
        conn.execute(
            text("""
                INSERT OR IGNORE INTO fct_health_score (
                    client_id, calculated_at,
                    score_recency, score_roi, score_automations, score_integrations,
                    score_chat, score_volume, bonus_cashback,
                    days_since_last_campaign, campaign_roi,
                    active_automations, integration_automations,
                    chat_usage_level,
                    messages_sent_current, messages_sent_previous,
                    messages_mom_change, plan_usage_pct,
                    total_score, health_status
                ) VALUES (
                    :client_id, :calculated_at,
                    :score_recency, :score_roi, :score_automations, :score_integrations,
                    :score_chat, :score_volume, :bonus_cashback,
                    :days_since_last_campaign, :campaign_roi,
                    :active_automations, :integration_automations,
                    :chat_usage_level,
                    :messages_sent_current, :messages_sent_previous,
                    :messages_mom_change, :plan_usage_pct,
                    :total_score, :health_status
                )
            """),
            row.to_dict(),
        )
    conn.commit()

# ---------------------------------------------------------------------------
# 4. fct_csm_activity_weekly
# ---------------------------------------------------------------------------
# Keep only calls within the 6-month backfill window
backfill_start = date(2025, 10, 1)
backfill_end   = month_end(2026, 3)

df_calls_window = df_calls[
    (df_calls["call_date"].dt.date >= backfill_start)
    & (df_calls["call_date"].dt.date <= backfill_end)
].copy()

if not df_calls_window.empty:
    # Week start = Monday
    df_calls_window["week_start"] = (
        df_calls_window["call_date"]
        .dt.to_period("W-SUN")
        .dt.start_time.dt.date
    )

    # Merge to get company -> csm mapping
    company_csm = df_clients[["hubspot_company_id", "csm_owner"]].drop_duplicates()
    df_calls_window = df_calls_window.merge(
        company_csm, on="hubspot_company_id", how="left", suffixes=("_call", "")
    )
    # Use call's own csm_owner if available, fallback to dim_clients
    if "csm_owner_call" in df_calls_window.columns:
        df_calls_window["csm_owner"] = df_calls_window["csm_owner_call"].fillna(
            df_calls_window["csm_owner"]
        )

    activity_rows = []
    for (csm, week), grp in df_calls_window.groupby(["csm_owner", "week_start"]):
        activity_rows.append({
            "csm_owner":                csm,
            "week_start":               str(week),
            "calls_total":              len(grp),
            "calls_consultoria":        int((grp["call_type"] == "consultoria").sum()),
            "calls_onboarding":         int((grp["call_type"] == "onboarding").sum()),
            "calls_urgencia":           int((grp["call_type"] == "urgencia").sum()),
            "calls_follow_up":          int((grp["call_type"] == "follow_up").sum()),
            "unique_clients_contacted": grp["hubspot_company_id"].nunique(),
            "total_duration_minutes":   int(grp["duration_minutes"].sum()),
        })

    df_activity = pd.DataFrame(activity_rows)
    with engine.connect() as conn:
        for _, row in df_activity.iterrows():
            conn.execute(
                text("""
                    INSERT OR REPLACE INTO fct_csm_activity_weekly
                        (csm_owner, week_start,
                         calls_total, calls_consultoria, calls_onboarding,
                         calls_urgencia, calls_follow_up,
                         unique_clients_contacted, total_duration_minutes)
                    VALUES
                        (:csm_owner, :week_start,
                         :calls_total, :calls_consultoria, :calls_onboarding,
                         :calls_urgencia, :calls_follow_up,
                         :unique_clients_contacted, :total_duration_minutes)
                """),
                row.to_dict(),
            )
        conn.commit()

# ---------------------------------------------------------------------------
# 5. fct_coverage_monthly
# ---------------------------------------------------------------------------
csm_list = df_clients["csm_owner"].dropna().unique().tolist()

coverage_rows = []
for year, month in MONTHS:
    ym         = ym_str(year, month)
    month_start = date(year, month, 1)
    end_date    = month_end(year, month)

    # Calls made during this month
    calls_month = df_calls[
        (df_calls["call_date"].dt.date >= month_start)
        & (df_calls["call_date"].dt.date <= end_date)
    ]

    for csm in csm_list:
        # Clients assigned to this CSM
        csm_clients = df_clients[df_clients["csm_owner"] == csm]

        with_cs    = csm_clients[csm_clients["has_cs"] == 1]["hubspot_company_id"].tolist()
        without_cs = csm_clients[csm_clients["has_cs"] == 0]["hubspot_company_id"].tolist()

        # Contacted = had at least 1 call that month
        called_companies = set(
            calls_month[calls_month["csm_owner"] == csm]["hubspot_company_id"].dropna()
        )

        with_cs_contacted    = sum(1 for c in with_cs    if c in called_companies)
        without_cs_contacted = sum(1 for c in without_cs if c in called_companies)

        n_with    = len(with_cs)
        n_without = len(without_cs)

        coverage_with    = (with_cs_contacted    / n_with    * 100) if n_with    > 0 else 0.0
        coverage_without = (without_cs_contacted / n_without * 100) if n_without > 0 else 0.0

        # Deterministic random SQL count per CSM/month (seed already set globally)
        sqls = random.randint(0, 4)

        coverage_rows.append({
            "csm_owner":                  csm,
            "year_month":                 ym,
            "clients_with_cs_total":      n_with,
            "clients_with_cs_contacted":  with_cs_contacted,
            "clients_without_cs_total":   n_without,
            "clients_without_cs_contacted": without_cs_contacted,
            "coverage_with_cs_pct":       round(coverage_with, 2),
            "coverage_without_cs_pct":    round(coverage_without, 2),
            "sqls_identified":            sqls,
        })

df_coverage = pd.DataFrame(coverage_rows)
with engine.connect() as conn:
    for _, row in df_coverage.iterrows():
        conn.execute(
            text("""
                INSERT OR REPLACE INTO fct_coverage_monthly
                    (csm_owner, year_month,
                     clients_with_cs_total, clients_with_cs_contacted,
                     clients_without_cs_total, clients_without_cs_contacted,
                     coverage_with_cs_pct, coverage_without_cs_pct,
                     sqls_identified)
                VALUES
                    (:csm_owner, :year_month,
                     :clients_with_cs_total, :clients_with_cs_contacted,
                     :clients_without_cs_total, :clients_without_cs_contacted,
                     :coverage_with_cs_pct, :coverage_without_cs_pct,
                     :sqls_identified)
            """),
            row.to_dict(),
        )
    conn.commit()

# ---------------------------------------------------------------------------
# 6. fct_revenue_retention_monthly
# ---------------------------------------------------------------------------
# Baseline MRR = sum of all client MRR from dim_clients
baseline_mrr = float(df_clients["mrr"].sum())

# Running MRR state — starts from baseline at 2025-10
running_mrr = baseline_mrr

retention_rows = []

for year, month in MONTHS:
    ym          = ym_str(year, month)
    month_start = date(year, month, 1)
    end_date    = month_end(year, month)

    # Filter deals closed in this month
    month_deals = df_deals[
        (df_deals["closed_date"].dt.date >= month_start)
        & (df_deals["closed_date"].dt.date <= end_date)
    ]

    def mrr_sum(deal_type):
        rows = month_deals[month_deals["deal_type"] == deal_type]
        return float(rows["mrr_delta"].sum()) if not rows.empty else 0.0

    mrr_start     = round(running_mrr, 2)
    mrr_churn     = round(mrr_sum("churn"), 2)        # negative values in raw data
    mrr_downgrade = round(mrr_sum("downgrade"), 2)    # negative values in raw data
    mrr_upsell    = round(mrr_sum("upsell"), 2)
    mrr_cross_sell = round(mrr_sum("cross_sell"), 2)
    mrr_new       = round(mrr_sum("new"), 2)

    # If no deals at all in this month, apply reasonable organic estimates
    if month_deals.empty:
        mrr_churn      = round(-mrr_start * 0.01, 2)   # ~1 % churn
        mrr_downgrade  = round(-mrr_start * 0.005, 2)  # ~0.5 % downgrade
        mrr_upsell     = round(mrr_start  * 0.015, 2)  # ~1.5 % upsell
        mrr_cross_sell = round(mrr_start  * 0.005, 2)  # ~0.5 % cross-sell
        mrr_new        = round(mrr_start  * 0.02,  2)  # ~2 % new

    mrr_end = round(
        mrr_start
        + mrr_new
        + mrr_upsell
        + mrr_cross_sell
        - abs(mrr_churn)
        - abs(mrr_downgrade),
        2,
    )

    grr = round(
        (mrr_start - abs(mrr_churn) - abs(mrr_downgrade)) / mrr_start, 4
    ) if mrr_start > 0 else 0.0

    nrr = round(mrr_end / mrr_start, 4) if mrr_start > 0 else 0.0

    retention_rows.append({
        "year_month":    ym,
        "mrr_start":     mrr_start,
        "mrr_churn":     mrr_churn,
        "mrr_downgrade": mrr_downgrade,
        "mrr_upsell":    mrr_upsell,
        "mrr_cross_sell": mrr_cross_sell,
        "mrr_new":       mrr_new,
        "mrr_end":       mrr_end,
        "grr":           grr,
        "nrr":           nrr,
    })

    # Next month's start MRR = this month's end MRR
    running_mrr = mrr_end

df_retention = pd.DataFrame(retention_rows)
with engine.connect() as conn:
    for _, row in df_retention.iterrows():
        conn.execute(
            text("""
                INSERT OR REPLACE INTO fct_revenue_retention_monthly
                    (year_month, mrr_start, mrr_churn, mrr_downgrade,
                     mrr_upsell, mrr_cross_sell, mrr_new, mrr_end, grr, nrr)
                VALUES
                    (:year_month, :mrr_start, :mrr_churn, :mrr_downgrade,
                     :mrr_upsell, :mrr_cross_sell, :mrr_new, :mrr_end, :grr, :nrr)
            """),
            row.to_dict(),
        )
    conn.commit()

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
n_clients = len(df_clients)
n_months  = len(MONTHS)
n_scores  = len(df_scores)

print(
    f"Backfill completo: {n_months} meses x {n_clients} clientes = "
    f"{n_scores} registros de score"
)
