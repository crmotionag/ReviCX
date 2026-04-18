"""
Populate data/revi_cs.db with real Nekt data exported to data/nekt_exports/.

PoC path: avoids needing AWS credentials in the dashboard by caching Nekt view
results to disk and replaying them into the SQLite mock. Streamlit keeps
reading SQLite (load_data_from_sqlite); pages render real Nekt-derived data.

Inputs: data/nekt_exports/*.json  (7 views; alerts split into p1+p2)
Outputs: overwrites dim/fct tables in data/revi_cs.db. Preserves app_users,
raw_hubspot_nps, and raw_* mock tables used by load_data_from_sqlite fallbacks.

Run: python scripts/sync_nekt_to_sqlite.py
"""
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent
EXPORTS = ROOT / "data" / "nekt_exports"
DB = ROOT / "data" / "revi_cs.db"


def read_export(name: str) -> pd.DataFrame:
    path = EXPORTS / f"{name}.json"
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    df = pd.DataFrame(payload["data"], columns=payload["columns"])
    # drop padding columns we added to force MCP overflow-to-disk
    for pad in ("_p1", "_p2", "_pad"):
        if pad in df.columns:
            df = df.drop(columns=[pad])
    return df


def load_alerts() -> pd.DataFrame:
    p1 = read_export("fct_alerts_p1")
    p2 = read_export("fct_alerts_p2")
    return pd.concat([p1, p2], ignore_index=True)


def reshape_coverage(cov_raw: pd.DataFrame, clients: pd.DataFrame) -> pd.DataFrame:
    """Granular (csm, month, slug) → per (csm, year_month) with has_cs splits."""
    clients_slim = clients[["csm_owner", "company_name", "has_cs"]].copy()
    clients_slim["slug"] = clients_slim["company_name"].str.lower().str.strip()

    contacted = cov_raw.merge(
        clients_slim,
        left_on=["csm_owner", "client_name_slug"],
        right_on=["csm_owner", "slug"],
        how="left",
    )
    contacted["has_cs"] = contacted["has_cs"].fillna(True)

    contacted_agg = contacted.groupby(["csm_owner", "year_month"]).agg(
        clients_with_cs_contacted=("has_cs", lambda s: int(s.sum())),
        clients_without_cs_contacted=("has_cs", lambda s: int((~s.astype(bool)).sum())),
    ).reset_index()

    totals = clients_slim.groupby("csm_owner").agg(
        clients_with_cs_total=("has_cs", lambda s: int(s.sum())),
        clients_without_cs_total=("has_cs", lambda s: int((~s.astype(bool)).sum())),
    ).reset_index()

    out = contacted_agg.merge(totals, on="csm_owner", how="left")
    out["coverage_with_cs_pct"] = (
        out["clients_with_cs_contacted"] / out["clients_with_cs_total"].replace(0, pd.NA) * 100
    ).round(1).fillna(0)
    out["coverage_without_cs_pct"] = (
        out["clients_without_cs_contacted"] / out["clients_without_cs_total"].replace(0, pd.NA) * 100
    ).round(1).fillna(0)
    out["sqls_identified"] = 0
    return out[
        [
            "csm_owner", "year_month",
            "clients_with_cs_total", "clients_with_cs_contacted",
            "clients_without_cs_total", "clients_without_cs_contacted",
            "coverage_with_cs_pct", "coverage_without_cs_pct",
            "sqls_identified",
        ]
    ]


def build_clients(dim: pd.DataFrame, upsell: pd.DataFrame) -> pd.DataFrame:
    clients = dim.copy()
    # Only active clients — drop anyone with a churn_date set.
    if "churn_date" in clients.columns:
        churn = clients["churn_date"].astype(str).str.strip()
        clients = clients[churn.isin(["", "None", "nan", "NaT", "null"]) | clients["churn_date"].isna()].copy()
    clients["has_cs"] = clients["has_cs"].fillna(False).astype(str).str.lower().isin(["true", "1"])
    clients["segment_ipc"] = None
    clients["plan_type"] = None
    clients["onboarding_completed"] = clients["onboarding_end_date"].notna() & (clients["onboarding_end_date"] != "")
    clients["contract_start_date"] = pd.to_datetime(clients["contract_start_date"], errors="coerce")
    clients["contract_end_date"] = clients["contract_start_date"] + pd.DateOffset(years=1)
    clients["days_as_client"] = (pd.Timestamp.today().normalize() - clients["contract_start_date"]).dt.days
    cb = upsell[["client_id", "cashback_enabled"]].copy()
    cb["cashback_enabled"] = cb["cashback_enabled"].astype(str).str.lower().isin(["true", "1"])
    clients = clients.merge(cb, on="client_id", how="left")
    clients["cashback_enabled"] = clients["cashback_enabled"].fillna(False).astype(bool)
    clients["days_to_renewal"] = pd.to_numeric(clients["days_to_renewal"], errors="coerce").astype("Int64")
    clients["mrr"] = pd.to_numeric(clients["mrr"], errors="coerce").fillna(0.0)
    return clients[
        [
            "client_id", "hubspot_company_id", "company_name", "segment_ipc",
            "csm_owner", "has_cs", "contract_start_date", "contract_end_date",
            "mrr", "plan_type", "cashback_enabled", "onboarding_completed",
            "days_as_client", "days_to_renewal",
        ]
    ]


def build_health(health: pd.DataFrame) -> pd.DataFrame:
    h = health.copy()
    int_cols = [
        "score_recency", "score_roi", "score_automations", "score_integrations",
        "score_chat", "score_volume", "bonus_cashback",
        "days_since_last_campaign", "active_automations", "integration_automations",
        "messages_sent_current", "messages_sent_previous", "total_score",
    ]
    float_cols = ["campaign_roi", "messages_mom_change", "plan_usage_pct"]
    for c in int_cols:
        h[c] = pd.to_numeric(h[c], errors="coerce").astype("Int64")
    for c in float_cols:
        h[c] = pd.to_numeric(h[c], errors="coerce")
    h["calculated_at"] = pd.to_datetime(h["calculated_at"]).dt.date.astype(str)
    return h


def build_alerts(alerts: pd.DataFrame) -> pd.DataFrame:
    a = alerts.copy()
    a["resolved"] = a["resolved"].astype(str).str.lower().isin(["true", "1"]).astype(int)
    a["resolved_at"] = None
    a["resolved_by"] = None
    a["alert_date"] = pd.to_datetime(a["alert_date"]).dt.date.astype(str)
    return a[
        [
            "client_id", "alert_date", "alert_id", "alert_name",
            "severity", "action_suggested", "resolved", "resolved_at", "resolved_by",
        ]
    ]


def build_csm_activity(csm_wk: pd.DataFrame) -> pd.DataFrame:
    c = csm_wk.copy()
    c["calls_total"] = pd.to_numeric(c["calls_total"], errors="coerce").fillna(0).astype(int)
    c["meetings_total"] = pd.to_numeric(c["meetings_total"], errors="coerce").fillna(0).astype(int)
    c["total_duration_minutes"] = pd.to_numeric(c["total_duration_minutes"], errors="coerce").fillna(0).astype(int)
    # pending: breakdown by call type not classified yet in Nekt
    for col in ("calls_consultoria", "calls_onboarding", "calls_urgencia", "calls_follow_up"):
        c[col] = 0
    # best-effort: unique clients contacted ≈ meetings_total (granular clients not exposed here)
    c["unique_clients_contacted"] = c["meetings_total"]
    c["week_start"] = pd.to_datetime(c["week_start"]).dt.date.astype(str)
    # Dedupe to satisfy PK(csm_owner, week_start)
    c = c.groupby(["csm_owner", "week_start"], as_index=False).agg(
        calls_total=("calls_total", "sum"),
        calls_consultoria=("calls_consultoria", "sum"),
        calls_onboarding=("calls_onboarding", "sum"),
        calls_urgencia=("calls_urgencia", "sum"),
        calls_follow_up=("calls_follow_up", "sum"),
        unique_clients_contacted=("unique_clients_contacted", "sum"),
        total_duration_minutes=("total_duration_minutes", "sum"),
    )
    return c


def build_revenue(rev: pd.DataFrame) -> pd.DataFrame:
    r = rev.copy()
    for c in ["mrr_start", "mrr_churn", "mrr_downgrade", "mrr_upsell",
              "mrr_cross_sell", "mrr_new", "mrr_end", "grr", "nrr"]:
        r[c] = pd.to_numeric(r[c], errors="coerce").fillna(0.0)
    return r[
        [
            "year_month", "mrr_start", "mrr_churn", "mrr_downgrade",
            "mrr_upsell", "mrr_cross_sell", "mrr_new", "mrr_end", "grr", "nrr",
        ]
    ]


def build_campaign_channels(upsell: pd.DataFrame) -> pd.DataFrame:
    """Synthesize raw_revi_campaigns rows that encode has_sms / has_email flags
    from Nekt upsell data. load_data_from_sqlite aggregates this table to derive
    channel flags for the Upsell page."""
    u = upsell[["client_id", "has_sms", "has_email"]].copy()
    u["has_sms"] = u["has_sms"].astype(str).str.lower().isin(["true", "1"])
    u["has_email"] = u["has_email"].astype(str).str.lower().isin(["true", "1"])
    rows = []
    for _, r in u.iterrows():
        if r["has_sms"]:
            rows.append({
                "campaign_id": f"{r['client_id']}-sms",
                "revi_client_id": r["client_id"],
                "campaign_name": "_synthetic_sms_flag",
                "sent_at": None, "recipients_count": 0, "messages_sent": 0,
                "revenue_attributed": 0, "cost": 0, "campaign_type": "sms",
            })
        if r["has_email"]:
            rows.append({
                "campaign_id": f"{r['client_id']}-email",
                "revi_client_id": r["client_id"],
                "campaign_name": "_synthetic_email_flag",
                "sent_at": None, "recipients_count": 0, "messages_sent": 0,
                "revenue_attributed": 0, "cost": 0, "campaign_type": "email",
            })
    return pd.DataFrame(rows)


def main():
    print(f"-> reading exports from {EXPORTS}")
    dim = read_export("dim_clients")
    health = read_export("fct_health_score")
    alerts = load_alerts()
    csm_wk = read_export("fct_csm_activity_weekly")
    cov_raw = read_export("fct_coverage_monthly")
    revenue = read_export("fct_revenue_retention_monthly")
    upsell = read_export("fct_upsell_flags")

    print(f"   dim_clients={len(dim)} health={len(health)} alerts={len(alerts)} "
          f"csm_wk={len(csm_wk)} cov_raw={len(cov_raw)} rev={len(revenue)} upsell={len(upsell)}")

    clients = build_clients(dim, upsell)
    active_ids = set(clients["client_id"])
    # Propagate active filter to fact tables keyed by client_id
    health = health[health["client_id"].isin(active_ids)].copy()
    alerts = alerts[alerts["client_id"].isin(active_ids)].copy()
    upsell = upsell[upsell["client_id"].isin(active_ids)].copy()

    health_out = build_health(health)
    alerts_out = build_alerts(alerts)
    csm_out = build_csm_activity(csm_wk)
    coverage_out = reshape_coverage(cov_raw, clients)
    revenue_out = build_revenue(revenue)
    campaigns_out = build_campaign_channels(upsell)

    engine = create_engine(f"sqlite:///{DB}")
    with engine.begin() as conn:
        # Truncate + refill dim/fct tables
        for tbl in [
            "dim_clients", "fct_health_score", "fct_alerts",
            "fct_csm_activity_weekly", "fct_coverage_monthly",
            "fct_revenue_retention_monthly",
        ]:
            conn.execute(text(f"DELETE FROM {tbl}"))

        clients.to_sql("dim_clients", conn, if_exists="append", index=False)
        health_out.to_sql("fct_health_score", conn, if_exists="append", index=False)
        alerts_out.to_sql("fct_alerts", conn, if_exists="append", index=False)
        csm_out.to_sql("fct_csm_activity_weekly", conn, if_exists="append", index=False)
        coverage_out.to_sql("fct_coverage_monthly", conn, if_exists="append", index=False)
        revenue_out.to_sql("fct_revenue_retention_monthly", conn, if_exists="append", index=False)

        # Replace raw_revi_campaigns with Nekt-derived channel flags only
        conn.execute(text("DELETE FROM raw_revi_campaigns"))
        if not campaigns_out.empty:
            campaigns_out.to_sql("raw_revi_campaigns", conn, if_exists="append", index=False)

    print(f"[ok] wrote {len(clients)} clients, {len(health_out)} scores, "
          f"{len(alerts_out)} alerts, {len(csm_out)} weekly activities, "
          f"{len(coverage_out)} coverage rows, {len(revenue_out)} revenue rows, "
          f"{len(campaigns_out)} synthetic campaigns")


if __name__ == "__main__":
    main()
