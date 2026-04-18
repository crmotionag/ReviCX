"""
04_generate_alerts.py
---------------------
Reads the latest (and previous) health score per client from fct_health_score,
joins with dim_clients for renewal info, evaluates every alert rule defined in
scoring_rules.yaml, and inserts new alerts into fct_alerts (skipping duplicates).

Rules implemented (v1.1):
  score_dropped_to_red  – health_status == 'red' AND previous != 'red'    → critical
  no_campaign_14d       – days_since_last_campaign >= 14                  → warning
  renewal_90d           – days_to_renewal <= 90                           → info
  renewal_30d_red       – days_to_renewal <= 30 AND health_status=='red'  → critical
  volume_drop_30pct     – messages_mom_change <= -30                      → warning
  low_base_coverage     – coverage_pct < 5 AND health_status != 'inactive'→ warning
  inactive_client       – health_status == 'inactive' AND days_to_renewal <= 60 → critical
  upsell_opportunity    – health_status=='green' AND plan_usage_pct >= 90 → info
"""

from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent.parent / "data" / "revi_cs.db"
RULES_PATH = Path(__file__).parent.parent / "config" / "scoring_rules.yaml"

engine = create_engine(f"sqlite:///{DB_PATH}")

# ---------------------------------------------------------------------------
# Load alert rules from YAML
# ---------------------------------------------------------------------------
with open(RULES_PATH, encoding="utf-8") as fh:
    cfg = yaml.safe_load(fh)

alert_rules = {rule["id"]: rule for rule in cfg["alerts"]}

# ---------------------------------------------------------------------------
# 1. Latest score per client
# ---------------------------------------------------------------------------
latest_q = """
SELECT
    hs.*
FROM fct_health_score hs
INNER JOIN (
    SELECT client_id, MAX(calculated_at) AS max_at
    FROM fct_health_score
    GROUP BY client_id
) latest ON hs.client_id = latest.client_id AND hs.calculated_at = latest.max_at
"""

# 2. Previous score per client (second latest)
previous_q = """
SELECT
    hs.client_id,
    hs.health_status    AS prev_health_status,
    hs.campaign_roi     AS prev_campaign_roi,
    hs.calculated_at    AS prev_calculated_at
FROM fct_health_score hs
INNER JOIN (
    SELECT client_id, MAX(calculated_at) AS second_at
    FROM fct_health_score
    WHERE (client_id, calculated_at) NOT IN (
        SELECT client_id, MAX(calculated_at)
        FROM fct_health_score
        GROUP BY client_id
    )
    GROUP BY client_id
) prev ON hs.client_id = prev.client_id AND hs.calculated_at = prev.second_at
"""

# 3. dim_clients for renewal info
clients_q = "SELECT client_id, company_name, days_to_renewal FROM dim_clients"

with engine.connect() as conn:
    df_latest   = pd.read_sql(text(latest_q),   conn)
    df_previous = pd.read_sql(text(previous_q), conn)
    df_clients  = pd.read_sql(text(clients_q),  conn)

# ---------------------------------------------------------------------------
# Merge datasets
# ---------------------------------------------------------------------------
df = df_latest.merge(df_previous, on="client_id", how="left")
df = df.merge(df_clients,  on="client_id", how="left")

# ---------------------------------------------------------------------------
# Evaluate alert conditions
# ---------------------------------------------------------------------------
alert_date = df_latest["calculated_at"].max()  # date of the latest scoring run

records = []

for _, row in df.iterrows():
    client_id       = row["client_id"]
    health_status   = row["health_status"]
    prev_status     = row.get("prev_health_status")          # NaN when no previous
    days_campaign   = row["days_since_last_campaign"]
    mom_change      = row["messages_mom_change"]
    plan_usage      = row["plan_usage_pct"]
    coverage        = row.get("coverage_pct")
    days_renewal    = row["days_to_renewal"]

    def _alert(rule_id):
        rule = alert_rules[rule_id]
        return {
            "client_id":       client_id,
            "alert_date":      alert_date,
            "alert_id":        rule_id,
            "alert_name":      rule["name"],
            "severity":        rule["severity"],
            "action_suggested": rule["action"],
        }

    # score_dropped_to_red
    if health_status == "red" and (pd.isna(prev_status) or prev_status != "red"):
        records.append(_alert("score_dropped_to_red"))

    # no_campaign_14d
    if pd.notna(days_campaign) and days_campaign >= 14:
        records.append(_alert("no_campaign_14d"))

    # renewal_90d
    if pd.notna(days_renewal) and 31 <= days_renewal <= 90:
        records.append(_alert("renewal_90d"))

    # renewal_30d_red
    if pd.notna(days_renewal) and days_renewal <= 30 and health_status == "red":
        records.append(_alert("renewal_30d_red"))

    # volume_drop_30pct
    if pd.notna(mom_change) and mom_change <= -30:
        records.append(_alert("volume_drop_30pct"))

    # low_base_coverage
    if health_status != "inactive" and (pd.isna(coverage) or coverage < 5):
        records.append(_alert("low_base_coverage"))

    # inactive_client
    if health_status == "inactive" and pd.notna(days_renewal) and days_renewal <= 60:
        records.append(_alert("inactive_client"))

    # upsell_opportunity
    if health_status == "green" and pd.notna(plan_usage) and plan_usage >= 90:
        records.append(_alert("upsell_opportunity"))

df_alerts = pd.DataFrame(records)

# ---------------------------------------------------------------------------
# 5. Insert into fct_alerts — skip duplicates (client_id + alert_id + alert_date)
# ---------------------------------------------------------------------------
if df_alerts.empty:
    print("Nenhum alerta gerado.")
else:
    # Load existing alert keys to deduplicate
    existing_q = """
        SELECT client_id, alert_id, alert_date FROM fct_alerts
    """
    with engine.connect() as conn:
        df_existing = pd.read_sql(text(existing_q), conn)

    if not df_existing.empty:
        existing_keys = set(
            zip(df_existing["client_id"], df_existing["alert_id"], df_existing["alert_date"])
        )
        df_alerts = df_alerts[
            ~df_alerts.apply(
                lambda r: (r["client_id"], r["alert_id"], str(r["alert_date"])) in existing_keys,
                axis=1,
            )
        ]

    df_alerts["resolved"] = 0
    df_alerts["resolved_at"] = None
    df_alerts["resolved_by"] = None

    if not df_alerts.empty:
        df_alerts.to_sql("fct_alerts", engine, if_exists="append", index=False)

# ---------------------------------------------------------------------------
# 6. Print summary
# ---------------------------------------------------------------------------
total     = len(df_alerts)
criticals = (df_alerts["severity"] == "critical").sum() if not df_alerts.empty else 0
warnings  = (df_alerts["severity"] == "warning").sum()  if not df_alerts.empty else 0
infos     = (df_alerts["severity"] == "info").sum()      if not df_alerts.empty else 0

print(f"Alertas gerados: {total} total")
print(f"Criticos: {criticals} | Warning: {warnings} | Info: {infos}")

# Top 5 most urgent: critical first, then by company_name
if not df_alerts.empty:
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    df_top = df_alerts.copy()
    df_top = df_top.merge(df_clients[["client_id", "company_name"]], on="client_id", how="left")
    df_top["sev_rank"] = df_top["severity"].map(severity_order)
    df_top = df_top.sort_values(["sev_rank", "company_name"]).head(5)

    print("\nTop 5 alertas mais urgentes:")
    for _, r in df_top.iterrows():
        name = r.get("company_name", r["client_id"])
        print(f"  [{r['severity'].upper():8s}] {name} — {r['alert_name']}")
