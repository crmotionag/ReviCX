"""
02_create_dimensions.py
-----------------------
Reads raw_hubspot_companies and raw_revi_client_config from SQLite,
JOINs them via revi_client_id, computes date-based dimension columns,
and writes the result to dim_clients.
"""

from pathlib import Path
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).parent.parent / "data" / "revi_cs.db"
REFERENCE_DATE = date(2026, 4, 8)

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
engine = create_engine(f"sqlite:///{DB_PATH}")

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
query = """
SELECT
    h.revi_client_id          AS client_id,
    h.hubspot_company_id,
    h.company_name,
    h.segment_ipc,
    h.csm_owner,
    h.has_cs,
    h.contract_start_date,
    h.contract_end_date,
    h.mrr,
    c.plan_type,
    c.cashback_enabled,
    c.onboarding_completed
FROM raw_hubspot_companies AS h
INNER JOIN raw_revi_client_config AS c
    ON h.revi_client_id = c.revi_client_id
WHERE h.revi_client_id IS NOT NULL
"""

with engine.connect() as conn:
    df = pd.read_sql(text(query), conn)

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
df["contract_start_date"] = pd.to_datetime(df["contract_start_date"]).dt.date
df["contract_end_date"] = pd.to_datetime(df["contract_end_date"]).dt.date

df["days_as_client"] = df["contract_start_date"].apply(
    lambda d: (REFERENCE_DATE - d).days if pd.notna(d) else None
)

df["days_to_renewal"] = df["contract_end_date"].apply(
    lambda d: (d - REFERENCE_DATE).days if pd.notna(d) else None
)

# Ensure correct column order matching schema
dim_clients = df[[
    "client_id",
    "hubspot_company_id",
    "company_name",
    "segment_ipc",
    "csm_owner",
    "has_cs",
    "contract_start_date",
    "contract_end_date",
    "mrr",
    "plan_type",
    "cashback_enabled",
    "onboarding_completed",
    "days_as_client",
    "days_to_renewal",
]]

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS dim_clients"))
    conn.commit()

dim_clients.to_sql("dim_clients", engine, if_exists="replace", index=False)

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
print(f"dim_clients criada com {len(dim_clients)} registros")
