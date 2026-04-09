"""
Sincroniza dados do HubSpot para o SQLite local.

Uso:
    python scripts/sync_hubspot.py

Requer a variavel de ambiente HUBSPOT_API_KEY com o token do Private App.
Ou crie o arquivo .env na raiz do projeto:
    HUBSPOT_API_KEY=pat-na1-xxxxxxx

O script:
1. Puxa Companies, Deals, Calls (Engagements) e NPS (via propriedade customizada)
2. Faz upsert nas tabelas raw_hubspot_* do SQLite
3. Printa resumo do que foi sincronizado
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "revi_cs.db"
ENV_PATH = PROJECT_ROOT / ".env"

HUBSPOT_BASE = "https://api.hubapi.com"

# Carregar .env se existir
if ENV_PATH.exists():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())

API_KEY = os.environ.get("HUBSPOT_API_KEY", "")
if not API_KEY:
    print("ERRO: Defina HUBSPOT_API_KEY no .env ou como variavel de ambiente.")
    print("  Exemplo: HUBSPOT_API_KEY=pat-na1-xxxxxxx")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def hubspot_get(endpoint, params=None):
    """GET request com retry e rate-limit handling."""
    url = f"{HUBSPOT_BASE}{endpoint}"
    for attempt in range(3):
        resp = requests.get(url, headers=HEADERS, params=params or {})
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 2))
            print(f"  Rate limit atingido, aguardando {retry_after}s...")
            time.sleep(retry_after)
        else:
            print(f"  ERRO {resp.status_code}: {resp.text[:200]}")
            if attempt < 2:
                time.sleep(1)
            else:
                resp.raise_for_status()
    return {}


def hubspot_search(object_type, properties, filters=None, limit=100):
    """Search API com paginacao."""
    url = f"{HUBSPOT_BASE}/crm/v3/objects/{object_type}/search"
    all_results = []
    after = 0
    body = {
        "properties": properties,
        "limit": limit,
    }
    if filters:
        body["filterGroups"] = filters

    while True:
        if after:
            body["after"] = after
        for attempt in range(3):
            resp = requests.post(url, headers=HEADERS, json=body)
            if resp.status_code == 200:
                break
            elif resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 2)))
            else:
                resp.raise_for_status()

        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after or len(results) < limit:
            break

    return all_results


def fetch_all_objects(object_type, properties, limit=100):
    """Paginacao via list endpoint (GET)."""
    all_results = []
    after = None
    while True:
        params = {"limit": limit, "properties": ",".join(properties)}
        if after:
            params["after"] = after
        data = hubspot_get(f"/crm/v3/objects/{object_type}", params)
        results = data.get("results", [])
        all_results.extend(results)
        paging = data.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after or len(results) < limit:
            break
    return all_results


def props_to_dict(obj):
    """Extrai properties de um objeto HubSpot."""
    props = obj.get("properties", {})
    props["hubspot_id"] = obj.get("id")
    return props


def safe_float(val, default=0.0):
    try:
        return float(val) if val else default
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    try:
        return int(float(val)) if val else default
    except (ValueError, TypeError):
        return default


def ts_to_date(val):
    """Converte timestamp HubSpot (ms ou ISO) para date string."""
    if not val:
        return None
    try:
        if isinstance(val, str) and "T" in val:
            return val[:10]
        ts = int(val) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# 1. Sync Companies
# ---------------------------------------------------------------------------
def sync_companies(engine):
    """
    Puxa companies do HubSpot.

    IMPORTANTE: Ajuste os nomes das propriedades abaixo para bater com o seu HubSpot.
    Use a API /crm/v3/properties/companies para listar as propriedades disponiveis.
    """
    print("\n[1/4] Sincronizando Companies...")

    # Propriedades a buscar — ajuste conforme seu HubSpot
    PROPS = [
        "name",                    # Nome da empresa
        "industry",                # Segmento/IPC — pode ser custom property
        "hubspot_owner_id",        # Owner (CSM)
        "hs_date_entered_customer",  # Data virou cliente
        "closedate",               # Data de fechamento
        "annualrevenue",           # MRR ou receita
        # ---- Propriedades customizadas da ReviCX ----
        # Descomente e ajuste os nomes conforme existirem no seu HubSpot:
        # "revi_client_id",        # ID do cliente na ReviCX
        # "segment_ipc",           # Segmento IPC
        # "has_cs",                # Tem CS contratado
        # "contract_start_date",   # Inicio do contrato
        # "contract_end_date",     # Fim do contrato
        # "mrr",                   # MRR
    ]

    results = fetch_all_objects("companies", PROPS)
    print(f"  {len(results)} companies encontradas no HubSpot")

    rows = []
    for obj in results:
        p = props_to_dict(obj)
        rows.append({
            "hubspot_company_id": p.get("hubspot_id"),
            "company_name": p.get("name", ""),
            "segment_ipc": p.get("segment_ipc") or p.get("industry", ""),
            "csm_owner": p.get("hubspot_owner_id", ""),  # Sera resolvido para nome depois
            "has_cs": 1 if p.get("has_cs") in ("true", "1", True) else 0,
            "contract_start_date": ts_to_date(p.get("contract_start_date") or p.get("hs_date_entered_customer")),
            "contract_end_date": ts_to_date(p.get("contract_end_date") or p.get("closedate")),
            "mrr": safe_float(p.get("mrr") or p.get("annualrevenue")),
            "revi_client_id": p.get("revi_client_id", p.get("hubspot_id")),
            "created_at": datetime.now().isoformat(),
        })

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql("raw_hubspot_companies", engine, if_exists="replace", index=False)
        print(f"  {len(df)} companies gravadas no SQLite")
    else:
        print("  Nenhuma company encontrada.")

    return len(rows)


# ---------------------------------------------------------------------------
# 2. Sync Deals
# ---------------------------------------------------------------------------
def sync_deals(engine):
    print("\n[2/4] Sincronizando Deals...")

    PROPS = [
        "dealname",
        "amount",
        "closedate",
        "dealstage",
        "pipeline",
        "dealtype",
        "hs_object_id",
        # Customizadas:
        # "mrr_before",
        # "mrr_after",
        # "deal_type_cs",  # new, upsell, cross_sell, downgrade, churn
    ]

    results = fetch_all_objects("deals", PROPS)
    print(f"  {len(results)} deals encontrados")

    rows = []
    for obj in results:
        p = props_to_dict(obj)

        # Associar company
        assoc = obj.get("associations", {}).get("companies", {}).get("results", [])
        company_id = assoc[0].get("id") if assoc else None

        amount = safe_float(p.get("amount"))
        rows.append({
            "deal_id": p.get("hubspot_id"),
            "hubspot_company_id": company_id,
            "deal_type": p.get("deal_type_cs") or p.get("dealtype", "new"),
            "mrr_before": safe_float(p.get("mrr_before")),
            "mrr_after": safe_float(p.get("mrr_after", amount)),
            "mrr_delta": safe_float(p.get("mrr_after", amount)) - safe_float(p.get("mrr_before")),
            "closed_date": ts_to_date(p.get("closedate")),
            "deal_stage": p.get("dealstage", ""),
        })

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql("raw_hubspot_deals", engine, if_exists="replace", index=False)
        print(f"  {len(df)} deals gravados")

    return len(rows)


# ---------------------------------------------------------------------------
# 3. Sync Calls (Engagements)
# ---------------------------------------------------------------------------
def sync_calls(engine):
    print("\n[3/4] Sincronizando Calls...")

    PROPS = [
        "hs_call_title",
        "hs_call_direction",
        "hs_call_duration",
        "hs_timestamp",
        "hubspot_owner_id",
        "hs_call_body",
        "hs_call_status",
        # "call_type",  # Custom: consultoria, onboarding, urgencia, follow_up
    ]

    results = fetch_all_objects("calls", PROPS)
    print(f"  {len(results)} calls encontradas")

    rows = []
    for obj in results:
        p = props_to_dict(obj)

        assoc = obj.get("associations", {}).get("companies", {}).get("results", [])
        company_id = assoc[0].get("id") if assoc else None

        duration = safe_int(p.get("hs_call_duration"))
        duration_min = duration // 60 if duration > 60 else duration

        rows.append({
            "call_id": p.get("hubspot_id"),
            "hubspot_company_id": company_id,
            "csm_owner": p.get("hubspot_owner_id", ""),
            "call_date": ts_to_date(p.get("hs_timestamp")),
            "duration_minutes": duration_min,
            "call_type": p.get("call_type", "follow_up"),
            "notes": (p.get("hs_call_body") or "")[:500],
        })

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql("raw_hubspot_calls", engine, if_exists="replace", index=False)
        print(f"  {len(df)} calls gravadas")

    return len(rows)


# ---------------------------------------------------------------------------
# 4. Sync NPS (Custom Object ou Property)
# ---------------------------------------------------------------------------
def sync_nps(engine):
    """
    NPS pode estar como:
    - Propriedade em Contacts (nps_score, nps_date)
    - Custom Object
    - Feedback Submissions API

    Ajuste conforme sua implementacao.
    """
    print("\n[4/4] Sincronizando NPS...")

    # Tentativa 1: Feedback Submissions
    try:
        data = hubspot_get("/crm/v3/objects/feedback_submissions", {
            "limit": 100,
            "properties": "hs_content,hs_rating,hs_submission_timestamp,hs_contact_id"
        })
        results = data.get("results", [])
        if results:
            rows = []
            for obj in results:
                p = props_to_dict(obj)
                score = safe_int(p.get("hs_rating"))
                if score == 0:
                    continue
                category = "promoter" if score >= 9 else ("passive" if score >= 7 else "detractor")
                rows.append({
                    "response_id": p.get("hubspot_id"),
                    "hubspot_company_id": None,  # Resolver via contact association
                    "contact_email": "",
                    "nps_score": score,
                    "nps_category": category,
                    "response_date": ts_to_date(p.get("hs_submission_timestamp")),
                })
            if rows:
                df = pd.DataFrame(rows)
                df.to_sql("raw_hubspot_nps", engine, if_exists="replace", index=False)
                print(f"  {len(df)} respostas NPS gravadas")
                return len(rows)
    except Exception as e:
        print(f"  Feedback Submissions nao disponivel: {e}")

    # Tentativa 2: Contacts com propriedade NPS
    try:
        CONTACT_PROPS = ["email", "nps_score", "nps_date", "associatedcompanyid"]
        contacts = fetch_all_objects("contacts", CONTACT_PROPS)
        rows = []
        for obj in contacts:
            p = props_to_dict(obj)
            score = safe_int(p.get("nps_score"))
            if score == 0:
                continue
            category = "promoter" if score >= 9 else ("passive" if score >= 7 else "detractor")
            rows.append({
                "response_id": f"nps-{p.get('hubspot_id')}",
                "hubspot_company_id": p.get("associatedcompanyid"),
                "contact_email": p.get("email", ""),
                "nps_score": score,
                "nps_category": category,
                "response_date": ts_to_date(p.get("nps_date")),
            })
        if rows:
            df = pd.DataFrame(rows)
            df.to_sql("raw_hubspot_nps", engine, if_exists="replace", index=False)
            print(f"  {len(df)} respostas NPS (via contacts) gravadas")
            return len(rows)
    except Exception as e:
        print(f"  NPS via contacts falhou: {e}")

    print("  NPS: nenhuma fonte encontrada. Mantendo dados existentes.")
    return 0


# ---------------------------------------------------------------------------
# Resolver Owner IDs para nomes
# ---------------------------------------------------------------------------
def resolve_owners(engine):
    """Converte hubspot_owner_id para nome legivel."""
    print("\nResolvendo nomes dos owners...")
    try:
        data = hubspot_get("/crm/v3/owners/")
        owners = {str(o["id"]): f"{o.get('firstName', '')} {o.get('lastName', '')}".strip()
                  for o in data.get("results", [])}
        if owners:
            with engine.begin() as conn:
                for owner_id, name in owners.items():
                    conn.execute(text(
                        "UPDATE raw_hubspot_companies SET csm_owner = :name WHERE csm_owner = :oid"
                    ), {"name": name, "oid": owner_id})
                    conn.execute(text(
                        "UPDATE raw_hubspot_calls SET csm_owner = :name WHERE csm_owner = :oid"
                    ), {"name": name, "oid": owner_id})
            print(f"  {len(owners)} owners resolvidos: {', '.join(owners.values())}")
    except Exception as e:
        print(f"  Erro ao resolver owners: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("SYNC HUBSPOT → SQLite")
    print(f"DB: {DB_PATH}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    # Garantir que o schema existe
    schema_path = PROJECT_ROOT / "sql" / "schema.sql"
    engine = create_engine(f"sqlite:///{DB_PATH}")
    with engine.begin() as conn:
        conn.executescript = None  # SQLAlchemy nao suporta executescript
        for statement in schema_path.read_text().split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(text(statement))

    companies_n = sync_companies(engine)
    deals_n = sync_deals(engine)
    calls_n = sync_calls(engine)
    nps_n = sync_nps(engine)
    resolve_owners(engine)

    print("\n" + "=" * 60)
    print("SYNC COMPLETO")
    print(f"  Companies: {companies_n}")
    print(f"  Deals: {deals_n}")
    print(f"  Calls: {calls_n}")
    print(f"  NPS: {nps_n}")
    print("=" * 60)
    print("\nProximo passo: rode os scripts de transformacao:")
    print("  python scripts/02_create_dimensions.py")
    print("  python scripts/03_calculate_health_score.py")
    print("  python scripts/04_generate_alerts.py")


if __name__ == "__main__":
    main()
