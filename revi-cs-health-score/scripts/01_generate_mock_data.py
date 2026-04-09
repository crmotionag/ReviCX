"""
01_generate_mock_data.py
Gera dados mock realistas para 60 clientes e-commerce da ReviCX.
Popula o banco SQLite em data/revi_cs.db.

Referencia: 2026-04-08
Seeds: random.seed(42), Faker('pt_BR') com seed_instance(42)
"""

import random
import sqlite3
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

# ---------------------------------------------------------------------------
# Config global
# ---------------------------------------------------------------------------
REFERENCE_DATE = date(2026, 4, 8)
random.seed(42)
fake = Faker("pt_BR")
fake.seed_instance(42)

DB_PATH = Path(__file__).parent.parent / "data" / "revi_cs.db"
SCHEMA_PATH = Path(__file__).parent.parent / "sql" / "schema.sql"

# ---------------------------------------------------------------------------
# Distribuicoes de segmentos / CSMs / planos
# ---------------------------------------------------------------------------
SEGMENT_DIST = (
    ["Suplementos"] * 15
    + ["Varejo"] * 12
    + ["Moda"] * 10
    + ["Alimentacao"] * 8
    + ["Bet"] * 8
    + ["SaaS"] * 7
)

PLAN_CONFIG = {
    "Starter":    {"msg_limit": 5_000,   "mrr_range": (297,  597),  "count": 20},
    "Growth":     {"msg_limit": 20_000,  "mrr_range": (897,  1497), "count": 25},
    "Enterprise": {"msg_limit": 100_000, "mrr_range": (1997, 4997), "count": 15},
}

PLANS_LIST = (
    ["Starter"] * 20
    + ["Growth"] * 25
    + ["Enterprise"] * 15
)

CSM_OWNERS = ["Ana Silva"] * 30 + ["Bruno Costa"] * 30

# ---------------------------------------------------------------------------
# Perfis de comportamento
# ---------------------------------------------------------------------------
PROFILE_DIST = (
    ["power_user"] * 15
    + ["operational"] * 25
    + ["at_risk"] * 20
)

AUTOMATION_TYPES_STANDARD = [
    "Abandono de Carrinho",
    "Boas-vindas",
    "Pos-compra",
    "Reengajamento",
    "Aniversario",
    "Lembrete de Pagamento",
]

AUTOMATION_TYPES_INTEGRATION = [
    "Integracao Shopify",
    "Integracao VTEX",
    "Integracao WooCommerce",
    "Integracao Nuvemshop",
    "Integracao Salesforce",
]

CALL_TYPES = ["consultoria", "onboarding", "urgencia", "follow_up"]
CALL_TYPE_WEIGHTS = [0.60, 0.20, 0.10, 0.10]

CAMPAIGN_TYPES = ["whatsapp", "sms", "email", "push"]
CAMPAIGN_TYPE_WEIGHTS = [0.70, 0.15, 0.10, 0.05]


# ---------------------------------------------------------------------------
# Utilitarios
# ---------------------------------------------------------------------------

def uid() -> str:
    return str(uuid.uuid4())


def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))


def rand_datetime(start: date, end: date) -> datetime:
    d = rand_date(start, end)
    h = random.randint(8, 18)
    m = random.randint(0, 59)
    return datetime(d.year, d.month, d.day, h, m)


def months_ago(n: int) -> date:
    """Retorna data aproximada de N meses atras a partir de REFERENCE_DATE."""
    return REFERENCE_DATE - timedelta(days=n * 30)


def year_month_str(d: date) -> str:
    return d.strftime("%Y-%m")


def six_month_periods():
    """Retorna lista de (year_month_str, date_start, date_end) para Nov2025-Abr2026."""
    periods = []
    for i in range(5, -1, -1):          # 5 meses atras ate o mes atual
        anchor = REFERENCE_DATE - timedelta(days=i * 30)
        ym = anchor.strftime("%Y-%m")
        # inicio e fim do mes calendario
        start = date(anchor.year, anchor.month, 1)
        if anchor.month == 12:
            end = date(anchor.year + 1, 1, 1) - timedelta(days=1)
        else:
            end = date(anchor.year, anchor.month + 1, 1) - timedelta(days=1)
        periods.append((ym, start, end))
    return periods


MONTHLY_PERIODS = six_month_periods()   # [(ym, start, end), ...]


# ---------------------------------------------------------------------------
# Geradores por perfil
# ---------------------------------------------------------------------------

def profile_campaign_params(profile: str, plan: str) -> dict:
    limit = PLAN_CONFIG[plan]["msg_limit"]
    if profile == "power_user":
        last_days_ago   = random.randint(1, 5)
        roi             = round(random.uniform(10, 25), 2)
        campaigns_month = random.randint(4, 5)
        usage_pct       = random.uniform(0.70, 1.00)
    elif profile == "operational":
        last_days_ago   = random.randint(8, 18)
        roi             = round(random.uniform(2, 8), 2)
        campaigns_month = random.randint(1, 2)
        usage_pct       = random.uniform(0.40, 0.70)
    else:  # at_risk
        last_days_ago   = random.randint(21, 60)
        roi             = round(random.uniform(0, 0.5), 2)
        campaigns_month = random.randint(0, 1)
        usage_pct       = random.uniform(0.05, 0.20)
    return {
        "last_days_ago":    last_days_ago,
        "roi":              roi,
        "campaigns_month":  campaigns_month,
        "usage_pct":        usage_pct,
        "msg_limit":        limit,
    }


def nps_score_for_profile(profile: str) -> int:
    if profile == "power_user":
        # 40% promotor (9-10), resto passivo
        if random.random() < 0.40:
            return random.randint(9, 10)
        return random.randint(7, 8)
    elif profile == "operational":
        # 50% passivo (7-8), resto distribuido
        if random.random() < 0.50:
            return random.randint(7, 8)
        if random.random() < 0.50:
            return random.randint(9, 10)
        return random.randint(4, 6)
    else:  # at_risk
        # 60% detrator (0-6)
        if random.random() < 0.60:
            return random.randint(0, 6)
        return random.randint(7, 8)


def nps_category(score: int) -> str:
    if score >= 9:
        return "promoter"
    elif score >= 7:
        return "passive"
    return "detractor"


# ---------------------------------------------------------------------------
# Geracao de clientes
# ---------------------------------------------------------------------------

def build_clients() -> list[dict]:
    """Retorna lista de 60 dicionarios descrevendo cada cliente."""
    segments  = SEGMENT_DIST.copy()
    csms      = CSM_OWNERS.copy()
    plans     = PLANS_LIST.copy()
    profiles  = PROFILE_DIST.copy()

    # CS: 30 com, 30 sem — embaralhados proporcionalmente
    has_cs_flags = [True] * 30 + [False] * 30

    random.shuffle(segments)
    random.shuffle(csms)
    random.shuffle(plans)
    random.shuffle(profiles)
    random.shuffle(has_cs_flags)

    clients = []
    for i in range(60):
        plan = plans[i]
        mrr_lo, mrr_hi = PLAN_CONFIG[plan]["mrr_range"]
        mrr = round(random.uniform(mrr_lo, mrr_hi), 2)

        # Contrato: inicio entre 6 e 18 meses atras; duracao 12 meses
        start_offset_days = random.randint(180, 540)
        contract_start = REFERENCE_DATE - timedelta(days=start_offset_days)
        contract_end   = contract_start + timedelta(days=365)

        clients.append({
            "hubspot_company_id": f"HS-{10000 + i}",
            "revi_client_id":     f"REVI-{20000 + i}",
            "company_name":       fake.company(),
            "segment_ipc":        segments[i],
            "csm_owner":          csms[i],
            "has_cs":             has_cs_flags[i],
            "contract_start_date": contract_start.isoformat(),
            "contract_end_date":   contract_end.isoformat(),
            "mrr":                mrr,
            "plan":               plan,
            "profile":            profiles[i],
        })
    return clients


# ---------------------------------------------------------------------------
# Insercao: raw_hubspot_companies
# ---------------------------------------------------------------------------

def insert_companies(conn: sqlite3.Connection, clients: list[dict]):
    rows = []
    for c in clients:
        rows.append((
            c["hubspot_company_id"],
            c["company_name"],
            c["segment_ipc"],
            c["csm_owner"],
            int(c["has_cs"]),
            c["contract_start_date"],
            c["contract_end_date"],
            c["mrr"],
            c["revi_client_id"],
            REFERENCE_DATE.isoformat(),
        ))
    conn.executemany(
        """INSERT INTO raw_hubspot_companies
           (hubspot_company_id, company_name, segment_ipc, csm_owner, has_cs,
            contract_start_date, contract_end_date, mrr, revi_client_id, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )


# ---------------------------------------------------------------------------
# Insercao: raw_hubspot_deals
# ---------------------------------------------------------------------------

def insert_deals(conn: sqlite3.Connection, clients: list[dict]) -> int:
    rows = []
    total = 0
    for c in clients:
        hid     = c["hubspot_company_id"]
        mrr     = c["mrr"]
        profile = c["profile"]
        start   = date.fromisoformat(c["contract_start_date"])
        end     = date.fromisoformat(c["contract_end_date"])

        # Deal inicial (new)
        rows.append((
            uid(), hid, "new", 0.0, mrr, mrr,
            start.isoformat(), "closed_won",
        ))
        total += 1

        if profile == "power_user":
            # 3-5 upsells
            for _ in range(random.randint(3, 5)):
                delta  = round(random.uniform(50, 300), 2)
                before = round(mrr - delta, 2)
                d      = rand_date(start, min(REFERENCE_DATE, end))
                rows.append((
                    uid(), hid, "upsell", before, mrr, delta,
                    d.isoformat(), "closed_won",
                ))
                total += 1

        elif profile == "operational":
            # 0-1 upsells ocasionais
            if random.random() < 0.3:
                delta  = round(random.uniform(30, 150), 2)
                before = round(mrr - delta, 2)
                d      = rand_date(start, min(REFERENCE_DATE, end))
                rows.append((
                    uid(), hid, "upsell", before, mrr, delta,
                    d.isoformat(), "closed_won",
                ))
                total += 1

        else:  # at_risk: 2-3 downgrades possiveis; 2 churns possiveis
            for _ in range(random.randint(2, 3)):
                delta  = round(random.uniform(50, 200), 2)
                before = round(mrr + delta, 2)
                d      = rand_date(start, min(REFERENCE_DATE, end))
                rows.append((
                    uid(), hid, "downgrade", before, mrr, -delta,
                    d.isoformat(), "closed_lost",
                ))
                total += 1
            if random.random() < 0.10:   # ~2 churns no total (10% de 20)
                d = rand_date(start, min(REFERENCE_DATE, end))
                rows.append((
                    uid(), hid, "churn", mrr, 0.0, -mrr,
                    d.isoformat(), "closed_lost",
                ))
                total += 1

    conn.executemany(
        """INSERT INTO raw_hubspot_deals
           (deal_id, hubspot_company_id, deal_type, mrr_before, mrr_after,
            mrr_delta, closed_date, deal_stage)
           VALUES (?,?,?,?,?,?,?,?)""",
        rows,
    )
    return total


# ---------------------------------------------------------------------------
# Insercao: raw_hubspot_calls
# ---------------------------------------------------------------------------
# Estrategia:
#   - 6 meses (Out 2025 - Mar 2026) -> ~26 semanas
#   - 15-20 calls/semana por CSM (total ~400-520 por CSM, ~800-1040 total)
#   - Clientes COM CS: ~1.5 calls/mes => 9 calls em 6 meses
#   - Clientes SEM CS: ~0.3 calls/mes => ~2 calls em 6 meses
# ---------------------------------------------------------------------------

def insert_calls(conn: sqlite3.Connection, clients: list[dict]) -> int:
    # Separar clientes por CSM
    by_csm: dict[str, list[dict]] = {}
    for c in clients:
        by_csm.setdefault(c["csm_owner"], []).append(c)

    period_start = date(2025, 10, 1)
    period_end   = date(2026, 3, 31)

    # Pre-calcular chamadas esperadas por cliente
    # (usado como pool para distribuicao por semana)
    client_calls: dict[str, list[date]] = {}  # hid -> lista de datas de call

    for c in clients:
        hid = c["hubspot_company_id"]
        n_calls = 9 if c["has_cs"] else 2
        # adicionar variacao
        n_calls = max(0, n_calls + random.randint(-2, 2))
        dates = []
        for _ in range(n_calls):
            dates.append(rand_date(period_start, period_end))
        client_calls[hid] = dates

    rows = []
    for csm, csm_clients in by_csm.items():
        # Agrupar calls por semana para garantir 15-20/semana
        week_start = period_start
        while week_start <= period_end:
            week_end = min(week_start + timedelta(days=6), period_end)
            calls_this_week = random.randint(15, 20)
            # Sortear clientes para essa semana
            for _ in range(calls_this_week):
                client = random.choice(csm_clients)
                hid    = client["hubspot_company_id"]
                call_date = rand_date(week_start, week_end)
                ctype  = random.choices(CALL_TYPES, weights=CALL_TYPE_WEIGHTS)[0]
                dur    = random.randint(15, 60)
                rows.append((
                    uid(), hid, csm,
                    call_date.isoformat(), dur, ctype,
                    f"Registro automatico - {ctype}",
                ))
            week_start += timedelta(days=7)

    conn.executemany(
        """INSERT INTO raw_hubspot_calls
           (call_id, hubspot_company_id, csm_owner, call_date,
            duration_minutes, call_type, notes)
           VALUES (?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Insercao: raw_hubspot_nps
# ---------------------------------------------------------------------------

def insert_nps(conn: sqlite3.Connection, clients: list[dict]) -> int:
    rows = []
    period_start = months_ago(6)
    for c in clients:
        hid     = c["hubspot_company_id"]
        profile = c["profile"]
        n       = random.randint(1, 3)
        for _ in range(n):
            score = nps_score_for_profile(profile)
            cat   = nps_category(score)
            d     = rand_date(period_start, REFERENCE_DATE)
            email = fake.email()
            rows.append((
                uid(), hid, email, score, cat, d.isoformat(),
            ))
    conn.executemany(
        """INSERT INTO raw_hubspot_nps
           (response_id, hubspot_company_id, contact_email,
            nps_score, nps_category, response_date)
           VALUES (?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Insercao: raw_revi_campaigns
# ---------------------------------------------------------------------------

def insert_campaigns(conn: sqlite3.Connection, clients: list[dict]) -> int:
    rows = []
    for c in clients:
        rid     = c["revi_client_id"]
        profile = c["profile"]
        plan    = c["plan"]
        params  = profile_campaign_params(profile, plan)
        limit   = params["msg_limit"]

        for ym, p_start, p_end in MONTHLY_PERIODS:
            n = params["campaigns_month"]
            if profile == "at_risk" and n == 0:
                # 50% de chance de nao ter nenhuma campanha nesse mes
                if random.random() < 0.50:
                    continue
                n = 1

            for _ in range(n):
                ctype      = random.choices(CAMPAIGN_TYPES, weights=CAMPAIGN_TYPE_WEIGHTS)[0]
                recipients = int(limit * params["usage_pct"] * random.uniform(0.3, 0.5))
                recipients = max(10, recipients)
                msgs       = int(recipients * random.uniform(0.90, 1.00))
                cost       = round(msgs * random.uniform(0.005, 0.02), 2)
                revenue    = round(cost * params["roi"], 2)
                sent_at    = rand_datetime(p_start, p_end)

                name = f"Campanha {ctype.capitalize()} {fake.word().capitalize()} {ym}"
                rows.append((
                    uid(), rid, name,
                    sent_at.isoformat(),
                    recipients, msgs,
                    revenue, cost, ctype,
                ))

    conn.executemany(
        """INSERT INTO raw_revi_campaigns
           (campaign_id, revi_client_id, campaign_name, sent_at,
            recipients_count, messages_sent, revenue_attributed, cost, campaign_type)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Insercao: raw_revi_automations
# ---------------------------------------------------------------------------

def insert_automations(conn: sqlite3.Connection, clients: list[dict]) -> int:
    rows = []
    for c in clients:
        rid     = c["revi_client_id"]
        profile = c["profile"]
        start   = date.fromisoformat(c["contract_start_date"])

        if profile == "power_user":
            n_std  = random.randint(3, 6)
            n_int  = random.randint(3, 5)
            # alguns pausados adicionais
            n_paused = random.randint(1, 2)
        elif profile == "operational":
            n_std  = random.randint(1, 2)
            n_int  = random.randint(1, 2)
            n_paused = random.randint(0, 1)
        else:  # at_risk
            n_std  = 0
            n_int  = 0
            n_paused = random.randint(0, 2)  # alguns em draft/paused

        # Ativas - standard
        for j in range(n_std):
            atype = random.choice(AUTOMATION_TYPES_STANDARD)
            created = rand_date(start, REFERENCE_DATE - timedelta(days=30))
            triggered = rand_date(created + timedelta(days=1), REFERENCE_DATE)
            rows.append((
                uid(), rid,
                f"{atype} - Ativa",
                "standard", "active",
                created.isoformat(), triggered.isoformat(),
            ))

        # Ativas - integration
        for j in range(n_int):
            atype = random.choice(AUTOMATION_TYPES_INTEGRATION)
            created = rand_date(start, REFERENCE_DATE - timedelta(days=30))
            triggered = rand_date(created + timedelta(days=1), REFERENCE_DATE)
            rows.append((
                uid(), rid,
                f"{atype} - Ativa",
                "integration", "active",
                created.isoformat(), triggered.isoformat(),
            ))

        # Pausadas / draft
        for j in range(n_paused):
            atype  = random.choice(AUTOMATION_TYPES_STANDARD)
            status = random.choice(["paused", "draft"])
            created = rand_date(start, REFERENCE_DATE)
            rows.append((
                uid(), rid,
                f"{atype} - {status.capitalize()}",
                "standard", status,
                created.isoformat(), None,
            ))

    conn.executemany(
        """INSERT INTO raw_revi_automations
           (automation_id, revi_client_id, automation_name,
            automation_type, status, created_at, last_triggered_at)
           VALUES (?,?,?,?,?,?,?)""",
        rows,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Insercao: raw_revi_chat
# ---------------------------------------------------------------------------

def insert_chat(conn: sqlite3.Connection, clients: list[dict]):
    rows = []
    for c in clients:
        rid     = c["revi_client_id"]
        profile = c["profile"]

        if profile == "power_user":
            has_human   = True
            agents      = random.randint(3, 8)
            has_flow    = True
            has_ai      = True
            last_conv   = rand_datetime(
                REFERENCE_DATE - timedelta(days=2),
                REFERENCE_DATE,
            )
        elif profile == "operational":
            has_human   = random.random() < 0.60
            agents      = random.randint(1, 3) if has_human else 0
            has_flow    = random.random() < 0.70
            has_ai      = False
            last_conv   = rand_datetime(
                REFERENCE_DATE - timedelta(days=10),
                REFERENCE_DATE - timedelta(days=1),
            )
        else:  # at_risk
            has_human   = False
            agents      = 0
            has_flow    = False
            has_ai      = False
            last_conv   = None

        rows.append((
            rid,
            int(has_human), agents,
            int(has_flow), int(has_ai),
            last_conv.isoformat() if last_conv else None,
        ))

    conn.executemany(
        """INSERT INTO raw_revi_chat
           (revi_client_id, has_human_agent, human_agents_count,
            has_chat_flow, has_ai_enabled, last_conversation_at)
           VALUES (?,?,?,?,?,?)""",
        rows,
    )


# ---------------------------------------------------------------------------
# Insercao: raw_revi_messages_monthly
# ---------------------------------------------------------------------------

def insert_messages_monthly(conn: sqlite3.Connection, clients: list[dict]):
    rows = []
    for c in clients:
        rid     = c["revi_client_id"]
        plan    = c["plan"]
        profile = c["profile"]
        limit   = PLAN_CONFIG[plan]["msg_limit"]

        if profile == "power_user":
            base_pct = random.uniform(0.70, 0.90)
            trend    = 0.03   # crescente ~3% ao mes
        elif profile == "operational":
            base_pct = random.uniform(0.40, 0.65)
            trend    = 0.00   # estaveel
        else:  # at_risk
            base_pct = random.uniform(0.10, 0.20)
            trend    = -0.02  # declinante

        for idx, (ym, _start, _end) in enumerate(MONTHLY_PERIODS):
            pct  = max(0.01, base_pct + trend * idx + random.uniform(-0.03, 0.03))
            msgs = int(limit * pct)
            rows.append((rid, ym, msgs, limit))

    conn.executemany(
        """INSERT INTO raw_revi_messages_monthly
           (revi_client_id, year_month, messages_sent, plan_limit)
           VALUES (?,?,?,?)""",
        rows,
    )


# ---------------------------------------------------------------------------
# Insercao: raw_revi_client_config
# ---------------------------------------------------------------------------

def insert_client_config(conn: sqlite3.Connection, clients: list[dict]):
    rows = []
    for c in clients:
        rid     = c["revi_client_id"]
        plan    = c["plan"]
        profile = c["profile"]

        if profile == "power_user":
            cashback   = random.random() < 0.70
            onboarding = True
            ob_days    = random.randint(30, 90)
        elif profile == "operational":
            cashback   = random.random() < 0.20
            onboarding = True
            ob_days    = random.randint(20, 60)
        else:  # at_risk
            cashback   = False
            onboarding = random.random() < 0.50
            ob_days    = random.randint(60, 180)

        ob_date = (
            (REFERENCE_DATE - timedelta(days=ob_days)).isoformat()
            if onboarding else None
        )

        rows.append((
            rid, plan,
            int(cashback),
            int(onboarding),
            ob_date,
        ))

    conn.executemany(
        """INSERT INTO raw_revi_client_config
           (revi_client_id, plan_type, cashback_enabled,
            onboarding_completed, onboarding_completed_at)
           VALUES (?,?,?,?,?)""",
        rows,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Garantir que o diretorio data existe
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Conectar e criar schema
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = OFF")   # FK desligada durante carga mock

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)

    print(f"Schema aplicado em: {DB_PATH}")

    # Gerar clientes
    clients = build_clients()

    # Inserir tabelas
    print("Inserindo clientes (raw_hubspot_companies)...")
    insert_companies(conn, clients)

    print("Inserindo deals (raw_hubspot_deals)...")
    n_deals = insert_deals(conn, clients)

    print("Inserindo calls (raw_hubspot_calls)...")
    n_calls = insert_calls(conn, clients)

    print("Inserindo NPS (raw_hubspot_nps)...")
    n_nps = insert_nps(conn, clients)

    print("Inserindo campanhas (raw_revi_campaigns)...")
    n_campaigns = insert_campaigns(conn, clients)

    print("Inserindo automacoes (raw_revi_automations)...")
    n_automations = insert_automations(conn, clients)

    print("Inserindo chat (raw_revi_chat)...")
    insert_chat(conn, clients)

    print("Inserindo mensagens mensais (raw_revi_messages_monthly)...")
    insert_messages_monthly(conn, clients)

    print("Inserindo config dos clientes (raw_revi_client_config)...")
    insert_client_config(conn, clients)

    conn.commit()
    conn.close()

    # Resumo por perfil
    profiles_count = {}
    for c in clients:
        profiles_count[c["profile"]] = profiles_count.get(c["profile"], 0) + 1

    print()
    print("=" * 55)
    print("RESUMO DA GERACAO DE DADOS MOCK")
    print("=" * 55)
    print(f"  60 clientes gerados, {n_campaigns} campanhas, {n_calls} calls, {n_automations} automacoes")
    print(f"  Deals: {n_deals}  |  NPS: {n_nps}")
    print(f"  Perfis: power_user={profiles_count.get('power_user',0)}, "
          f"operational={profiles_count.get('operational',0)}, "
          f"at_risk={profiles_count.get('at_risk',0)}")
    print(f"  DB: {DB_PATH}")
    print("=" * 55)


if __name__ == "__main__":
    main()
