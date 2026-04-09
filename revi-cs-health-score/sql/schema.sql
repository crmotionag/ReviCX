-- =============================================================================
-- SCHEMA: ReviCX Health Score
-- Ambiente: SQLite (mock) -> Nekt (producao)
-- =============================================================================

-- FONTES RAW (simulam o que vira do HubSpot e da ReviCX via Nekt)

-- Fonte: HubSpot > Companies
CREATE TABLE IF NOT EXISTS raw_hubspot_companies (
    hubspot_company_id TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    segment_ipc TEXT,
    csm_owner TEXT,
    has_cs BOOLEAN DEFAULT 0,
    contract_start_date DATE,
    contract_end_date DATE,
    mrr REAL DEFAULT 0,
    revi_client_id TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Fonte: HubSpot > Deals
CREATE TABLE IF NOT EXISTS raw_hubspot_deals (
    deal_id TEXT PRIMARY KEY,
    hubspot_company_id TEXT,
    deal_type TEXT,
    mrr_before REAL,
    mrr_after REAL,
    mrr_delta REAL,
    closed_date DATE,
    deal_stage TEXT,
    FOREIGN KEY (hubspot_company_id) REFERENCES raw_hubspot_companies(hubspot_company_id)
);

-- Fonte: HubSpot > Engagements (calls do CSM)
CREATE TABLE IF NOT EXISTS raw_hubspot_calls (
    call_id TEXT PRIMARY KEY,
    hubspot_company_id TEXT,
    csm_owner TEXT,
    call_date DATE,
    duration_minutes INTEGER,
    call_type TEXT,
    notes TEXT,
    FOREIGN KEY (hubspot_company_id) REFERENCES raw_hubspot_companies(hubspot_company_id)
);

-- Fonte: HubSpot > NPS
CREATE TABLE IF NOT EXISTS raw_hubspot_nps (
    response_id TEXT PRIMARY KEY,
    hubspot_company_id TEXT,
    contact_email TEXT,
    nps_score INTEGER,
    nps_category TEXT,
    response_date DATE,
    FOREIGN KEY (hubspot_company_id) REFERENCES raw_hubspot_companies(hubspot_company_id)
);

-- Fonte: ReviCX > Campanhas
CREATE TABLE IF NOT EXISTS raw_revi_campaigns (
    campaign_id TEXT PRIMARY KEY,
    revi_client_id TEXT NOT NULL,
    campaign_name TEXT,
    sent_at DATETIME,
    recipients_count INTEGER,
    messages_sent INTEGER,
    revenue_attributed REAL DEFAULT 0,
    cost REAL DEFAULT 0,
    campaign_type TEXT
);

-- Fonte: ReviCX > Automacoes
CREATE TABLE IF NOT EXISTS raw_revi_automations (
    automation_id TEXT PRIMARY KEY,
    revi_client_id TEXT NOT NULL,
    automation_name TEXT,
    automation_type TEXT,
    status TEXT,
    created_at DATETIME,
    last_triggered_at DATETIME
);

-- Fonte: ReviCX > Modulo de Atendimento
CREATE TABLE IF NOT EXISTS raw_revi_chat (
    revi_client_id TEXT PRIMARY KEY,
    has_human_agent BOOLEAN DEFAULT 0,
    human_agents_count INTEGER DEFAULT 0,
    has_chat_flow BOOLEAN DEFAULT 0,
    has_ai_enabled BOOLEAN DEFAULT 0,
    last_conversation_at DATETIME
);

-- Fonte: ReviCX > Mensagens (agregado mensal)
CREATE TABLE IF NOT EXISTS raw_revi_messages_monthly (
    revi_client_id TEXT,
    year_month TEXT,
    messages_sent INTEGER,
    plan_limit INTEGER,
    PRIMARY KEY (revi_client_id, year_month)
);

-- Fonte: ReviCX > Config do cliente
CREATE TABLE IF NOT EXISTS raw_revi_client_config (
    revi_client_id TEXT PRIMARY KEY,
    plan_type TEXT,
    cashback_enabled BOOLEAN DEFAULT 0,
    onboarding_completed BOOLEAN DEFAULT 0,
    onboarding_completed_at DATE
);

-- =============================================================================
-- TABELAS MODELADAS
-- =============================================================================

-- Dimensao: Clientes (join HubSpot + ReviCX)
CREATE TABLE IF NOT EXISTS dim_clients (
    client_id TEXT PRIMARY KEY,
    hubspot_company_id TEXT,
    company_name TEXT,
    segment_ipc TEXT,
    csm_owner TEXT,
    has_cs BOOLEAN,
    contract_start_date DATE,
    contract_end_date DATE,
    mrr REAL,
    plan_type TEXT,
    cashback_enabled BOOLEAN,
    onboarding_completed BOOLEAN,
    days_as_client INTEGER,
    days_to_renewal INTEGER
);

-- Fato: Health Score (APPEND-ONLY)
CREATE TABLE IF NOT EXISTS fct_health_score (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    calculated_at DATE NOT NULL,
    score_recency INTEGER DEFAULT 0,
    score_roi INTEGER DEFAULT 0,
    score_automations INTEGER DEFAULT 0,
    score_integrations INTEGER DEFAULT 0,
    score_chat INTEGER DEFAULT 0,
    score_volume INTEGER DEFAULT 0,
    bonus_cashback INTEGER DEFAULT 0,
    days_since_last_campaign INTEGER,
    campaign_roi REAL,
    active_automations INTEGER,
    integration_automations INTEGER,
    chat_usage_level TEXT,
    messages_sent_current INTEGER,
    messages_sent_previous INTEGER,
    messages_mom_change REAL,
    plan_usage_pct REAL,
    total_score INTEGER NOT NULL,
    health_status TEXT NOT NULL,
    UNIQUE(client_id, calculated_at)
);

-- Fato: Alertas gerados
CREATE TABLE IF NOT EXISTS fct_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    alert_date DATE NOT NULL,
    alert_id TEXT NOT NULL,
    alert_name TEXT,
    severity TEXT,
    action_suggested TEXT,
    resolved BOOLEAN DEFAULT 0,
    resolved_at DATETIME,
    resolved_by TEXT
);

-- Fato: Atividades do CSM (agregado semanal)
CREATE TABLE IF NOT EXISTS fct_csm_activity_weekly (
    csm_owner TEXT,
    week_start DATE,
    calls_total INTEGER DEFAULT 0,
    calls_consultoria INTEGER DEFAULT 0,
    calls_onboarding INTEGER DEFAULT 0,
    calls_urgencia INTEGER DEFAULT 0,
    calls_follow_up INTEGER DEFAULT 0,
    unique_clients_contacted INTEGER DEFAULT 0,
    total_duration_minutes INTEGER DEFAULT 0,
    PRIMARY KEY (csm_owner, week_start)
);

-- Fato: Cobertura de carteira (mensal)
CREATE TABLE IF NOT EXISTS fct_coverage_monthly (
    csm_owner TEXT,
    year_month TEXT,
    clients_with_cs_total INTEGER,
    clients_with_cs_contacted INTEGER,
    clients_without_cs_total INTEGER,
    clients_without_cs_contacted INTEGER,
    coverage_with_cs_pct REAL,
    coverage_without_cs_pct REAL,
    sqls_identified INTEGER DEFAULT 0,
    PRIMARY KEY (csm_owner, year_month)
);

-- Fato: GRR e NRR (mensal)
CREATE TABLE IF NOT EXISTS fct_revenue_retention_monthly (
    year_month TEXT PRIMARY KEY,
    mrr_start REAL,
    mrr_churn REAL,
    mrr_downgrade REAL,
    mrr_upsell REAL,
    mrr_cross_sell REAL,
    mrr_new REAL,
    mrr_end REAL,
    grr REAL,
    nrr REAL
);

-- =============================================================================
-- AUTENTICACAO E CONTROLE DE ACESSO
-- =============================================================================

CREATE TABLE IF NOT EXISTS app_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    area TEXT NOT NULL,
    role TEXT NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    is_admin BOOLEAN DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login DATETIME
);
