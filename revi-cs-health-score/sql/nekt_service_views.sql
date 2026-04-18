-- =============================================================================
-- nekt_service_views.sql
-- 7 service views that power the Revi CS Dashboard (Streamlit).
-- Dialect: Amazon Athena / Trino. Not compatible with SQLite/Postgres.
-- Refresh: run in order (dim_clients first; the fct_* views depend on it).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. dim_clients — canonical client dimension
--    Joins APPDATA companies + HubSpot companies + HubSpot owners + CAS.
--    Dedupes companies_activity_summary (has duplicate ids).
--    Bridges APPDATA and HubSpot via gupshup_app_name.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.dim_clients AS
WITH cas_dedup AS (
    SELECT
        id,
        gupshup_app_name,
        has_customer_service,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM nekt_service.companies_activity_summary
),
owners AS (
    SELECT
        id AS owner_id,
        TRIM(CONCAT(COALESCE(firstname, ''), ' ', COALESCE(lastname, ''))) AS owner_name,
        email AS owner_email
    FROM nekt_raw.hubnew_owners
),
hc AS (
    SELECT
        id AS hubspot_company_id,
        properties.gupshupapp                        AS gupshup_app_name_hs,
        properties.name                              AS hs_company_name,
        properties.mrr_fixo                          AS mrr_fixo_raw,
        properties.cs_responsavel                    AS cs_responsavel_id,
        properties.cx_responsavel                    AS cx_responsavel_id,
        properties.hubspot_owner_id                  AS hubspot_owner_id,
        properties.createdate                        AS hs_createdate,
        properties.closedate                         AS hs_closedate,
        properties.data_do_churn                     AS churn_date_raw,
        properties.valor_do_churn                    AS valor_do_churn_raw,
        properties.lifecyclestage                    AS lifecyclestage,
        properties.onboarding_start_date             AS onboarding_start_raw,
        properties.onboarding_end_date               AS onboarding_end_raw,
        properties.motivo_de_churn                   AS motivo_de_churn,
        properties.razao_do_churn                    AS razao_do_churn,
        properties.consultoria_estrategica_contratada AS consultoria_estrategica,
        properties.data_da_ultima_reuniao_cs         AS data_ultima_reuniao_cs_raw,
        properties.hs_csm_sentiment                  AS csm_sentiment,
        properties.faturamento_mensal_estimado       AS faturamento_mensal_estimado
    FROM nekt_raw.hubnew_companies
)
SELECT
    ac.id                                            AS client_id,
    ac.name                                          AS company_name,
    ac.gupshup_app_name,
    hc.hubspot_company_id,
    TRY_CAST(hc.mrr_fixo_raw AS DOUBLE)              AS mrr,
    COALESCE(cs_owner.owner_name, cx_owner.owner_name, hs_owner.owner_name) AS csm_owner,
    COALESCE(hc.cs_responsavel_id, hc.cx_responsavel_id, hc.hubspot_owner_id) AS csm_owner_id,
    hc.lifecyclestage,
    ac.business_sector,
    ac.is_active,
    cas.has_customer_service,
    hc.consultoria_estrategica,
    hc.csm_sentiment,
    TRY_CAST(hc.faturamento_mensal_estimado AS DOUBLE) AS faturamento_mensal_estimado,
    CAST(TRY(FROM_ISO8601_TIMESTAMP(hc.hs_createdate)) AS TIMESTAMP) AS hs_createdate,
    CAST(TRY(FROM_ISO8601_TIMESTAMP(hc.hs_closedate)) AS TIMESTAMP)  AS contract_start_date,
    TRY_CAST(hc.churn_date_raw AS DATE)              AS churn_date,
    TRY_CAST(hc.valor_do_churn_raw AS DOUBLE)        AS valor_do_churn,
    CAST(TRY(FROM_ISO8601_TIMESTAMP(hc.onboarding_start_raw)) AS TIMESTAMP) AS onboarding_start_date,
    CAST(TRY(FROM_ISO8601_TIMESTAMP(hc.onboarding_end_raw))   AS TIMESTAMP) AS onboarding_end_date,
    CAST(TRY(FROM_ISO8601_TIMESTAMP(hc.data_ultima_reuniao_cs_raw)) AS TIMESTAMP) AS data_ultima_reuniao_cs,
    hc.motivo_de_churn,
    hc.razao_do_churn,
    DATE_DIFF('day', CURRENT_DATE,
        DATE_ADD('year', 1, CAST(TRY(FROM_ISO8601_TIMESTAMP(hc.hs_closedate)) AS DATE))
    ) AS days_to_renewal
FROM nekt_raw.app_postgres_public_companies ac
LEFT JOIN cas_dedup cas    ON cas.id = ac.id AND cas.rn = 1
LEFT JOIN hc               ON hc.gupshup_app_name_hs = ac.gupshup_app_name
LEFT JOIN owners cs_owner  ON cs_owner.owner_id = hc.cs_responsavel_id
LEFT JOIN owners cx_owner  ON cx_owner.owner_id = hc.cx_responsavel_id
LEFT JOIN owners hs_owner  ON hs_owner.owner_id = hc.hubspot_owner_id;


-- -----------------------------------------------------------------------------
-- 2. fct_health_score — 6 scores + cashback bonus per client
--    Sources: dim_clients, whatsapp_campaign_results, automation_execution_summary,
--             flow_execution_summary, app_postgres_public_users (AI flag),
--             cashback_configs, companies_activity_summary (messages).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.fct_health_score AS
WITH cas AS (
    SELECT
        id, gupshup_app_name, has_customer_service,
        total_active_automations, total_active_flows, active_integrations,
        total_messages_this_month, total_messages_prev_month,
        whatsapp_marketing_package_limit,
        last_campaign_at, first_campaign_at,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM nekt_service.companies_activity_summary
),
ranked_campaigns AS (
    SELECT
        company_id, campaign_roi, whatsapp_campaign_sent_at,
        ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY whatsapp_campaign_sent_at DESC) AS rn
    FROM nekt_service.whatsapp_campaign_results
    WHERE whatsapp_campaign_sent_at IS NOT NULL
),
campaigns AS (
    SELECT
        company_id,
        MAX(whatsapp_campaign_sent_at) AS last_campaign_at,
        AVG(campaign_roi) FILTER (WHERE rn <= 3) AS campaign_roi_last3
    FROM ranked_campaigns
    GROUP BY company_id
),
autos AS (
    SELECT
        company_id,
        COUNT(*) FILTER (WHERE automation_is_active AND automation_type  = 'Personalizada') AS active_automations,
        COUNT(*) FILTER (WHERE automation_is_active AND automation_type <> 'Personalizada') AS integration_automations
    FROM nekt_service.automation_execution_summary
    GROUP BY company_id
),
flows AS (
    SELECT company_id, COUNT(*) FILTER (WHERE flow_is_active) AS active_flows
    FROM nekt_service.flow_execution_summary
    GROUP BY company_id
),
ai AS (
    -- has_ai = exists at least one user bound to an ai_agent (is_ai_agent flag is never true).
    SELECT DISTINCT company_id
    FROM nekt_raw.app_postgres_public_users
    WHERE ai_agent_id IS NOT NULL AND ai_agent_id <> ''
),
cashback AS (
    SELECT DISTINCT company_id
    FROM nekt_raw.app_postgres_public_cashback_configs
    WHERE is_active = TRUE
),
assembled AS (
    SELECT
        dc.client_id, dc.company_name, dc.csm_owner, dc.mrr, dc.days_to_renewal,
        COALESCE(DATE_DIFF('day', CAST(c.last_campaign_at AS DATE), CURRENT_DATE), 9999) AS days_since_last_campaign,
        COALESCE(c.campaign_roi_last3, 0) AS campaign_roi,
        COALESCE(a.active_automations, 0) AS active_automations,
        COALESCE(a.integration_automations, 0) AS integration_automations,
        COALESCE(f.active_flows, 0) AS active_flows,
        COALESCE(cas.has_customer_service, FALSE) AS has_human_agent,
        (COALESCE(f.active_flows, 0) > 0) AS has_chat_flow,
        (ai.company_id IS NOT NULL) AS has_ai_enabled,
        CASE
            WHEN COALESCE(cas.has_customer_service, FALSE)
             AND (COALESCE(f.active_flows, 0) > 0 OR ai.company_id IS NOT NULL) THEN 'advanced'
            WHEN COALESCE(cas.has_customer_service, FALSE) OR COALESCE(f.active_flows, 0) > 0 THEN 'essential'
            ELSE 'none'
        END AS chat_usage_level,
        COALESCE(cas.total_messages_this_month, 0) AS messages_sent_current,
        COALESCE(cas.total_messages_prev_month, 0) AS messages_sent_previous,
        COALESCE(cas.whatsapp_marketing_package_limit, 0) AS plan_limit_current,
        CASE
            WHEN COALESCE(cas.total_messages_prev_month, 0) > 0
            THEN ROUND((COALESCE(cas.total_messages_this_month, 0) - cas.total_messages_prev_month) * 100.0 / cas.total_messages_prev_month, 2)
        END AS messages_mom_change,
        CASE
            WHEN COALESCE(cas.whatsapp_marketing_package_limit, 0) > 0
            THEN ROUND(COALESCE(cas.total_messages_this_month, 0) * 100.0 / cas.whatsapp_marketing_package_limit, 2)
        END AS plan_usage_pct,
        CASE WHEN cb.company_id IS NOT NULL THEN 1 ELSE 0 END AS cashback_enabled
    FROM nekt_service.dim_clients dc
    LEFT JOIN cas       ON cas.id = dc.client_id AND cas.rn = 1
    LEFT JOIN campaigns c  ON c.company_id = dc.client_id
    LEFT JOIN autos     a  ON a.company_id = dc.client_id
    LEFT JOIN flows     f  ON f.company_id = dc.client_id
    LEFT JOIN ai           ON ai.company_id = dc.client_id
    LEFT JOIN cashback  cb ON cb.company_id = dc.client_id
),
scored AS (
    SELECT *,
        CASE WHEN days_since_last_campaign <= 7  THEN 5
             WHEN days_since_last_campaign <= 20 THEN 3 ELSE 0 END AS score_recency,
        CASE WHEN campaign_roi >= 10 THEN 5
             WHEN campaign_roi >= 1  THEN 3 ELSE 0 END AS score_roi,
        CASE WHEN active_automations >= 3 THEN 5
             WHEN active_automations >= 1 THEN 3 ELSE 0 END AS score_automations,
        CASE WHEN integration_automations >= 4 THEN 5
             WHEN integration_automations  = 3 THEN 4
             WHEN integration_automations >= 1 THEN 2 ELSE 0 END AS score_integrations,
        CASE WHEN chat_usage_level = 'advanced'  THEN 5
             WHEN chat_usage_level = 'essential' THEN 4 ELSE 0 END AS score_chat,
        CASE WHEN messages_mom_change > 0 OR plan_usage_pct >= 80 THEN 5
             WHEN messages_mom_change >= -10 THEN 3 ELSE 0 END AS score_volume,
        CASE WHEN cashback_enabled = 1 THEN 2 ELSE 0 END AS bonus_cashback
    FROM assembled
)
SELECT
    client_id, company_name, csm_owner, mrr, days_to_renewal,
    CURRENT_DATE AS calculated_at,
    score_recency, score_roi, score_automations, score_integrations,
    score_chat, score_volume, bonus_cashback,
    days_since_last_campaign, campaign_roi,
    active_automations, integration_automations, active_flows,
    chat_usage_level, has_human_agent, has_chat_flow, has_ai_enabled,
    messages_sent_current, messages_sent_previous,
    messages_mom_change, plan_usage_pct, cashback_enabled,
    (score_recency + score_roi + score_automations + score_integrations
     + score_chat + score_volume + bonus_cashback) AS total_score,
    CASE
        WHEN (score_recency + score_roi + score_automations + score_integrations
              + score_chat + score_volume + bonus_cashback) >= 26 THEN 'green'
        WHEN (score_recency + score_roi + score_automations + score_integrations
              + score_chat + score_volume + bonus_cashback) >= 16 THEN 'yellow'
        ELSE 'red'
    END AS health_status
FROM scored;


-- -----------------------------------------------------------------------------
-- 3. fct_alerts — append-only alerts produced from the current health snapshot.
--    Status-change alerts (score_dropped_to_red / roi_dropped_below_1) require
--    history and are NOT emitted here — add them once we persist daily snapshots.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.fct_alerts AS
WITH base AS (
    SELECT
        client_id, company_name, csm_owner, health_status, total_score,
        days_since_last_campaign, campaign_roi, messages_mom_change,
        plan_usage_pct, days_to_renewal
    FROM nekt_service.fct_health_score
)
SELECT * FROM (
    SELECT client_id, company_name, csm_owner,
        'no_campaign_14d' AS alert_id,
        'Sem campanha ha 14+ dias' AS alert_name,
        'warning' AS severity,
        'Notificar CSM para agendar consultoria' AS action_suggested,
        CAST(days_since_last_campaign AS VARCHAR) AS alert_metric,
        CURRENT_DATE AS alert_date, FALSE AS resolved
    FROM base
    WHERE days_since_last_campaign >= 14 AND days_since_last_campaign < 9999

    UNION ALL SELECT client_id, company_name, csm_owner,
        'roi_below_1', 'ROI abaixo de 1x', 'warning',
        'Agendar analise de campanha com cliente',
        CAST(CAST(campaign_roi AS DECIMAL(10,2)) AS VARCHAR),
        CURRENT_DATE, FALSE
    FROM base WHERE campaign_roi < 1 AND campaign_roi > 0

    UNION ALL SELECT client_id, company_name, csm_owner,
        'renewal_90d', 'Renovacao em 90 dias', 'info',
        'Iniciar processo de reaplicacao SPICE',
        CAST(days_to_renewal AS VARCHAR),
        CURRENT_DATE, FALSE
    FROM base WHERE days_to_renewal <= 90 AND days_to_renewal >= 31

    UNION ALL SELECT client_id, company_name, csm_owner,
        'renewal_30d_red', 'Renovacao em 30 dias + Score Vermelho', 'critical',
        'Escalar para gestao - risco alto de churn',
        CAST(days_to_renewal AS VARCHAR),
        CURRENT_DATE, FALSE
    FROM base WHERE days_to_renewal <= 30 AND days_to_renewal >= 0 AND health_status = 'red'

    UNION ALL SELECT client_id, company_name, csm_owner,
        'volume_drop_30pct', 'Queda de volume >30%', 'warning',
        'Investigar causa e agendar call',
        CAST(CAST(messages_mom_change AS DECIMAL(10,2)) AS VARCHAR),
        CURRENT_DATE, FALSE
    FROM base WHERE messages_mom_change <= -30

    UNION ALL SELECT client_id, company_name, csm_owner,
        'upsell_opportunity', 'Oportunidade de upsell', 'info',
        'Qualificar como SQL de expansao',
        CAST(CAST(plan_usage_pct AS DECIMAL(10,2)) AS VARCHAR),
        CURRENT_DATE, FALSE
    FROM base WHERE health_status = 'green' AND plan_usage_pct >= 90

    UNION ALL SELECT client_id, company_name, csm_owner,
        'score_red', 'Score em Vermelho', 'critical',
        'Criar task urgente para o CSM',
        CAST(total_score AS VARCHAR),
        CURRENT_DATE, FALSE
    FROM base WHERE health_status = 'red'
);


-- -----------------------------------------------------------------------------
-- 4. fct_upsell_flags — 7 product flags per client + upsell_opportunities_count.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.fct_upsell_flags AS
WITH cas AS (
    SELECT id, total_active_flows, total_active_automations, active_integrations,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC) AS rn
    FROM nekt_service.companies_activity_summary
),
autos AS (
    SELECT
        company_id,
        COUNT(*) FILTER (WHERE automation_is_active AND automation_type  = 'Personalizada') AS std_autos,
        COUNT(*) FILTER (WHERE automation_is_active AND automation_type <> 'Personalizada') AS integ_autos
    FROM nekt_service.automation_execution_summary
    GROUP BY company_id
),
sms   AS (SELECT DISTINCT company_id FROM nekt_raw.app_postgres_public_sms_campaigns),
email AS (SELECT DISTINCT company_id FROM nekt_raw.app_postgres_public_email_campaigns),
ai    AS (SELECT DISTINCT company_id FROM nekt_raw.app_postgres_public_users
          WHERE ai_agent_id IS NOT NULL AND ai_agent_id <> ''),
cb    AS (SELECT DISTINCT company_id FROM nekt_raw.app_postgres_public_cashback_configs
          WHERE is_active = TRUE)
SELECT
    dc.client_id, dc.company_name, dc.csm_owner, dc.mrr,
    (COALESCE(cas.total_active_flows, 0) > 0) AS has_chat_flow,
    (sms.company_id   IS NOT NULL)            AS has_sms,
    (email.company_id IS NOT NULL)            AS has_email,
    (ai.company_id    IS NOT NULL)            AS has_ai_enabled,
    (COALESCE(a.integ_autos, 0) >= 2)         AS has_integration_automations,
    (COALESCE(a.std_autos,   0) > 0)          AS has_active_automations,
    (cb.company_id    IS NOT NULL)            AS cashback_enabled,
    COALESCE(a.std_autos,   0) AS std_autos_count,
    COALESCE(a.integ_autos, 0) AS integ_autos_count,
    COALESCE(cas.total_active_flows, 0) AS active_flows_count,
    CARDINALITY(COALESCE(cas.active_integrations, ARRAY[])) AS ecommerce_integrations_count,
    ((CASE WHEN COALESCE(cas.total_active_flows, 0) > 0 THEN 0 ELSE 1 END)
   + (CASE WHEN sms.company_id   IS NOT NULL          THEN 0 ELSE 1 END)
   + (CASE WHEN email.company_id IS NOT NULL          THEN 0 ELSE 1 END)
   + (CASE WHEN ai.company_id    IS NOT NULL          THEN 0 ELSE 1 END)
   + (CASE WHEN COALESCE(a.integ_autos, 0) >= 2       THEN 0 ELSE 1 END)
   + (CASE WHEN COALESCE(a.std_autos,   0) >  0       THEN 0 ELSE 1 END)
   + (CASE WHEN cb.company_id    IS NOT NULL          THEN 0 ELSE 1 END)
    ) AS upsell_opportunities_count
FROM nekt_service.dim_clients dc
LEFT JOIN cas   ON cas.id = dc.client_id AND cas.rn = 1
LEFT JOIN autos a   ON a.company_id     = dc.client_id
LEFT JOIN sms       ON sms.company_id   = dc.client_id
LEFT JOIN email     ON email.company_id = dc.client_id
LEFT JOIN ai        ON ai.company_id    = dc.client_id
LEFT JOIN cb        ON cb.company_id    = dc.client_id;


-- -----------------------------------------------------------------------------
-- 5. fct_csm_activity_weekly — meetings + calls per CSM per week.
--    Both sources canonicalised on lowercase email; name enriched at the end.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.fct_csm_activity_weekly AS
WITH owners_lookup AS (
    SELECT
        id AS owner_id,
        email AS owner_email,
        TRIM(CONCAT(COALESCE(firstname, ''), ' ', COALESCE(lastname, ''))) AS owner_name
    FROM nekt_raw.hubnew_owners
),
meeting_base AS (
    SELECT
        LOWER(organizer) AS csm_email,
        CAST(DATE_TRUNC('week', start_date) AS DATE) AS week_start,
        id AS meeting_id,
        meeting_duration_minutes
    FROM nekt_trusted.customer_success_meetings
    WHERE start_date IS NOT NULL AND organizer IS NOT NULL
),
meeting_agg AS (
    SELECT
        csm_email, week_start,
        COUNT(DISTINCT meeting_id) AS meetings_total,
        SUM(COALESCE(meeting_duration_minutes, 0)) AS meeting_duration_minutes
    FROM meeting_base
    GROUP BY 1, 2
),
call_base AS (
    SELECT
        LOWER(o.owner_email) AS csm_email,
        CAST(DATE_TRUNC('week', TRY(FROM_ISO8601_TIMESTAMP(c.properties.hs_timestamp))) AS DATE) AS week_start,
        c.id AS call_id,
        TRY_CAST(c.properties.hs_call_duration AS BIGINT) AS call_duration_ms
    FROM nekt_raw.hubspot_calls c
    INNER JOIN owners_lookup o ON o.owner_id = c.properties.hubspot_owner_id
    WHERE c.properties.hs_timestamp IS NOT NULL
),
call_agg AS (
    SELECT
        csm_email, week_start,
        COUNT(DISTINCT call_id) AS calls_total,
        CAST(SUM(COALESCE(call_duration_ms, 0)) / 60000 AS BIGINT) AS calls_duration_minutes
    FROM call_base
    WHERE csm_email IS NOT NULL AND week_start IS NOT NULL
    GROUP BY 1, 2
),
joined AS (
    SELECT
        COALESCE(m.csm_email,  c.csm_email)  AS csm_email,
        COALESCE(m.week_start, c.week_start) AS week_start,
        COALESCE(m.meetings_total, 0)           AS meetings_total,
        COALESCE(c.calls_total, 0)              AS calls_total,
        COALESCE(m.meeting_duration_minutes, 0) AS meeting_duration_minutes,
        COALESCE(c.calls_duration_minutes, 0)   AS calls_duration_minutes
    FROM meeting_agg m
    FULL JOIN call_agg c ON m.csm_email = c.csm_email AND m.week_start = c.week_start
)
SELECT
    COALESCE(o.owner_name, j.csm_email) AS csm_owner,
    j.csm_email,
    j.week_start,
    j.meetings_total,
    j.calls_total,
    (j.meetings_total + j.calls_total)                      AS activities_total,
    (j.meeting_duration_minutes + j.calls_duration_minutes) AS total_duration_minutes
FROM joined j
LEFT JOIN owners_lookup o ON LOWER(o.owner_email) = j.csm_email;


-- -----------------------------------------------------------------------------
-- 6. fct_coverage_monthly — granular client-facing meetings per CSM per month.
--    Convention: meeting summary follows 'CSM <> Client' pattern.
--    Dashboard joins dim_clients on client_name_slug for portfolio coverage %.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.fct_coverage_monthly AS
WITH meeting_attendees AS (
    SELECT
        m.id AS meeting_id,
        LOWER(m.organizer) AS csm_email,
        m.start_date,
        m.summary,
        LOWER(att) AS attendee_email
    FROM nekt_trusted.customer_success_meetings m
    CROSS JOIN UNNEST(m.attendees) AS t(att)
    WHERE m.start_date IS NOT NULL
      AND m.organizer  IS NOT NULL
      AND m.summary LIKE '%<>%'
),
client_facing AS (
    SELECT DISTINCT meeting_id, csm_email, start_date, summary
    FROM meeting_attendees
    WHERE attendee_email NOT LIKE '%@userevi.com'
      AND attendee_email <> ''
),
owners_lookup AS (
    SELECT
        LOWER(email) AS owner_email,
        TRIM(CONCAT(COALESCE(firstname, ''), ' ', COALESCE(lastname, ''))) AS owner_name
    FROM nekt_raw.hubnew_owners
)
SELECT
    ol.owner_name AS csm_owner,
    DATE_FORMAT(cf.start_date, '%Y-%m')              AS year_month,
    CAST(DATE_TRUNC('month', cf.start_date) AS DATE) AS month_start,
    LOWER(TRIM(SPLIT_PART(cf.summary, '<>', 1)))     AS client_name_slug,
    COUNT(DISTINCT cf.meeting_id)                     AS meetings_count
FROM client_facing cf
INNER JOIN owners_lookup ol ON ol.owner_email = cf.csm_email
GROUP BY 1, 2, 3, 4;


-- -----------------------------------------------------------------------------
-- 7. fct_revenue_retention_monthly — MRR waterfall (GRR/NRR) per month.
--    Built from hubnew_companies.mrr_fixo + data_do_churn + valor_do_churn
--    (no deals<>companies association table exists in Nekt).
--    downgrade / upsell / cross_sell hardcoded to 0 (operation not classifying).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW nekt_service.fct_revenue_retention_monthly AS
WITH months AS (
    SELECT DATE_ADD('month', seq.s, DATE '2024-01-01') AS month_start
    FROM UNNEST(SEQUENCE(0, 30)) AS seq(s)
),
companies AS (
    SELECT
        id,
        properties.name AS company_name,
        TRY_CAST(properties.mrr_fixo AS DOUBLE) AS mrr,
        CAST(TRY(FROM_ISO8601_TIMESTAMP(properties.closedate)) AS DATE) AS contract_start_date,
        TRY_CAST(properties.data_do_churn  AS DATE)   AS churn_date,
        TRY_CAST(properties.valor_do_churn AS DOUBLE) AS churn_value
    FROM nekt_raw.hubnew_companies
    WHERE properties.mrr_fixo IS NOT NULL AND properties.mrr_fixo <> ''
),
monthly_buckets AS (
    SELECT
        DATE_FORMAT(m.month_start, '%Y-%m') AS year_month,
        m.month_start,
        COALESCE(SUM(CASE
            WHEN c.contract_start_date IS NOT NULL
             AND c.contract_start_date < m.month_start
             AND (c.churn_date IS NULL OR c.churn_date >= m.month_start)
            THEN c.mrr ELSE 0 END), 0) AS mrr_start,
        COALESCE(SUM(CASE
            WHEN c.contract_start_date IS NOT NULL
             AND DATE_TRUNC('month', c.contract_start_date) = m.month_start
            THEN c.mrr ELSE 0 END), 0) AS mrr_new,
        COALESCE(SUM(CASE
            WHEN c.churn_date IS NOT NULL
             AND DATE_TRUNC('month', c.churn_date) = m.month_start
            THEN COALESCE(c.churn_value, c.mrr) ELSE 0 END), 0) AS mrr_churn
    FROM months m
    LEFT JOIN companies c ON TRUE
    GROUP BY m.month_start
)
SELECT
    year_month,
    month_start,
    mrr_start,
    0.0 AS mrr_downgrade,
    0.0 AS mrr_upsell,
    0.0 AS mrr_cross_sell,
    mrr_new,
    mrr_churn,
    (mrr_start - mrr_churn + mrr_new) AS mrr_end,
    CASE WHEN mrr_start > 0 THEN ROUND((mrr_start - mrr_churn) * 100.0 / mrr_start, 2) END AS grr,
    CASE WHEN mrr_start > 0 THEN ROUND((mrr_start - mrr_churn) * 100.0 / mrr_start, 2) END AS nrr
FROM monthly_buckets
WHERE month_start <= CURRENT_DATE
ORDER BY month_start;
