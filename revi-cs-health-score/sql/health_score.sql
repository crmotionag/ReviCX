-- =============================================================================
-- health_score.sql
-- Calculates Health Score for all active clients from raw tables.
-- Compatible: SQLite (mock) and PostgreSQL / Nekt (production).
-- Reference date : 2026-04-08
-- Current month  : 2026-04
-- Previous month : 2026-03
-- =============================================================================

WITH

-- ---------------------------------------------------------------------------
-- 1. Last campaign date and average ROI for the last 3 campaigns per client
-- ---------------------------------------------------------------------------
campaigns AS (
    SELECT
        revi_client_id,
        MAX(sent_at)                                        AS last_campaign_at,
        CAST(
            julianday(date('2026-04-08')) -
            julianday(date(MAX(sent_at)))
        AS INTEGER)                                         AS days_since_last_campaign,

        -- ROI = revenue_attributed / cost averaged over last 3 campaigns
        -- We rank per client and keep only rank <= 3
        AVG(roi_per_campaign)                               AS campaign_roi

    FROM (
        SELECT
            revi_client_id,
            sent_at,
            CASE
                WHEN cost > 0 THEN revenue_attributed / cost
                ELSE 0
            END                                             AS roi_per_campaign,
            ROW_NUMBER() OVER (
                PARTITION BY revi_client_id
                ORDER BY sent_at DESC
            )                                               AS rn
        FROM raw_revi_campaigns
    ) ranked
    WHERE rn <= 3
    GROUP BY revi_client_id
),

-- ---------------------------------------------------------------------------
-- 2. Active automations split by type (standard vs. integration)
-- ---------------------------------------------------------------------------
automations AS (
    SELECT
        revi_client_id,
        COUNT(CASE WHEN status = 'active' AND automation_type != 'integration' THEN 1 END)
            AS active_automations,
        COUNT(CASE WHEN status = 'active' AND automation_type  = 'integration' THEN 1 END)
            AS integration_automations
    FROM raw_revi_automations
    GROUP BY revi_client_id
),

-- ---------------------------------------------------------------------------
-- 3. Chat usage level
-- ---------------------------------------------------------------------------
chat AS (
    SELECT
        revi_client_id,
        has_human_agent,
        has_chat_flow,
        has_ai_enabled,
        CASE
            -- advanced: human AND (flow OR ai)
            WHEN has_human_agent = 1 AND (has_chat_flow = 1 OR has_ai_enabled = 1)
                THEN 'advanced'
            -- essential: human OR flow (but not both conditions above)
            WHEN has_human_agent = 1 OR has_chat_flow = 1
                THEN 'essential'
            ELSE 'none'
        END                                                 AS chat_usage_level
    FROM raw_revi_chat
),

-- ---------------------------------------------------------------------------
-- 4. Monthly message volume: current and previous month
-- ---------------------------------------------------------------------------
messages AS (
    SELECT
        revi_client_id,
        MAX(CASE WHEN year_month = '2026-04' THEN messages_sent  END) AS messages_sent_current,
        MAX(CASE WHEN year_month = '2026-04' THEN plan_limit     END) AS plan_limit_current,
        MAX(CASE WHEN year_month = '2026-03' THEN messages_sent  END) AS messages_sent_previous
    FROM raw_revi_messages_monthly
    WHERE year_month IN ('2026-04', '2026-03')
    GROUP BY revi_client_id
),

-- ---------------------------------------------------------------------------
-- 5. Cashback flag from client config
-- ---------------------------------------------------------------------------
config AS (
    SELECT
        revi_client_id,
        cashback_enabled
    FROM raw_revi_client_config
),

-- ---------------------------------------------------------------------------
-- 6. Assemble all dimensions per client
-- ---------------------------------------------------------------------------
assembled AS (
    SELECT
        dc.client_id,
        dc.company_name,
        dc.segment_ipc,
        dc.csm_owner,
        dc.has_cs,
        dc.contract_start_date,
        dc.contract_end_date,
        dc.mrr,
        dc.plan_type,
        dc.days_to_renewal,

        -- Campaign metrics
        COALESCE(c.days_since_last_campaign, 9999)          AS days_since_last_campaign,
        COALESCE(c.campaign_roi, 0)                         AS campaign_roi,

        -- Automation counts
        COALESCE(a.active_automations, 0)                   AS active_automations,
        COALESCE(a.integration_automations, 0)              AS integration_automations,

        -- Chat
        COALESCE(ch.chat_usage_level, 'none')               AS chat_usage_level,

        -- Volume
        COALESCE(m.messages_sent_current,  0)               AS messages_sent_current,
        COALESCE(m.messages_sent_previous, 0)               AS messages_sent_previous,
        COALESCE(m.plan_limit_current,     0)               AS plan_limit_current,

        -- MoM change % ( (current - previous) / previous * 100 )
        CASE
            WHEN COALESCE(m.messages_sent_previous, 0) > 0
                THEN ROUND(
                    (COALESCE(m.messages_sent_current, 0) - m.messages_sent_previous)
                    * 100.0 / m.messages_sent_previous,
                    2)
            ELSE NULL
        END                                                  AS messages_mom_change,

        -- Plan usage %
        CASE
            WHEN COALESCE(m.plan_limit_current, 0) > 0
                THEN ROUND(
                    COALESCE(m.messages_sent_current, 0) * 100.0 / m.plan_limit_current,
                    2)
            ELSE NULL
        END                                                  AS plan_usage_pct,

        -- Cashback
        COALESCE(cfg.cashback_enabled, 0)                   AS cashback_enabled

    FROM dim_clients dc
    LEFT JOIN campaigns  c   ON c.revi_client_id   = dc.client_id
    LEFT JOIN automations a  ON a.revi_client_id   = dc.client_id
    LEFT JOIN chat        ch ON ch.revi_client_id  = dc.client_id
    LEFT JOIN messages    m  ON m.revi_client_id   = dc.client_id
    LEFT JOIN config      cfg ON cfg.revi_client_id = dc.client_id
),

-- ---------------------------------------------------------------------------
-- 7. Score each dimension
-- ---------------------------------------------------------------------------
scored AS (
    SELECT
        *,

        -- Recency score (days since last campaign)
        CASE
            WHEN days_since_last_campaign <= 7  THEN 5
            WHEN days_since_last_campaign <= 20 THEN 3
            ELSE 0
        END                                                  AS score_recency,

        -- ROI score (average ROI of last 3 campaigns)
        CASE
            WHEN campaign_roi >= 10 THEN 5
            WHEN campaign_roi >= 1  THEN 3
            ELSE 0
        END                                                  AS score_roi,

        -- Standard automations score
        CASE
            WHEN active_automations >= 3 THEN 5
            WHEN active_automations >= 1 THEN 3
            ELSE 0
        END                                                  AS score_automations,

        -- Integration automations score
        CASE
            WHEN integration_automations >= 4 THEN 5
            WHEN integration_automations  = 3 THEN 4
            WHEN integration_automations >= 1 THEN 2
            ELSE 0
        END                                                  AS score_integrations,

        -- Chat score
        CASE
            WHEN chat_usage_level = 'advanced'  THEN 5
            WHEN chat_usage_level = 'essential' THEN 4
            ELSE 0
        END                                                  AS score_chat,

        -- Volume score
        CASE
            -- Growing: MoM > 0 OR usage >= 80 %
            WHEN messages_mom_change > 0
              OR plan_usage_pct >= 80                        THEN 5
            -- Stable: MoM between -10 % and 0 % (inclusive)
            WHEN messages_mom_change >= -10                  THEN 3
            ELSE 0
        END                                                  AS score_volume,

        -- Cashback bonus
        CASE WHEN cashback_enabled = 1 THEN 2 ELSE 0 END    AS bonus_cashback

    FROM assembled
),

-- ---------------------------------------------------------------------------
-- 8. Sum and classify
-- ---------------------------------------------------------------------------
final AS (
    SELECT
        client_id,
        date('2026-04-08')                                  AS calculated_at,

        score_recency,
        score_roi,
        score_automations,
        score_integrations,
        score_chat,
        score_volume,
        bonus_cashback,

        days_since_last_campaign,
        campaign_roi,
        active_automations,
        integration_automations,
        chat_usage_level,
        messages_sent_current,
        messages_sent_previous,
        messages_mom_change,
        plan_usage_pct,

        (score_recency
         + score_roi
         + score_automations
         + score_integrations
         + score_chat
         + score_volume
         + bonus_cashback)                                   AS total_score,

        CASE
            WHEN (score_recency + score_roi + score_automations
                  + score_integrations + score_chat + score_volume
                  + bonus_cashback) >= 26 THEN 'green'
            WHEN (score_recency + score_roi + score_automations
                  + score_integrations + score_chat + score_volume
                  + bonus_cashback) >= 16 THEN 'yellow'
            ELSE 'red'
        END                                                  AS health_status

    FROM scored
)

SELECT * FROM final
ORDER BY total_score DESC, client_id;
