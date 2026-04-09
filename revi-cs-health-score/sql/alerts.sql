-- =============================================================================
-- alerts.sql
-- Generates CS alerts by crossing the latest fct_health_score with dim_clients.
-- Compatible: SQLite (mock) and PostgreSQL / Nekt (production).
-- Reference date: current_date (resolved at runtime)
-- =============================================================================

WITH

-- ---------------------------------------------------------------------------
-- 1. Latest health score snapshot per client
-- ---------------------------------------------------------------------------
latest_score AS (
    SELECT
        fhs.*,
        -- Previous health status using LAG ordered by calculated_at
        LAG(fhs.health_status) OVER (
            PARTITION BY fhs.client_id
            ORDER BY fhs.calculated_at
        )                                                   AS prev_health_status
    FROM fct_health_score fhs
    WHERE fhs.calculated_at = (
        SELECT MAX(calculated_at)
        FROM fct_health_score
        WHERE client_id = fhs.client_id
    )
),

-- ---------------------------------------------------------------------------
-- 2. Join with dim_clients for company metadata
-- ---------------------------------------------------------------------------
base AS (
    SELECT
        dc.client_id,
        dc.company_name,
        dc.days_to_renewal,
        ls.health_status,
        ls.prev_health_status,
        ls.days_since_last_campaign,
        ls.campaign_roi,
        ls.messages_mom_change,
        ls.plan_usage_pct,
        ls.calculated_at
    FROM dim_clients dc
    INNER JOIN latest_score ls ON ls.client_id = dc.client_id
)

-- =============================================================================
-- ALERT CHECKS  (UNION ALL — one row per triggered alert per client)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. score_dropped_to_red
--    Current status is red AND previous status was NOT red
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'score_dropped_to_red'                                  AS alert_id,
    'Health score dropped to red'                           AS alert_name,
    'critical'                                              AS severity,
    'Schedule urgent CSM call; review campaign recency, automations and volume' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE health_status = 'red'
  AND (prev_health_status IS NULL OR prev_health_status != 'red')

UNION ALL

-- ---------------------------------------------------------------------------
-- 2. no_campaign_14d
--    No campaign sent in the last 14 days
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'no_campaign_14d'                                       AS alert_id,
    'No campaign sent in the last 14 days'                  AS alert_name,
    'high'                                                  AS severity,
    'Contact client to plan next campaign; check if they need creative support' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE days_since_last_campaign >= 14

UNION ALL

-- ---------------------------------------------------------------------------
-- 3. roi_dropped_below_1
--    Campaign ROI fell below 1 (not generating positive return)
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'roi_dropped_below_1'                                   AS alert_id,
    'Campaign ROI dropped below 1x'                         AS alert_name,
    'high'                                                  AS severity,
    'Review campaign targeting, segmentation and offer strategy with client' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE campaign_roi < 1

UNION ALL

-- ---------------------------------------------------------------------------
-- 4. renewal_90d
--    Contract renewing within 90 days
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'renewal_90d'                                           AS alert_id,
    'Contract renewal within 90 days'                       AS alert_name,
    'medium'                                                AS severity,
    'Initiate renewal conversation; present usage report and expansion opportunities' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE days_to_renewal <= 90

UNION ALL

-- ---------------------------------------------------------------------------
-- 5. renewal_30d_red
--    Contract renewing within 30 days AND health status is red (churn risk)
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'renewal_30d_red'                                       AS alert_id,
    'Renewal in 30 days with red health status — churn risk'AS alert_name,
    'critical'                                              AS severity,
    'Escalate to CS lead; prepare retention offer and executive outreach' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE days_to_renewal <= 30
  AND health_status = 'red'

UNION ALL

-- ---------------------------------------------------------------------------
-- 6. volume_drop_30pct
--    Month-over-month message volume dropped 30 % or more
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'volume_drop_30pct'                                     AS alert_id,
    'Message volume dropped 30% or more vs previous month'  AS alert_name,
    'high'                                                  AS severity,
    'Investigate cause of volume drop; check platform issues, campaign pause or low engagement' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE messages_mom_change <= -30

UNION ALL

-- ---------------------------------------------------------------------------
-- 7. upsell_opportunity
--    Green health AND plan usage >= 90 % — client is ready for upsell
-- ---------------------------------------------------------------------------
SELECT
    client_id,
    company_name,
    'upsell_opportunity'                                    AS alert_id,
    'Upsell opportunity: green health + plan usage >= 90%'  AS alert_name,
    'low'                                                   AS severity,
    'Present plan upgrade or expanded features; schedule strategic growth call' AS action_suggested,
    date('now')                                             AS alert_date
FROM base
WHERE health_status = 'green'
  AND plan_usage_pct >= 90

-- =============================================================================
-- Final ordering: critical first, then by company name
-- =============================================================================
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high'     THEN 2
        WHEN 'medium'   THEN 3
        WHEN 'low'      THEN 4
        ELSE 5
    END,
    company_name,
    alert_id;
