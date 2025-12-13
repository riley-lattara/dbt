{{ config(materialized = "table") }}

-- Google Ads Campaign Performance
-- Combines campaign spend with conversions WITHOUT duplication
-- Uses ROW_NUMBER to assign spend only to first row per campaign/date
-- Captures ALL spend (including Performance Max, Demand Gen, Video)

WITH campaign_spend AS (
    SELECT * FROM {{ ref('int_google_campaign_spend') }}
),

campaign_conversions AS (
    SELECT * FROM {{ ref('int_google_campaign_conversions') }}
),

optimization_conv AS (
    SELECT * FROM {{ ref('optimization_conversions') }}
    WHERE platform = 'google'
),

joined AS (
    SELECT 
        COALESCE(cs.date, cc.date) AS date,
        COALESCE(cs.account, cc.account) AS account,
        COALESCE(cs.customer_id, cc.customer_id) AS customer_id,
        COALESCE(cs.campaign, cc.campaign) AS campaign,
        COALESCE(cs.campaign_id, cc.campaign_id) AS campaign_id,
        cc.conversion_action_name,
        cs.spend,
        cs.impressions,
        cs.clicks,
        cc.conversions,
        cc.conversion_value,
        ROW_NUMBER() OVER (
            PARTITION BY 
                COALESCE(cs.date, cc.date),
                COALESCE(cs.customer_id, cc.customer_id),
                COALESCE(cs.campaign_id, cc.campaign_id)
            ORDER BY cc.conversion_action_name NULLS LAST
        ) AS rn
    FROM campaign_spend cs
    FULL OUTER JOIN campaign_conversions cc
        ON cs.date = cc.date
        AND cs.customer_id = cc.customer_id
        AND cs.campaign_id = cc.campaign_id
)

SELECT 
    j.date,
    j.account,
    j.customer_id,
    j.campaign,
    j.campaign_id,
    j.conversion_action_name,
    CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END AS spend,
    CASE WHEN j.rn = 1 THEN j.impressions ELSE 0 END AS impressions,
    CASE WHEN j.rn = 1 THEN j.clicks ELSE 0 END AS clicks,
    COALESCE(j.conversions, 0) AS conversions,
    COALESCE(j.conversion_value, 0) AS conversion_value,
    div0(CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END, CASE WHEN j.rn = 1 THEN j.clicks ELSE 0 END) AS cpc,
    div0(CASE WHEN j.rn = 1 THEN j.clicks ELSE 0 END, CASE WHEN j.rn = 1 THEN j.impressions ELSE 0 END) * 100 AS ctr,
    div0(CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END, COALESCE(j.conversions, 0)) AS cpa,
    div0(COALESCE(j.conversion_value, 0), CASE WHEN j.rn = 1 THEN j.spend ELSE 0 END) AS roas,
    CASE WHEN oc.optimization_conversion IS NOT NULL THEN TRUE ELSE FALSE END AS is_optimization_conversion
FROM joined j
LEFT JOIN optimization_conv oc
    ON j.account = oc.account
    AND j.conversion_action_name = oc.optimization_conversion
ORDER BY 1 DESC, 2, 3, 4
