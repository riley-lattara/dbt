{{ config(materialized = "table") }}

-- Google Ads Ad Group Performance
-- Captures Search, Demand Gen, and Video campaigns
-- Performance Max excluded (no ad groups)

SELECT
    date,
    account,
    customer_id,
    campaign,
    campaign_id,
    campaign_type,
    adgroup,
    ad_group_id,
    spend,
    impressions,
    clicks,
    conversions,
    conversion_value,
    div0(spend, clicks) AS cpc,
    div0(clicks, impressions) * 100 AS ctr,
    div0(spend, conversions) AS cpa,
    div0(conversion_value, spend) AS roas
FROM {{ ref('int_google_adgroup_spend') }}
ORDER BY 1 DESC, 2, 3
