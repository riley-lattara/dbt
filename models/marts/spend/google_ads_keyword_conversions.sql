{{ config(materialized = "table") }}

-- Google Ads Keyword Conversions
-- Provides conversion detail by keyword and conversion action
-- Does NOT include spend to prevent duplication when joined to performance data

SELECT
    date,
    account,
    customer_id,
    campaign,
    campaign_id,
    adgroup,
    ad_group_id,
    keyword,
    match_type,
    conversion_action_name,
    conversions,
    conversion_value
FROM {{ ref('int_google_conversions') }}
ORDER BY 1 DESC, 2, 3, 4, 5
