{{config(materialized = "table")}} 

select
  s.date,
  s.account,
  s.customer_id,
  s.campaign,
  s.campaign_id,
  s.adgroup,
  s.ad_group_id,
  s.keyword,
  s.match_type,
  s.spend,
  s.impressions,
  s.clicks,
  c.conversion_action_name,
  c.conversions,
  c.conversion_value,
  div0(s.spend, s.clicks) as cpc,
  div0(s.clicks, s.impressions) * 100 as ctr,
  div0(s.spend, c.conversions) as cpa,
  div0(c.conversion_value, s.spend) as roas
from {{ ref('int_google_spend') }} s
left join {{ ref('int_google_conversions') }} c
  on s.date = c.date
  and s.campaign_id = c.campaign_id
  and s.ad_group_id = c.ad_group_id
  and s.keyword = c.keyword
  and s.match_type = c.match_type
order by 1 desc, 2, 3, 4, 5, 6, 7