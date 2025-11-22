{{
    config(
        materialized='table'
    )
}}

SELECT USER_ID
     , COUNT(*) AS PURCHASE_COUNT
     , SUM(REVENUE_AMOUNT) AS TOTAL_REVENUE
FROM {{ ref('v_stg_revenue__events') }}
GROUP BY USER_ID