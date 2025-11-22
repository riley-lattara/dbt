{{
    config(
        materialized='table'
    )
}}

SELECT DEVICE_ID_UUID AS ADJUST_IDFV
     , USER_ID_INTEGER AS AMPLITUDE_USER_ID
FROM {{ ref('v_stg_amplitude__merge_ids') }}