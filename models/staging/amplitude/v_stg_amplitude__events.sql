SELECT USER_ID
     , DEVICE_ID
     , EVENT_TYPE
     , EVENT_TIME
     , EVENT_PROPERTIES
FROM {{ source('amplitude', 'EVENTS_726530') }}