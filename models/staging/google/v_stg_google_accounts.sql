SELECT id as customer_id
         , descriptive_name as account_name
    FROM (
        SELECT id
             , descriptive_name
             , ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM FIVETRAN_DATABASE.GOOGLE_ADS_FM.ACCOUNT_HISTORY
    )
    WHERE rn = 1