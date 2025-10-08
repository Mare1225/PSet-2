SELECT DISTINCT
    CAST(rate_code_id AS INTEGER) AS rate_code_sk, 
    rate_code_description
FROM {{ ref('taxi_unification') }}
