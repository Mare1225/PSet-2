SELECT DISTINCT
    CAST(payment_type_id AS INTEGER) AS payment_type_sk, 
    payment_type_description
FROM {{ ref('taxi_unification') }}