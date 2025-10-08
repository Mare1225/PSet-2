SELECT DISTINCT
    service_type AS service_type_sk, 
FROM {{ ref('taxi_unification') }}
