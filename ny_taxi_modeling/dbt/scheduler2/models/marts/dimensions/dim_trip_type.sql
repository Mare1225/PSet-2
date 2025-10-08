SELECT DISTINCT
    CAST(trip_type AS INTEGER) AS trip_type_sk, 
    trip_type_description
FROM {{ ref('taxi_unification') }}