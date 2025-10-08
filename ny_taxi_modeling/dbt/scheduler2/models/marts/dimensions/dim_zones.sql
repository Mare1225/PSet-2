SELECT
    CAST(taxi_zone_id AS INTEGER) AS zone_sk, 
    borough,
    zone_id
    
FROM {{ ref('stg_taxis_zones') }} 
WHERE taxi_zone_id IS NOT NULL