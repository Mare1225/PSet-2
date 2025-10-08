WITH trips AS (
    SELECT 
        *,
        
        DATEDIFF(SECOND, pickup_datetime_utc, dropoff_datetime_utc) AS trip_duration_seconds,
        
        DATE_TRUNC('day', pickup_datetime_utc) AS pickup_date_key,
        DATE_TRUNC('day', dropoff_datetime_utc) AS dropoff_date_key,
        EXTRACT(HOUR FROM pickup_datetime_utc) AS pickup_time_key,
        EXTRACT(HOUR FROM dropoff_datetime_utc) AS dropoff_time_key
        
    FROM {{ ref('taxi_unification') }}
),

dim_date_joined AS (
    SELECT
        t.*,
        
        dim_date_pu.date_sk AS pickup_date_sk,
        dim_date_do.date_sk AS dropoff_date_sk
        
    FROM trips t
    
    LEFT JOIN {{ ref('dim_date') }} dim_date_pu 
        ON t.pickup_date_key = dim_date_pu.date_key 
    LEFT JOIN {{ ref('dim_date') }} dim_date_do 
        ON t.dropoff_date_key = dim_date_do.date_key
),

dim_time_joined AS (
    SELECT
        d.*,
        
        dim_time_pu.time_sk AS pickup_time_sk,
        dim_time_do.time_sk AS dropoff_time_sk
        
    FROM dim_date_joined d
    
    LEFT JOIN {{ ref('dim_time') }} dim_time_pu 
        ON d.pickup_time_key = dim_time_pu.time_sk 
    LEFT JOIN {{ ref('dim_time') }} dim_time_do 
        ON d.dropoff_time_key = dim_time_do.time_sk
),

dim_zone_joined AS (
    SELECT
        t.*,
        
        dim_zone_pu.zone_sk AS pu_zone_sk,
        dim_zone_do.zone_sk AS do_zone_sk
        
    FROM dim_time_joined t
    
    LEFT JOIN {{ ref('dim_zones') }} dim_zone_pu 
        ON t.pu_location_id = dim_zone_pu.zone_sk 
    LEFT JOIN {{ ref('dim_zones') }} dim_zone_do 
        ON t.do_location_id = dim_zone_do.zone_sk
),


dim_all_joined AS (
    SELECT
        z.*,
        
        dim_vendor.vendor_sk,
        dim_rate.rate_code_sk,
        dim_payment.payment_type_sk,
        dim_trip.trip_type_sk,
        dim_service.service_type_sk

    FROM dim_zone_joined z
    
    LEFT JOIN {{ ref('dim_vendor') }} dim_vendor 
        ON z.vendor_id = dim_vendor.vendor_sk 
    LEFT JOIN {{ ref('dim_rate_code') }} dim_rate 
        ON z.rate_code_id = dim_rate.rate_code_sk
    LEFT JOIN {{ ref('dim_payment_type') }} dim_payment
        ON z.payment_type_id = dim_payment.payment_type_sk
    LEFT JOIN {{ ref('dim_trip_type') }} dim_trip
        ON z.trip_type = dim_trip.trip_type_sk
    LEFT JOIN {{ref('dim_service_type')}} dim_service
        ON z.service_type = dim_service.service_type_sk
        
    
)

SELECT

    pickup_date_sk,
    dropoff_date_sk,
    pickup_time_sk,
    dropoff_time_sk,
    pu_zone_sk,
    do_zone_sk,
    vendor_sk,
    rate_code_sk,
    payment_type_sk,
    trip_type_sk,
    service_type_sk
    
    -- MÉTRICAS (HECHOS)
    passenger_count,
    trip_distance,
    
    -- Métricas de Ingreso y Costo
    fare_amount,
    extra_charge,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount,
    
    -- Métricas Calculadas
    trip_duration_seconds
    
FROM dim_all_joined
WHERE pickup_date_sk IS NOT NULL AND dropoff_date_sk IS NOT NULL