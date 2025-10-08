WITH source_data AS (
    SELECT * FROM {{ source('ny_taxi2', 'ny_taxi_yellow') }}

    WHERE

        "tpep_pickup_datetime" IS NOT NULL
        AND "tpep_dropoff_datetime" IS NOT NULL
        AND "VendorID" IS NOT NULL
        AND "RatecodeID" IS NOT NULL
        AND "PULocationID" IS NOT NULL
        AND "DOLocationID" IS NOT NULL
        AND "fare_amount" IS NOT NULL
        

        AND "VendorID" >= 0
        AND "passenger_count" >= 0  
        AND "RatecodeID" >= 0
        AND "PULocationID" >= 0
        AND "DOLocationID" >= 0
        AND "fare_amount" >= 0
        AND "extra" >= 0
        AND "mta_tax" >= 0
        AND "tip_amount" >= 0
        AND "tolls_amount" >= 0
        AND "trip_distance" >= 0
        AND "total_amount" >= 0
        AND "improvement_surcharge" >= 0

        
        AND "fare_amount" <= 150  
        AND "trip_distance" <= 100
        AND "passenger_count" <= 7
        AND "mta_tax" <= 50
        AND "tolls_amount" <= 10
        AND "improvement_surcharge" <= 10

        AND "tpep_pickup_datetime" >= '2015-01-01'
        AND "tpep_pickup_datetime" < '2025-09-01'

        AND "tpep_dropoff_datetime" >= '2015-01-01'
        AND "tpep_dropoff_datetime" < '2025-09-01'
),

renamed_and_cleaned AS (
    SELECT

        CAST("VendorID" AS INT) AS vendor_id,
        CAST("RatecodeID" AS INT) AS rate_code_id,
        CAST("PULocationID" AS INT) AS pu_location_id,
        CAST("DOLocationID" AS INT) AS do_location_id,
        CAST("payment_type" AS INT) AS payment_type_id,


        "tpep_pickup_datetime" AS pickup_datetime_utc,
        "tpep_dropoff_datetime" AS dropoff_datetime_utc,
        

        'yellow' AS service_type,
        CAST(NULL AS INT) AS trip_type, 
        CAST(NULL AS VARCHAR) AS trip_type_description,
        
        CASE "VendorID"
            WHEN 1 THEN 'Creative Mobile Technologies'
            WHEN 2 THEN 'Curb Mobility'
            WHEN 6 THEN 'Myle Technologies Inc'
            WHEN 7 THEN 'Helix'
            ELSE 'Other'
        END AS vendor_description,

        CASE "RatecodeID"
            WHEN 1 THEN 'Standard rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau or Westchester'
            WHEN 5 THEN 'Negotiated fare'
            WHEN 6 THEN 'Group ride'
            WHEN 99 THEN 'Null/unknown'
            ELSE 'Other'
        END AS rate_code_description,

        CASE "store_and_fwd_flag"
            WHEN 'Y' THEN TRUE
            WHEN 'N' THEN FALSE
            ELSE NULL
        END AS stored_and_forwarded,
        
        CASE "payment_type"
            WHEN 0 THEN 'Flex Fare trip'
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided Trip'
            ELSE 'Other'
        END AS payment_type_description,
        

        CAST("trip_distance" AS NUMERIC(18, 3)) AS trip_distance,
        CAST("fare_amount" AS NUMERIC(18, 3)) AS fare_amount,
        CAST("extra" AS NUMERIC(18, 3)) AS extra_charge,
        CAST("mta_tax" AS NUMERIC(18, 3)) AS mta_tax,
        CAST("tip_amount" AS NUMERIC(18, 3)) AS tip_amount, 
        CAST("tolls_amount" AS NUMERIC(18, 3)) AS tolls_amount,
        CAST("improvement_surcharge" AS NUMERIC(18, 3)) AS improvement_surcharge,
        CAST("total_amount" AS NUMERIC(18, 2)) AS total_amount,
        CAST("passenger_count" AS INT) AS passenger_count,
        CAST("congestion_surcharge" AS NUMERIC(18, 3)) AS congestion_surcharge,
        
             
    FROM source_data
)

SELECT 

    r.vendor_id,
    r.vendor_description,
    r.rate_code_id,
    r.rate_code_description,
    r.payment_type_id,
    r.payment_type_description,

    r.pickup_datetime_utc,
    r.dropoff_datetime_utc,

    r.service_type,
    r.trip_type, 
    r.trip_type_description,
    r.stored_and_forwarded,

    r.trip_distance,
    r.fare_amount,
    r.extra_charge,
    r.mta_tax,
    r.tip_amount, 
    r.tolls_amount,
    r.improvement_surcharge,
    r.total_amount,
    r.passenger_count,
    r.congestion_surcharge,
    r.pu_location_id,
    r.do_location_id,

    pz.borough AS pickup_borough,
    pz.zone_id AS pickup_zone,
    

    dz.borough AS dropoff_borough,
    dz.zone_id AS dropoff_zone

FROM renamed_and_cleaned r

LEFT JOIN {{ ref('stg_taxis_zones') }} pz 
    ON r.pu_location_id = pz.taxi_zone_id

LEFT JOIN {{ ref('stg_taxis_zones') }} dz 
    ON r.do_location_id = dz.taxi_zone_id