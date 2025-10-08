SELECT

    CAST("LocationID" AS INTEGER) AS taxi_zone_id,
    
    CAST("Borough" AS VARCHAR) AS borough,
    
    CAST("Zone" AS VARCHAR) AS zone_id,
    
FROM {{ source('ny_taxi2', 'ny_taxi_zones') }}

