WITH unique_dates AS (

    SELECT DISTINCT DATE_TRUNC('day', pickup_datetime_utc) AS date_key 
    FROM {{ ref('taxi_unification') }}
    WHERE pickup_datetime_utc IS NOT NULL
    
    UNION 
    
    SELECT DISTINCT DATE_TRUNC('day', dropoff_datetime_utc) AS date_key
    FROM {{ ref('taxi_unification') }}
    WHERE dropoff_datetime_utc IS NOT NULL
),

date_attributes AS (
    SELECT
        date_key, 
        
        CAST(TO_VARCHAR(date_key, 'YYYYMMDD') AS INTEGER) AS date_sk, 
        
        EXTRACT(YEAR FROM date_key) AS year,
        EXTRACT(MONTH FROM date_key) AS month,
        TO_VARCHAR(date_key, 'YYYY-MM') AS year_month_key,
        EXTRACT(DAY FROM date_key) AS day_of_month,
        DAYNAME(date_key) AS day_of_week_name,
        EXTRACT(DAYOFWEEK_ISO FROM date_key) AS day_of_week_iso
        
    FROM unique_dates
    WHERE date_key IS NOT NULL 
)

SELECT * FROM date_attributes
ORDER BY date_key