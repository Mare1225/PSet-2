WITH unique_hours AS (

    SELECT DISTINCT EXTRACT(HOUR FROM pickup_datetime_utc) AS hour_key
    FROM {{ ref('taxi_unification') }}
    WHERE pickup_datetime_utc IS NOT NULL
    
    UNION 

    SELECT DISTINCT EXTRACT(HOUR FROM dropoff_datetime_utc) AS hour_key
    FROM {{ ref('taxi_unification') }}
    WHERE dropoff_datetime_utc IS NOT NULL
)

SELECT
    CAST(hour_key AS INTEGER) AS time_sk, 
    hour_key AS hour_of_day,
    
    CASE 
        WHEN hour_key >= 6 AND hour_key < 12 THEN 'Mañana'      
        WHEN hour_key>= 12 AND hour_key < 17 THEN 'Tarde'       
        WHEN hour_key >= 17 AND hour_key < 22 THEN 'Noche'       
        ELSE 'Madrugada'                                      
    END AS time_slot,

    
FROM unique_hours
ORDER BY hour_key
