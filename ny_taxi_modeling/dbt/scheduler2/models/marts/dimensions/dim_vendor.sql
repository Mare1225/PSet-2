SELECT DISTINCT
    CAST(vendor_id AS INTEGER) AS vendor_sk, 
    vendor_description

FROM {{ ref('taxi_unification') }}
