WITH ods_data AS (
    SELECT * FROM {{ source('ods_nyc_taxi_trip', 'dataload_nyc_taxi_trip')}}
),

latest_usd_idr AS (
    SELECT Price
    FROM {{ ref('int_usd_idr')}}
    ORDER BY _Date_ DESC
    LIMIT 1
)

-- Dropping E-Hail Fee Column because it's 100% empty/null
SELECT
    VendorID,
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    store_and_fwd_flag,
    RatecodeID,
    PULocationID,
    DOLocationID,
    passenger_count,
    trip_distance,
    fare_amount * (SELECT Price FROM latest_usd_idr) AS fare_amount_in_idr,
    extra * (SELECT Price FROM latest_usd_idr) AS extra_in_idr,
    mta_tax * (SELECT Price FROM latest_usd_idr) AS mta_tax_in_idr,
    tip_amount * (SELECT Price FROM latest_usd_idr) AS tip_amount_in_idr,
    tolls_amount * (SELECT Price FROM latest_usd_idr) AS tolls_amount_in_idr,
    improvement_surcharge * (SELECT Price FROM latest_usd_idr) AS improvement_surcharge_in_idr,
    total_amount * (SELECT Price FROM latest_usd_idr) AS total_amount_in_idr,
    payment_type,
    trip_type,
    congestion_surcharge * (SELECT Price FROM latest_usd_idr) AS congestion_surcharge_in_idr
FROM
    ods_data
