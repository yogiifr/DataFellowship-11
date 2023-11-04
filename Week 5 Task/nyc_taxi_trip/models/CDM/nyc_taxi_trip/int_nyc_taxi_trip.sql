WITH ods_data AS (
    SELECT * FROM {{ source('ods_nyc_taxi_trip', 'dataload_nyc_taxi_trip')}}
)

-- Dropping E-Hail Fee Collumn karena 100% kosong/null
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
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_type,
    congestion_surcharge
  FROM
    ods_data