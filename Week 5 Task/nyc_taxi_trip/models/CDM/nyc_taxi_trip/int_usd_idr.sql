WITH usd_idr AS (
    SELECT * FROM {{ source('ods_nyc_taxi_trip', 'dataload_usd_idr')}}
)

-- Dropping E-Hail Fee Collumn karena 100% kosong/null
SELECT
  _Date_,
  Price,
FROM
  usd_idr

