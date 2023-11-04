WITH source AS (

    SELECT * FROM {{ source('raw', 'nyc_taxi_trip')}}
)

SELECT * FROM source