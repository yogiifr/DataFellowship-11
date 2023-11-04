WITH source AS (

    SELECT * FROM {{ source('raw', 'usd_idr')}}
)

SELECT * FROM source