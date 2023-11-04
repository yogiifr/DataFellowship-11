WITH monthly_trip_distance AS (
  SELECT
    CASE
      {{ convert_month_number_to_name('EXTRACT(MONTH FROM lpep_pickup_datetime)') }}
    END AS month_name,
    RateCodeID,
    SUM(trip_distance) AS total_trip_distance
  FROM
    {{ source('cdm_nyc_taxi_trip', 'int_nyc_taxi_trip')}}
  GROUP BY
    1, RateCodeID
)

SELECT
  month_name,
  CASE
    WHEN RateCodeID = 1 THEN 'Standard rate'
    WHEN RateCodeID = 2 THEN 'JFK'
    WHEN RateCodeID = 3 THEN 'Newark'
    WHEN RateCodeID = 4 THEN 'Nassau or Westchester'
    WHEN RateCodeID = 5 THEN 'Negotiated fare'
    WHEN RateCodeID = 6 THEN 'Group ride'
  END AS rate_code_name,
  total_trip_distance
FROM
  monthly_trip_distance
ORDER BY
  CASE
    WHEN month_name = 'January' THEN 1
    WHEN month_name = 'February' THEN 2
    WHEN month_name = 'March' THEN 3
    WHEN month_name = 'April' THEN 4
    WHEN month_name = 'May' THEN 5
    WHEN month_name = 'June' THEN 6
    WHEN month_name = 'July' THEN 7
    WHEN month_name = 'August' THEN 8
    WHEN month_name = 'September' THEN 9
    WHEN month_name = 'October' THEN 10
    WHEN month_name = 'November' THEN 11
    WHEN month_name = 'December' THEN 12
  END
