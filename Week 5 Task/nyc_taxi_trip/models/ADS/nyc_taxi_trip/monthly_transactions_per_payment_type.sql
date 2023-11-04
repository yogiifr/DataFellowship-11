WITH monthly_payment_counts AS (
  SELECT
    CASE
      {{ convert_month_number_to_name('EXTRACT(MONTH FROM lpep_pickup_datetime)') }}
    END AS month_name,
    payment_type,
    COUNT(*) AS transaction_count
  FROM
    {{ source('cdm_nyc_taxi_trip', 'int_nyc_taxi_trip')}}
  GROUP BY
    1, payment_type
)

SELECT
  month_name,
  CASE
    WHEN payment_type = 1 THEN 'Credit card'
    WHEN payment_type = 2 THEN 'Cash'
    WHEN payment_type = 3 THEN 'No charge'
    WHEN payment_type = 4 THEN 'Dispute'
    WHEN payment_type = 5 THEN 'Unknown'
    WHEN payment_type = 6 THEN 'Voided trip'
  END AS payment_type_name,
  transaction_count
FROM
  monthly_payment_counts
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
