-- macros/convert_month_number_to_name.sql
{% macro convert_month_number_to_name(month_number) %}
  WHEN {{ month_number }} = 1 THEN 'January'
  WHEN {{ month_number }} = 2 THEN 'February'
  WHEN {{ month_number }} = 3 THEN 'March'
  WHEN {{ month_number }} = 4 THEN 'April'
  WHEN {{ month_number }} = 5 THEN 'May'
  WHEN {{ month_number }} = 6 THEN 'June'
  WHEN {{ month_number }} = 7 THEN 'July'
  WHEN {{ month_number }} = 8 THEN 'August'
  WHEN {{ month_number }} = 9 THEN 'September'
  WHEN {{ month_number }} = 10 THEN 'October'
  WHEN {{ month_number }} = 11 THEN 'November'
  WHEN {{ month_number }} = 12 THEN 'December'
{% endmacro %}
