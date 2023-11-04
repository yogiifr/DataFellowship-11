# Week 5 Task Submission NYC Taxi Trip Data Analysis and Currency Conversion with dbt and Airflow

## Approach Strategy

In this project, we are tasked with analyzing NYC Taxi Trip data to extract valuable insights using dbt for data transformations and Airflow for orchestrating the data pipeline. Below is the comprehensive structure of our approach to this project.

### Dataset: NYC Taxi Trip
The dataset consists of trip records submitted by yellow taxi Technology Service Providers (TSPs) in New York City. Each record represents a single trip taken in a yellow taxi during the year 2020. The dataset includes information about pick-up and drop-off dates/times, locations, trip distances, fares, rate types, payment types, and passenger counts.

You can find the NYC Taxi Trip data on Kaggle: [NYC Taxi Trip Data](https://www.kaggle.com/datasets/anandaramg/taxi-trip-data-nyc/data).

### Task 1: Monthly Total Passengers
**Objective**: Analyze the number of passengers in NYC taxi trips on a monthly basis.

**Approach**: 
1. Extract data from the 'raw.nyc_taxi_trip' table in our Data Warehouse.
2. Use dbt to transform the data, aggregating passenger counts on a monthly basis.
3. Create a dbt model to store monthly passenger counts.
4. Add descriptions to tables and columns for data governance.
5. Implement custom tests to ensure data quality and reliability.

### Task 2: Monthly Transactions per Payment Type
**Objective**: Understand the distribution of different payment types used by passengers each month.

**Approach**: 
1. Extract data from the 'raw.nyc_taxi_trip' table in our Data Warehouse.
2. Use dbt to transform the data, grouping transactions by payment type on a monthly basis.
3. Create a dbt model to store monthly transaction counts per payment type.
4. Add descriptions to tables and columns for data governance.
5. Implement custom tests to ensure data quality and reliability.

### Task 3: Monthly Trip Distance per Rate Code
**Objective**: Analyze trip distances based on different rate codes every month.

**Approach**: 
1. Extract data from the 'raw.nyc_taxi_trip' table in our Data Warehouse.
2. Use dbt to transform the data, aggregating trip distances by rate code on a monthly basis.
3. Create a dbt model to store monthly trip distance data per rate code.
4. Add descriptions to tables and columns for data governance.
5. Implement custom tests to ensure data quality and reliability.

### Bonus Task: Changing Conversion Rate of USD to IDR
**Objective**: Convert the currency of the data from USD to Indonesian Rupiah (IDR).

**Approach**: 
1. Access historical exchange rate data from [USD/IDR Historical Data](https://www.investing.com/currencies/usd-idr-historical-data) to determine the conversion rate for USD to IDR.
2. Apply the exchange rate to the entire dataset to convert all fares and payments to IDR, ensuring accuracy.

## Conclusion

In this data warehousing project, we have laid out a comprehensive approach to extract, transform, and analyze NYC Taxi Trip data to provide the business team with valuable insights. We have incorporated data governance, custom tests, and custom macros into our dbt project to ensure data quality and reliability. Additionally, we have taken on the bonus challenge of converting the currency from USD to IDR to meet specific requirements. By following this approach, we aim to deliver meaningful insights that will enable the business team to make informed decisions and optimize their operations.
