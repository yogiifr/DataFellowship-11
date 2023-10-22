# Week 2 Task Submission: Building BigQuery Tables and Data Pipelines for Korean Drama Ratings Dashboard

## Group 3 Approach

Group 3 was assigned the task of creating a BigQuery table that serves as a source for a dashboard, based on the "Top 250 Korean Dramas" dataset obtained from Kaggle. The task involved three key components: loading CSV data into BigQuery, transforming the data, and setting up data pipelines for data extraction.

### Dataset : Top 250 Korean Dramas

This dataset comprises information on the top 250 Korean Dramas, sourced from the MyDramaList website, and is presented in CSV format. **With 17 columns and 251 rows**, the dataset primarily contains textual data, such as drama names, release years, episode counts, and more. While the majority of the data was collected from MyDramaList (https://mydramalist.com), details regarding Production Companies' names were gathered from Wikipedia (https://www.wikipedia.org). The dataset provides a valuable resource for exploring and analyzing the Korean Drama landscape and can be found on Kaggle (https://www.kaggle.com/datasets/ahbab911/top-250-korean-dramas-kdrama-dataset).

### Task 1: Load CSV Data into GCS Bucket

To fulfill the first part of the task, Group 3 followed these steps:

1. **Data Preparation**: Downloaded the "Top 250 Korean Dramas" dataset in CSV format from Kaggle.

2. **GCS Bucket Setup**: Uploaded the downloaded CSV data directly into a Google Cloud Storage (GCS) bucket. 

By performing these two steps, Group 3 ensured that the dataset was readily available in a GCS bucket for further processing and analysis. The dataset was not loaded into BigQuery in this part of the task.

### Task 2: Transform Data in BigQuery

For the data transformation part of the task, Group 3 took the following steps using a Python script with Apache Beam:

1. **Data Transformation in Python Script**: Group 3 created a Python script (`csv_gcs_to_bigquery.py`) that uses Apache Beam to transform the raw data. The script reads CSV data, processes it, and prepares it for loading into BigQuery. Here's an overview of the script's main components:

    - `RowTransformer`: A custom DoFn class within the script that processes each row of the CSV data. It performs transformations like date formatting and data type conversion. This step is crucial for preparing the data for BigQuery.

    - `MergeLines`: Another custom DoFn class that helps to handle multi-line data within the CSV. It merges lines when necessary and ensures that each row is processed correctly.

    - `LogData`: A custom DoFn class for logging intermediate data at various stages of processing, which is useful for debugging and monitoring.

2. **Python Script Execution**: Group 3 executed the Python script with appropriate arguments to specify the input data location, output BigQuery table, GCS temp location, staging location, and project ID.

3. **Output to BigQuery**: The script writes the transformed data into a specified BigQuery table with the defined schema. It checks if the table exists and appends data to it if needed.

The provided Python script efficiently performs data transformation, ensuring that the data is in the desired format for the dashboard. It also handles complex scenarios like multi-line data, making it suitable for processing large datasets.


### Task 3: Data Pipelines

For setting up data pipelines to extract and process data, Group 3 followed these steps and used a Python script named `bigquery_to_bigquery.py`:

1. **Data Pipeline Execution**: After confirming that the BigQuery raw data was available and fulfilled, Group 3 initiated the `bigquery_to_bigquery.py` script to perform transformations required for the dashboard data. This script was designed to process data from the Korean Drama Dataset.

2. **Data Transformation for Dashboard**: The data transformation included categorizing dramas based on predefined rating criteria and calculating the average number of episodes. The rating criteria used are:
   - Rating < 8.5: Categorized as "Great"
   - Rating between 8.5 - 8.9: Categorized as "Excellent"
   - Rating >= 9: Categorized as "Exceptional"

3. **Data Extraction and Load**: The script extracted the transformed data and loaded it into a new BigQuery table to be used for the dashboard. The script also ensured that the output table existed and, if not, created it. The schema for the new table was defined according to the requirements of the dashboard.

The provided Python script (`bigquery_to_bigquery.py`) leverages Apache Beam to efficiently perform data transformation and load it into BigQuery. It also meets the specific criteria for the dashboard, enabling meaningful analysis and visualization.

---

## Conclusion

In completing the assigned tasks, Group 3 successfully set up a data processing pipeline to transform raw data from the "Top 250 Korean Dramas" dataset into a format suitable for creating a dashboard. The tasks involved data extraction, transformation, and data pipeline creation.
