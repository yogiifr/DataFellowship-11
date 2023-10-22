# Week 3 Task Submission Telco Customer Churn Data Pipeline: Google Cloud Implementation

## Approach Strategy

For this task, the approach involves building a data pipeline to predict customer churn by analyzing customer data. We use Google Cloud services, including Google Cloud Storage (GCS), Pub/Sub, Dataflow, and BigQuery, to create a robust data processing system. The strategy involves setting up the infrastructure, ingesting data, transforming and storing it, and enabling data analysts to generate summary tables for insights.

### Dataset : Telco Customer Churn

The dataset contains customer information, including churn status, services signed up for, account details, and demographic data. This information is valuable for predicting and understanding customer behavior.
(https://www.kaggle.com/datasets/blastchar/telco-customer-churn)


### Task 1: GCS Bucket with the Data

- Created a Google Cloud Storage (GCS) bucket to store the incoming data from customers.

### Task 2: Pub/Sub Topic List with Details

- Established a Pub/Sub topic named "telco-task" to act as the entry point for customer data.

### Task 3: Pub/Sub Subscription List with Details

- Configured two Pub/Sub subscriptions:
  - "telco-task-sub" for the main data processing.
  - "telco-task-backup" for creating a backup of the data.

### Task 4: Dataflow Job List with Details
 
- Utilized Dataflow to build three job templates:
  1. `text-files-from-gcs-to-pub-sub` To ingest data from GCS and publish it to the "telco-task" Pub/Sub topic.
  2. `pub-sub-subscription-to-bigquery` (main data): To process data from "telco-task-sub" and load it into the "telcoTask-main" BigQuery table.
  3. `pub-sub-subscription-to-bigquery` (backup data): To process data from "telco-task-backup" and load it into the "telcoTask-backup" BigQuery table.
- Due to limitations, the Dataflow jobs were run sequentially.

### Task 5: BigQuery Table with Ingested Data

- Created a BigQuery table named "telcoTask-main" to store the ingested and processed main data.

### Task 6: Backup Storage with Ingested Data

- Established a backup storage solution, "telcoTask-backup," using BigQuery to hold a copy of the data for redundancy and backup purposes.

### Bonus Task: Help Data Analysts Generate Summary Tables

- For data transformation and summary table creation, the `bq2bq_summary.py` script from a previous project was employed.
- This Python script performs essential data transformations and aggregations to extract meaningful insights from the dataset.
- As a result, a "telcoTask-summary" table was established to store the outcomes of the summary queries.
- Data analysts can now efficiently access and utilize this table to gain valuable insights from the data.

---
## Conclusion

This data pipeline operates as follows:

1. Data is ingested from customers into the GCS bucket.
2. Pub/Sub receives data from GCS via the "telco-task" topic and distributes it to "telco-task-sub" for main processing and "telco-task-backup" for backup.
3. Dataflow jobs sequentially process data from both subscriptions and load it into BigQuery tables, "telcoTask-main" and "telcoTask-backup."
4. Data analysts can use SQL queries to create summary tables and extract insights from the "telcoTask-summary" table.

This pipeline ensures the secure storage and analysis of customer data, aiding in predicting customer behavior and implementing targeted retention programs.
