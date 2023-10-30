import os
import csv
from pathlib import Path

from airflow import DAG
from datetime import datetime


from airflow.configuration import conf
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator


def transform_users(**kwargs):
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='fetch_users_data')

    transformed_users = []

    for user in return_value["users"]:
        transformed_user = {
            "id": user["id"],
            "firstName": user["firstName"],
            "lastName": user["lastName"],
            "age": user["age"],
            "email": user["email"],
            "username": user["username"],
        }

        transformed_users.append(transformed_user)

    return transformed_users

def users_stores(**kwargs):
    ti = kwargs['ti']
    transformed_users = ti.xcom_pull(task_ids='transform_user')

    # Define the CSV file path for age group A
    csv_file_path = './dags/output/users.csv'

    # Check if the CSV file already exists or create a new one
    write_header = not os.path.exists(csv_file_path)

    # Write user data to the CSV file
    with open(csv_file_path, mode='w', newline='') as file:
        fieldnames = ["id","firstName", "lastName", "age", "email", "username"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for transformed_user in transformed_users:
            writer.writerow(transformed_user)

def transform_carts(**kwargs):
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='fetch_carts_data')

    transformed_carts = []

    for cart in return_value["carts"]:
        for product in cart["products"]:
            transformed_cart = {
                "id": cart["id"],
                "productId": product["id"],
                "productTitle": product["title"],
                "price": product["price"],
                "quantity": product["quantity"],
                "discountPercentage": product["discountPercentage"],
                "discountedPrice": product["discountedPrice"],
            }

            transformed_carts.append(transformed_cart)

    return transformed_carts

def carts_stores(**kwargs):
    ti = kwargs['ti']
    transformed_carts = ti.xcom_pull(task_ids='transform_carts')

    # Define the CSV file path for age group A
    csv_file_path = './dags/output/carts.csv'

    # Check if the CSV file already exists or create a new one
    write_header = not os.path.exists(csv_file_path)

    # Write user data to the CSV file
    with open(csv_file_path, mode='w', newline='') as file:
        fieldnames = ["id", "productId", "productTitle", "price", "quantity", "discountPercentage", "discountedPrice"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for transformed_cart in transformed_carts:
            writer.writerow(transformed_cart)

def transform_posts(**kwargs):
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='fetch_posts_data')

    transformed_posts = []

    for post in return_value["posts"]:
        transformed_post = {
            "id": post["id"],
            "title": post["title"],
            "body": post["body"],
            "userId": post["userId"],
            "tags": post["tags"],
            "reactions": post["reactions"],
        }

        transformed_posts.append(transformed_post)

    return transformed_posts

def posts_stores(**kwargs):
    ti = kwargs['ti']
    transformed_posts = ti.xcom_pull(task_ids='transform_posts')

    # Define the CSV file path for age group A
    csv_file_path = './dags/output/posts.csv'

    # Check if the CSV file already exists or create a new one
    write_header = not os.path.exists(csv_file_path)

    # Write user data to the CSV file
    with open(csv_file_path, mode='w', newline='') as file:
        fieldnames = ["id","title", "body", "userId", "tags", "reactions"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for transformed_post in transformed_posts:
            writer.writerow(transformed_post)

def transform_todos(**kwargs):
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='fetch_todos_data')

    transformed_todos = []

    for todo in return_value["todos"]:
        transformed_todo = {
            "id": todo["id"],
            "todo": todo["todo"],
            "completed": todo["completed"],
            "userId": todo["userId"],
        }

        transformed_todos.append(transformed_todo)

    return transformed_todos

def todos_stores(**kwargs):
    ti = kwargs['ti']
    transformed_todos = ti.xcom_pull(task_ids='transform_todos')

    # Define the CSV file path for age group A
    csv_file_path = './dags/output/todos.csv'

    # Check if the CSV file already exists or create a new one
    write_header = not os.path.exists(csv_file_path)

    # Write user data to the CSV file
    with open(csv_file_path, mode='w', newline='') as file:
        fieldnames = ["id","todo", "completed", "userId"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        for transformed_todo in transformed_todos:
            writer.writerow(transformed_todo)

comp_home_path = Path(conf.get("core", "dags_folder")).parent.absolute()
comp_bucket_path = "dags/output/" # <- if your file is within a folder
comp_local_path = os.path.join(comp_home_path, comp_bucket_path)

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", "dummy_json")
USERS_OUTPUT = os.environ.get("GCP_TABLE_NAME", "users")
CARTS_OUTPUT = os.environ.get("GCP_TABLE_NAME", "carts")
POSTS_OUTPUT = os.environ.get("GCP_TABLE_NAME", "posts")
TODOS_OUTPUT = os.environ.get("GCP_TABLE_NAME", "todos")
SUMMARY_OUTPUT = os.environ.get("GCP_TABLE_NAME", "summary")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "handy-honor-401601")



with DAG(
    'dummy_json_task',
    description='Random User API Ingestion',
    start_date=datetime(2023, 10, 28),
    schedule_interval="* 17 * * *",
    tags=['WEEK4']
) as dag:
    
    api_check = HttpSensor(
        task_id='api_check',
        http_conn_id='dummy_json_api',
        endpoint='/users',
        dag=dag,
    )

    fetch_users_data = SimpleHttpOperator(
        task_id='fetch_users_data',
        http_conn_id='dummy_json_api',
        endpoint='/users',
        method='GET',
        response_filter=lambda response: response.json(), # Parse the JSON response
        dag=dag,
    )

    transform_users = PythonOperator(
        task_id='transform_user',
        python_callable=transform_users,
        provide_context=True,
        dag=dag,
        retries=0,           # Disable retries
    )

    users_stores = PythonOperator(
        task_id='users_stores',
        python_callable=users_stores,
        provide_context=True,
        dag=dag
    )

    users_to_gcs = LocalFilesystemToGCSOperator(
       task_id="users_to_gcs",
       src=comp_local_path+"users.csv",# PATH_TO_UPLOAD_FILE
       dst="dummy_json_task/users.csv",# BUCKET_FILE_LOCATION
       bucket="fellowship-yogi",#using NO 'gs://' nor '/' at the end, only the project, folders, if any, in dst
       dag=dag
   )

    fetch_carts_data = SimpleHttpOperator(
        task_id='fetch_carts_data',
        http_conn_id='dummy_json_api',
        endpoint='/carts',
        method='GET',
        response_filter=lambda response: response.json(), # Parse the JSON response
        dag=dag,
    )

    transform_carts = PythonOperator(
        task_id='transform_carts',
        python_callable=transform_carts,
        provide_context=True,
        dag=dag
    )

    carts_stores = PythonOperator(
        task_id='carts_stores',
        python_callable=carts_stores,
        provide_context=True,
        dag=dag
    )

    carts_to_gcs = LocalFilesystemToGCSOperator(
       task_id="carts_to_gcs",
       src=comp_local_path+"carts.csv",# PATH_TO_UPLOAD_FILE
       dst="dummy_json_task/carts.csv",# BUCKET_FILE_LOCATION
       bucket="fellowship-yogi",#using NO 'gs://' nor '/' at the end, only the project, folders, if any, in dst
       dag=dag
   )
    
    fetch_posts_data = SimpleHttpOperator(
        task_id='fetch_posts_data',
        http_conn_id='dummy_json_api',
        endpoint='/posts',
        method='GET',
        response_filter=lambda response: response.json(), # Parse the JSON response
        dag=dag,
    )

    transform_posts = PythonOperator(
        task_id='transform_posts',
        python_callable=transform_posts,
        provide_context=True,
        dag=dag
    )

    posts_stores = PythonOperator(
        task_id='posts_stores',
        python_callable=posts_stores,
        provide_context=True,
        dag=dag
    )

    posts_to_gcs = LocalFilesystemToGCSOperator(
       task_id="posts_to_gcs",
       src=comp_local_path+"posts.csv",# PATH_TO_UPLOAD_FILE
       dst="dummy_json_task/posts.csv",# BUCKET_FILE_LOCATION
       bucket="fellowship-yogi",#using NO 'gs://' nor '/' at the end, only the project, folders, if any, in dst
       dag=dag
   )

    fetch_todos_data = SimpleHttpOperator(
        task_id='fetch_todos_data',
        http_conn_id='dummy_json_api',
        endpoint='/todos',
        method='GET',
        response_filter=lambda response: response.json(), # Parse the JSON response
        dag=dag,
    )

    transform_todos = PythonOperator(
        task_id='transform_todos',
        python_callable=transform_todos,
        provide_context=True,
        dag=dag
    )

    todos_stores = PythonOperator(
        task_id='todos_stores',
        python_callable=todos_stores,
        provide_context=True,
        dag=dag
    )

    todos_to_gcs = LocalFilesystemToGCSOperator(
       task_id="todos_to_gcs",
       src=comp_local_path+"todos.csv",# PATH_TO_UPLOAD_FILE
       dst="dummy_json_task/todos.csv",# BUCKET_FILE_LOCATION
       bucket="fellowship-yogi",#using NO 'gs://' nor '/' at the end, only the project, folders, if any, in dst
       dag=dag
   )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id=DATASET_NAME,
        location='asia-southeast2',
        dag=dag
    )

    users_load_csv = GCSToBigQueryOperator(
        task_id='users_load_csv',
        bucket='fellowship-yogi',
        source_objects=['dummy_json_task/users.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{USERS_OUTPUT}",
        schema_fields=[
                        {
                            "name": "id",
                            "type": "INTEGER",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "firstName",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "lastName",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "age",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "email",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "username",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                        ],
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )

    carts_load_csv = GCSToBigQueryOperator(
        task_id='carts_load_csv',
        bucket='fellowship-yogi',
        source_objects=['dummy_json_task/carts.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{CARTS_OUTPUT}",
        schema_fields=[
                        {
                            "name": "id",
                            "type": "INTEGER",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "productId",
                            "type": "INTEGER",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "productTitle",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "price",
                            "type": "FLOAT",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "quantity",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "discountPercentage",
                            "type": "FLOAT",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "discountedPrice",
                            "type": "FLOAT",
                            "mode": "NULLABLE"
                        }
                        ],
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )

    posts_load_csv = GCSToBigQueryOperator(
        task_id='posts_load_csv',
        bucket='fellowship-yogi',
        source_objects=['dummy_json_task/posts.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{POSTS_OUTPUT}",
        schema_fields=[
            {
                "name": "id",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "title",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "body",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "userId",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "tags",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "reactions",
                "type": "INTEGER",
                "mode": "NULLABLE"
            }
            ],
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )

    todos_load_csv = GCSToBigQueryOperator(
        task_id='todos_load_csv',
        bucket='fellowship-yogi',
        source_objects=['dummy_json_task/todos.csv'],
        destination_project_dataset_table=f"{DATASET_NAME}.{TODOS_OUTPUT}",
        schema_fields=[
                        {
                            "name": "id",
                            "type": "INTEGER",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "todo",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "completed",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "userId",
                            "type": "INTEGER",
                            "mode": "NULLABLE"
                        }],
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )

    business_summary = BigQueryInsertJobOperator(
        task_id="business_summary",
        configuration={
            "query": {
                "query": '''
                    SELECT
                        'Total Users' AS Metric,
                        COUNT(*) AS Value
                    FROM `handy-honor-401601.dummy_json.users`
                    UNION ALL
                    SELECT
                        'Total Products in Carts',
                        COUNT(*)
                    FROM `handy-honor-401601.dummy_json.carts`
                    UNION ALL
                    SELECT
                        'Listed Todos',
                        COUNT(*)
                    FROM `handy-honor-401601.dummy_json.todos`
                    UNION ALL
                    SELECT
                        'Total Posts',
                        COUNT(*)
                    FROM `handy-honor-401601.dummy_json.posts`
                ''',
                'destinationTable': {
                    'projectId': PROJECT_ID,
                    'datasetId': DATASET_NAME,
                    'tableId': SUMMARY_OUTPUT
                },
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

api_check >> [fetch_users_data, fetch_carts_data, fetch_posts_data, fetch_todos_data]
fetch_users_data >> transform_users >> users_stores >> users_to_gcs
fetch_carts_data >> transform_carts >> carts_stores >> carts_to_gcs
fetch_posts_data >> transform_posts >> posts_stores >> posts_to_gcs
fetch_todos_data >> transform_todos >> todos_stores >> todos_to_gcs
[users_to_gcs, carts_to_gcs, posts_to_gcs, todos_to_gcs] >> create_dataset
create_dataset >> [users_load_csv, carts_load_csv, posts_load_csv, todos_load_csv] >> business_summary


# Things that can be improved
## 1. Put the json first into the datalake(gcs), then the rest transform follows -> implementing nested repeated json
## 2. Use task group to visualize the DAG better
## 3. Do some python effeciency for better clean code, and not repeated line