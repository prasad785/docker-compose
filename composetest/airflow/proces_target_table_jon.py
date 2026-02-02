from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# -----------------------
# Python functions
# -----------------------

def extract_data():
    print("Extracting data from source tables")

def transform_data():
    print("Joining and transforming data")

def load_data():
    print("Loading data into target table")

# -----------------------
# DAG Definition
# -----------------------

with DAG(
    dag_id="process_target_table_join",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    # Task order
    extract >> transform >> load
