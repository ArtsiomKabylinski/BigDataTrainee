# filename: process_csv_dag.py
from datetime import datetime
import os
import pandas as pd
import re
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

PROCESSED_DATA_DATASET = Dataset("/opt/airflow/data/output.csv")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "data", "tiktok_google_play_reviews.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "output.csv")

def replace_nulls_func():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"Файл не найден: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    df = df.replace(['null', 'NULL', 'None', None], '-')
    df = df.fillna('-')
    df.to_csv(OUTPUT_FILE, index=False)
    print("Nulls replaced.")

def sort_data_func():
    if not os.path.exists(OUTPUT_FILE):
        raise FileNotFoundError(f"Файл не найден: {OUTPUT_FILE}")
    df = pd.read_csv(OUTPUT_FILE)
    if 'created_date' in df.columns:
        df["created_date"] = pd.to_datetime(
            df["created_date"], errors="coerce"
        )
        df = df.sort_values(by="created_date")
    df.to_csv(OUTPUT_FILE, index=False)
    print("Data sorted by created_date.")

def clean_content_func():
    if not os.path.exists(OUTPUT_FILE):
        raise FileNotFoundError(f"Файл не найден: {OUTPUT_FILE}")
    df = pd.read_csv(OUTPUT_FILE)
    if 'content' in df.columns:
        df['content'] = df['content'].astype(str).apply(
            lambda x: re.sub(r"[^\w\s.,!?;:'-]", "", x)
        )
    df.to_csv(OUTPUT_FILE, index=False)
    print("Content cleaned.")

with DAG(
    dag_id="1_process_csv_file",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["csv_processing", "first_dag"]
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_csv",
        filepath=INPUT_FILE,
        poke_interval=10,
        timeout=600,
        mode="reschedule",
    )

    @task.branch(task_id="check_file_empty")
    def check_file_empty() -> str:
        if not os.path.exists(INPUT_FILE) or os.path.getsize(INPUT_FILE) == 0:
            return "log_empty_file"
        return "transform_group.replace_nulls"

    branch_task = check_file_empty()

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "Внимание: Файл пустой. Обработка остановлена."',
    )

    with TaskGroup("transform_group") as transform_group:
        t1_replace = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls_func
        )
        
        t2_sort = PythonOperator(
            task_id="sort_data",
            python_callable=sort_data_func
        )

        t3_clean = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content_func,
            outlets=[PROCESSED_DATA_DATASET]
        )

        t1_replace >> t2_sort >> t3_clean


    wait_for_file >> branch_task
    branch_task >> log_empty_file
    branch_task >> transform_group
    