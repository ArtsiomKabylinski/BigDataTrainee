# filename: process_csv_dag.py
from datetime import datetime
import os
import pandas as pd
import re
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import BranchPythonOperator
from utils.config import INPUT_FILE, OUTPUT_FILE_1, PROCESSED_DATA_DATASET, logger, OUTPUT_FILE_FINAL, OUTPUT_FILE_2

def replace_nulls_func():
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"File not found: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, keep_default_na=False, na_values=[])
    df = df.replace(['null', 'NULL', 'None', 'none', 'NONE', ''], '-')
    df = df.fillna('-')
    df.to_csv(OUTPUT_FILE_1, index=False)
    logger.info("Nulls replaced.")

def check_file_empty() -> str:
    try:
        df = pd.read_csv(INPUT_FILE, nrows=1)
        if df.empty:
            logger.info("File is empty.")
            return "log_empty_file"
            
    except pd.errors.EmptyDataError:
        logger.info("File is completely empty (0 bytes).")
        return "log_empty_file"
    except FileNotFoundError:
        logger.error(f"File not found: {INPUT_FILE}")
        return "log_empty_file"
    
    logger.info("File is not empty. Proceeding with processing.")
    return "transform_group.replace_nulls"

def sort_data_func():
    if not os.path.exists(OUTPUT_FILE_1):
        raise FileNotFoundError(f"File not found: {OUTPUT_FILE_1}")
    df = pd.read_csv(OUTPUT_FILE_1)
    if 'created_date' in df.columns:
        df["created_date"] = pd.to_datetime(
            df["created_date"], errors="coerce"
        )
        df = df.sort_values(by="created_date")
    df.to_csv(OUTPUT_FILE_2, index=False)
    logger.info("Data sorted by created_date.")

def clean_content_func():
    if not os.path.exists(OUTPUT_FILE_2):
        raise FileNotFoundError(f"File not found: {OUTPUT_FILE_2}")
    df = pd.read_csv(OUTPUT_FILE_2)
    if 'content' in df.columns:
        df['content'] = df['content'].astype(str).apply(
            lambda x: re.sub(r"[^\w\s.,!?;:'-]", "", x)
        )
    df.to_csv(OUTPUT_FILE_FINAL, index=False)
    logger.info("Content cleaned.")

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

    check_file_emptynes = BranchPythonOperator(
        task_id="check_file_empty",
        python_callable=check_file_empty
    )

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command='echo "Warning: File is empty. Processing stopped."',
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


    wait_for_file >> check_file_emptynes
    check_file_emptynes >> log_empty_file
    check_file_emptynes >> transform_group
    