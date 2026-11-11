from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.datasets import Dataset

PROCESSED_DATA_DATASET = Dataset("/opt/airflow/data/output.csv")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "output.csv")

def load_csv_to_mongo():
    if not os.path.exists(OUTPUT_FILE):
        raise FileNotFoundError(f"Файл не найден: {OUTPUT_FILE}")
        
    df = pd.read_csv(OUTPUT_FILE)
    records = df.to_dict('records')
    
    if not records:
        print("Файл не содержит записей.")
        return

    hook = MongoHook(mongo_conn_id="mongo_default")
    
    client = hook.get_conn()
    db = client.get_database()
    collection = db["comments"]
    
    result = collection.insert_many(records)
    print(f"Успешно загружено {len(result.inserted_ids)} документов в MongoDB.")
    
    client.close()

with DAG(
    dag_id="2_load_to_mongodb",
    start_date=datetime(2023, 1, 1),
    schedule=[PROCESSED_DATA_DATASET],
    catchup=False,
    tags=["second_dag", "mongodb_loader"]
) as dag:

    load_task = PythonOperator(
        task_id="load_data_to_mongo",
        python_callable=load_csv_to_mongo
    )
    
