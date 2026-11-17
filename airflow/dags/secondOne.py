from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from utils.config import PROCESSED_DATA_DATASET,logger, OUTPUT_FILE_FINAL
from pymongo.errors import BulkWriteError

def load_csv_to_mongo():
    if not os.path.exists(OUTPUT_FILE_FINAL):
        raise FileNotFoundError(f"File not found: {OUTPUT_FILE_FINAL}")
        
    df = pd.read_csv(OUTPUT_FILE_FINAL)
    records = df.to_dict('records')
    
    if not records:
        logger.info("File is empty.")
        return

    for r in records:
        r.pop("_id", None)
        
    hook = MongoHook(mongo_conn_id="mongo_default")
    
    with hook.get_conn() as client:
        db = client.get_database()
        collection = db["comments"]
        try:
            result = collection.insert_many(records, ordered=False)
            logger.info("Successfully loaded %d documents into MongoDB.",len(result.inserted_ids))
        except BulkWriteError as e:
            inserted = e.details.get("nInserted", 0)
            dup_errors = [
                err for err in e.details.get("writeErrors", [])
                if err.get("code") == 11000
            ]
            logger.warning("Inserted %d documents, skipped %d duplicates.",inserted,len(dup_errors))

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
    
