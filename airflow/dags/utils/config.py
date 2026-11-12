import os
from airflow.datasets import Dataset
import logging
PROCESSED_DATA_DATASET = Dataset("/opt/airflow/data/output.csv")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
INPUT_FILE = os.path.join(BASE_DIR, "data", "tiktok_google_play_reviews.csv")
OUTPUT_FILE_1 = os.path.join(BASE_DIR, "data", "output1.csv")
OUTPUT_FILE_2 = os.path.join(BASE_DIR, "data", "output2.csv")
OUTPUT_FILE_FINAL = os.path.join(BASE_DIR, "data", "output_final.csv")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
