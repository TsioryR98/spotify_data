from airflow import DAG
from datetime import datetime
import os

from utils.extract_album_data import extract_album_data
from utils.request_auth_api import request_auth_api
from utils.extract_album_details import extract_album_details

from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("client_id")
CLIENT_SECRET = os.getenv("client_secret")
BASE_OUTPUT_DIR = os.getenv("base_output_dir")


defautls_args = {
    "start_date": datetime(2024, 1, 1),
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="spotify_etl_dag",
    description="ETL 6:00AM daily spotify data",
    schedule="0 6 * * *",
    default_args=defautls_args,
    catchup=False,
    tags=["spotify", "etl"],
) as dag:
    token = request_auth_api(CLIENT_ID, CLIENT_SECRET)

    extract = extract_album_data(
        token=token,
        time_range="2015-2025",
        types="album",
        markets=["US", "FR", "DE", "CA", "BR", "JP", "GB"],
        output_dir=BASE_OUTPUT_DIR,
    )

    extract_details = extract_album_details(
        token=token,
        markets=["US", "FR", "DE", "CA", "BR", "JP", "GB"],
        output_dir=BASE_OUTPUT_DIR,
    )

    token >> extract >> extract_details
