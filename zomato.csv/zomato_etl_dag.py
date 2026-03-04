from airflow import models
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import pubsub_v1, storage
import json

BUCKET_NAME = 'zomato-analysis-cleaninggg'
TOPIC_NAME = 'projects/swift-setup-461011-s1/topics/zomato_csv_topic'
RAW_PREFIX = 'data/'

def detect_and_publish(**context):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=RAW_PREFIX)

    publisher = pubsub_v1.PublisherClient()

    for blob in blobs:
        if blob.name.endswith('.csv'):
       
            if blob.metadata and blob.metadata.get("published") == "true":
                continue

            gcs_path = f'gs://{BUCKET_NAME}/{blob.name}'
            message = json.dumps({'gcs_path': gcs_path}).encode('utf-8')
            publisher.publish(TOPIC_NAME, message)
            print(f"Published message for: {gcs_path}")

            
            metadata = blob.metadata or {}
            metadata["published"] = "true"
            blob.metadata = metadata
            blob.patch()

default_args = {
    'start_date': days_ago(1),
}

with models.DAG(
    'zomato_etl_dag',
    default_args=default_args,
    schedule_interval='*/10 * * * *',  
    catchup=False,
    max_active_runs=1,
) as dag:

    detect_new_files = PythonOperator(
        task_id='detect_and_publish',
        python_callable=detect_and_publish,
    )