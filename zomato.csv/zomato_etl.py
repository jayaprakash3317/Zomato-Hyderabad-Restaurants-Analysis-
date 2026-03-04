from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

PROJECT_ID = 'swift-setup-461011-s1'
REGION = 'us-central1'
CLUSTER_NAME = 'zomato-cluster-spark32'

PYSPARK_BUCKET = 'gs://zomato-analysis-cleaninggg/scripts'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

with DAG(
    'zomato_etl',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
) as dag:

    cleaning_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_BUCKET}/zomato_cleaning.py",
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"
            ],
        },
    }

    normalize_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_BUCKET}/normalize_zomato.py",
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"
            ],
        },
    }

    export_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"{PYSPARK_BUCKET}/export_to_bigquery.py",
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar"
            ],
        },
    }

    task_cleaning = DataprocSubmitJobOperator(
        task_id='zomato_cleaning',
        job=cleaning_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    task_normalize = DataprocSubmitJobOperator(
        task_id='zomato_normalize',
        job=normalize_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    task_export = DataprocSubmitJobOperator(
        task_id='zomato_export',
        job=export_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    task_cleaning >> task_normalize >> task_export
