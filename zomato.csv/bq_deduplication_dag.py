from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with models.DAG(
    dag_id='bq_deduplication_dag',
    default_args=default_args,
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'deduplication'],
) as dag:


    clean_fact_table = BigQueryInsertJobOperator(
        task_id='clean_fact_table',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `swift-setup-461011-s1.zomato_new.fact_restaurant_data` AS
                    SELECT * EXCEPT(rn) FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY restaurant_id, location_id, cuisine_id
                                ORDER BY rate DESC, votes DESC, approx_cost_for_two DESC
                            ) AS rn
                        FROM `swift-setup-461011-s1.zomato_new.fact_restaurant_data`
                    )
                    WHERE rn = 1;
                """,
                "useLegacySql": False,
            }
        },
        location='asia-south2',
    )

   
    merge_staging_to_final = BigQueryInsertJobOperator(
        task_id='merge_staging_to_final',
        configuration={
            "query": {
                "query": """
MERGE `swift-setup-461011-s1.zomato_new.fact_restaurant_data` T
USING (
    SELECT * EXCEPT(rn) FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY restaurant_id, location_id, cuisine_id
                ORDER BY rate DESC, votes DESC, approx_cost_for_two DESC
            ) AS rn
        FROM `swift-setup-461011-s1.zomato_new.staging_restaurant_data`
    )
    WHERE rn = 1
) S
ON T.restaurant_id = S.restaurant_id
AND T.location_id = S.location_id
AND T.cuisine_id = S.cuisine_id
WHEN MATCHED THEN
    UPDATE SET
        rate = S.rate,
        votes = S.votes,
        approx_cost_for_two = S.approx_cost_for_two,
        ingestion_date = CAST(S.ingestion_date AS DATE)
WHEN NOT MATCHED THEN
    INSERT (
        restaurant_id,
        location_id,
        cuisine_id,
        rate,
        votes,
        approx_cost_for_two,
        ingestion_date
    )
    VALUES (
        S.restaurant_id,
        S.location_id,
        S.cuisine_id,
        S.rate,
        S.votes,
        S.approx_cost_for_two,
        CAST(S.ingestion_date AS DATE)
    );
                """,
                "useLegacySql": False,
            }
        },
        location='asia-south2',
    )

    
    clean_fact_table >> merge_staging_to_final