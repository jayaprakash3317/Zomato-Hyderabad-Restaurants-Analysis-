                                                  ########## Phase-1 :  Zomato Bangalore Restaurants Analysis ###########

Overview
This project analyzes Zomato restaurant data from Bangalore to understand dining preferences, ratings, and pricing trends. Using Python, SQL, Hadoop FS, Hive, PySpark, and Google BigQuery, we process and analyze restaurant data to provide insights for restaurateurs and food platforms.

Dataset
The dataset, sourced from Kaggle, contains restaurant details with columns like Restaurant_ID, Name, Location, Cuisine, Rating, and Price_Range.

Workflow
1.	Data Ingestion with Hadoop FS:
–	Use Python to download the dataset and upload it to Hadoop Distributed File System (HDFS) for scalable storage.
2.	Data Storage and Schema Definition with Hive:
–	Create a Hive external table, defining the schema (e.g., Restaurant_ID as STRING, Rating as FLOAT, Price_Range as STRING).
–	Use HiveQL to load data from HDFS into the table.
3.	Data Processing with PySpark:
–	Use PySpark to clean data, handling missing values, standardizing cuisine types, and removing duplicates.
–	Perform aggregations to calculate average ratings by cuisine and location.
4.	Advanced Analytics with BigQuery:
–	Export processed data to Google BigQuery for advanced SQL-based analytics.
–	Use Python’s google-cloud-bigquery library to load data into a BigQuery table.
–	Run SQL queries to analyze rating trends by price range and location.
5.	Visualization with Python:
–	Use Matplotlib and Seaborn to visualize results, such as rating distributions or cuisine popularity.

Outcomes
•	Identify high-rated cuisines and locations.
•	Highlight pricing trends and their impact on ratings.
•	Provide insights for restaurant marketing and menu planning.

Tools
•	Python: Data ingestion, preprocessing, and visualization.
•	SQL: Querying in Hive and BigQuery for analytics.
•	Hadoop FS: Scalable storage for raw data.
•	Hive: Structured data management and initial querying.
•	PySpark: Large-scale data processing and transformation.
•	BigQuery: Cloud-based analytics for complex queries.


                                               
                                              ########## Phase-2 :  Zomato Bangalore Restaurants Analysis (Automating Process) ###########

Objective
In this iteration, students will design and implement a modern, serverless, and orchestrated ETL (Extract, Transform, Load) data pipeline leveraging key Google Cloud Platform (GCP) services. The pipeline will handle ingestion of CSV data files related to the P1 project, which are periodically uploaded to Google Cloud Storage (GCS).
A Cloud Composer (Apache Airflow) workflow will be scheduled to monitor and detect these incoming files. On detection, the workflow will publish metadata to a Pub/Sub topic. This triggers a Dataflow (Apache Beam) job that ingests and transforms the CSV data and writes it into a BigQuery table for downstream analytics and reporting.
This pipeline promotes modularity, automation, and scalability—essential qualities for cloud-native data engineering.

Workflow:

1. Data Source Setup (GCS)
•	Create a Cloud Storage bucket.
•	Upload sample CSV files periodically to simulate real-time ingestion.
•	File naming pattern: data_<timestamp>.csv

2. Orchestration (Cloud Composer)
•	Create a Cloud Composer environment.
•	Write an Apache Airflow DAG:
o	Schedule it (e.g., every 10 minutes).
o	Check for new CSV files in the GCS bucket using GCS Sensor or custom logic.
o	When a file is found, publish a message to a Pub/Sub topic containing the metadata

3. Messaging Layer (Pub/Sub)
•	Create a Pub/Sub topic.
•	The Airflow DAG should publish messages to this topic containing file metadata..
•	Create a Pub/Sub subscription that the Dataflow job will listen to for triggering the pipeline.

4. Transformation & Ingestion (Dataflow)
Create a Dataflow pipeline using Apache Beam (Python):
•	Consume messages from the Pub/Sub subscription.
•	Parse CSV, validate/clean data.
•	Transform (e.g., type casting, formatting).
•	Write the final, clean data to a BigQuery table in append mode.

5. Data Warehouse (BigQuery)
•	Create a destination table with schema matching the CSV data.
•	Partition by date or timestamp if required.

6. Create the airflow Dag for Dataflow pipeline (schedule it for every 10 mins if 1st job success, it means if new file added then only second job should run )

7.Optional: create email alerts for job start and finish(success or fail) using pub/sub service

TOOLS & SERVICES
Component    	Technology
File Storage	Cloud Storage
Scheduler	Cloud Composer (Airflow)
Messaging	Pub/Sub
ETL	Dataflow (Apache Beam)
Data Storage	BigQuery
Language	Python 




                                                           
                                                
