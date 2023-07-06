from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import os


# Function to ingest CSV file into PySpark DataFrame
def ingest_csv_file():
    spark = SparkSession.builder.appName("CSV Ingestion").getOrCreate()

    # Specify the directory path where the CSV files are located
    directory_path = "/path/to/csv/files/"

    # Iterate over CSV files in the directory
    for file_name in os.listdir(directory_path):
        if file_name.endswith(".csv"):
            csv_path = os.path.join(directory_path, file_name)

            # Read CSV file into DataFrame
            df = spark.read.csv(csv_path, header=True, inferSchema=True)

            # Perform further operations on the DataFrame as needed
            # ...

            # Print DataFrame schema and sample records
            df.printSchema()
            df.show(5, truncate=False)


# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('csv_ingestion', default_args=default_args, schedule_interval=None)

# Define the task
ingest_task = PythonOperator(
    task_id='ingest_csv_file',
    python_callable=ingest_csv_file,
    dag=dag
)

# Set the task dependencies
ingest_task
