from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from pyspark.sql import SparkSession
import os
import pymysql

default_args = {
    'start_date': datetime(2023, 7, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('extract_transform_load', default_args=default_args, schedule_interval=None)


def extract_data():
    spark = SparkSession.builder.appName('ExtractData').getOrCreate()

    # Replace <csv_file_path> with the path of your CSV file
    csv_file_path = '/path/to/csv/file.csv'

    # Read the CSV file into a DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Save the DataFrame as a temporary table for further processing
    df.createOrReplaceTempView('extracted_data')


def transform_data():
    spark = SparkSession.builder.appName('TransformData').getOrCreate()

    # Retrieve the input file path from the configuration
    input_file_path = "{{ dag_run.conf['input_file_path'] }}"

    # Read the transformed data from the input file into a DataFrame
    df = spark.read.csv(input_file_path, header=True, inferSchema=True)

    # Remove duplicates
    df_no_duplicates = df.dropDuplicates()

    # Replace <transformed_file_path> with the path where you want to save the transformed data
    transformed_file_path = '/path/to/transformed/file.csv'

    # Save the transformed data as a CSV file
    df_no_duplicates.write.csv(transformed_file_path, header=True, mode='overwrite')

    # Pass the transformed file path to the next DAG
    return transformed_file_path


def load_data():
    # Retrieve the transformed file path from the previous task's output
    transformed_file_path = "{{ task_instance.xcom_pull(task_ids='transform_data') }}"

    # Define the MySQL connection details
    mysql_conn_id = 'your_mysql_connection_id'
    mysql_table = 'your_table_name'

    # Establish MySQL connection
    connection = pymysql.connect(
        host='your_mysql_host',
        port=your_mysql_port,
        user='your_mysql_user',
        password='your_mysql_password',
        database='your_mysql_database'
    )

    try:
        with connection.cursor() as cursor:
            # Truncate the table before loading data
            truncate_query = f"TRUNCATE TABLE {mysql_table}"
            cursor.execute(truncate_query)

            # Load data from the transformed file into the MySQL table
            load_query = f"""
                LOAD DATA INFILE '{transformed_file_path}'
                INTO TABLE {mysql_table}
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 ROWS
            """
            cursor.execute(load_query)

        connection.commit()

    finally:
        connection.close()


extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag
)

extract_data_task >> transform_data_task >> load_data_task
