from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import pymysql as mysql

default_args = {
    'start_date': datetime(2023, 7, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('extract_transform_load', default_args=default_args, schedule_interval=None)


def extract_data():
    spark = SparkSession.builder.appName('EtlTask').getOrCreate()

    # Replace <csv_file_path> with the path of your CSV file
    csv_file_path = '/opt/airflow/logs/sample.csv'

    # Read the CSV file into a DataFrame
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Save the DataFrame as a temporary table for further processing
    df.createOrReplaceTempView('extracted_data')


def transform_data():
    spark = SparkSession.builder.appName('EtlTask').getOrCreate()
    # Retrieve the data from the 'extracted_data' table
    extracted_data = spark.table("extracted_data")

    # Show the data
    extracted_data.show()

    # Retrieve the input file path from the configuration

    df = spark.sql("SELECT * FROM extracted_data")  # Add your SQL query here

    # Read the transformed data from the input file into a DataFrame
   # df = spark.read.csv(input_file_path, header=True, inferSchema=True)

    # Remove duplicates
    df_no_duplicates = df.dropDuplicates()

    # Replace <transformed_file_path> with the path where you want to save the transformed data
    transformed_file_path = '/opt/airflow/logs/cleaned_file.csv'

    # Save the transformed data as a CSV file
    df_no_duplicates.coalesce(1).write.csv(transformed_file_path, header=True, mode='overwrite')

    # Pass the transformed file path to the next DAG
    return transformed_file_path


def load_data():
    # Retrieve the transformed file path from the previous task's output
    transformed_file_path = "{{ task_instance.xcom_pull(task_ids='transform_data') }}"

    # Define the MySQL connection details

    mysql_table = 'tbltest'

    # Establish MySQL connection
    connection = mysql.connect(
        host='192.168.110.166',
        port=3306,
        user='sa',
        password='zZ123*321',
        database='test'
    )

    try:
        with connection.cursor() as cursor:
            # Truncate the table before loading data
            truncate_query = f"TRUNCATE TABLE {mysql_table}"
            cursor.execute(truncate_query)

            # Load data from the transformed file into the MySQL table
            load_query =  "LOAD DATA INFILE "+transformed_file_path+ "INTO TABLE "+mysql_table "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS"
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
