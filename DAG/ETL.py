from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


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
    df = df.dropDuplicates(['Name', 'IBAN'])


    # Replace <transformed_file_path> with the path where you want to save the transformed data
    transformed_file_path = '/opt/airflow/logs/cleaned_file.csv'

    # Assign unique 'id' to any duplicated rows
    df_no_duplicates = df.withColumn('ID', monotonically_increasing_id())
    # Save the transformed data as a CSV file
    df_no_duplicates.coalesce(1).write.csv(transformed_file_path, header=True, mode='overwrite')

    # Pass the transformed file path to the next DAG
    return transformed_file_path


def load_data(**kwargs):

    spark = SparkSession.builder.appName('EtlTask').getOrCreate()

    # Retrieve the transformed file path from the previous task's output
    task_instance = kwargs['task_instance']
    transformed_file_path = task_instance.xcom_pull(task_ids='transform_data_task')

    # Define the MySQL connection details





    df = spark.read.csv(transformed_file_path, header=True, inferSchema=True)


    # Insert data row by row
    df.foreachPartition(lambda rows: print(rows))


def insert_rows(rows):
    connection = mysql.connect(
        host='192.168.110.166',
        port=3306,
        user='sa',
        password='zZ123*321',
        db='test'
    )
    mysql_table = 'tbltest'
    cursor = connection.cursor()

    insert_query = f"INSERT INTO {mysql_table}  (id, name, iban) VALUES (%s, %s, %s)"
    for row in rows:
        cursor.execute(insert_query, (row.ID, row.Name, row.IBAN))

    connection.commit()
    cursor.close()
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
