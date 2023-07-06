from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Function to print "Hello"
def print_hello():
    print("Hello")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('hello_dag', default_args=default_args, schedule_interval=None)

# Define the task
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

# Set the task dependencies
hello_task
