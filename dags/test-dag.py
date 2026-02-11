from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  
from datetime import datetime, timedelta


def print_hello():
    print("Hello from Airflow 3.0!")
    return "Task completed successfully"

def print_goodbye():
    print("Goodbye from Airflow 3.0!")
    return "Goodbye task completed"

# Define default arguments
default_args = {
    'owner': 'pragadeesh',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG for Airflow 3.0',
    schedule=timedelta(minutes=5),
    catchup=False,
)

# Define tasks
hello_task = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=dag,
)

goodbye_task = PythonOperator(
    task_id='say_goodbye',
    python_callable=print_goodbye,
    dag=dag,
)

# Set task dependencies
hello_task >> goodbye_task