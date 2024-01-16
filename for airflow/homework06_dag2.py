from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag2 = DAG(
    'random_square_dag',
    default_args=default_args,
    description='DAG with PythonOperator generating and squaring a random number',
    schedule_interval='@daily',
)

def generate_and_square_random_number():
    random_number = random.randint(1, 100)
    squared_number = random_number ** 2
    print(f"Random Number: {random_number}, Squared Number: {squared_number}")

t2 = PythonOperator(
    task_id='generate_and_square_random_number',
    python_callable=generate_and_square_random_number,
    dag=dag2,
)
