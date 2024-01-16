from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag1 = DAG(
    'random_number_dag',
    default_args=default_args,
    description='DAG with BashOperator generating a random number',
    schedule_interval='@daily',
)

def generate_random_number():
    return random.randint(1, 100)

t1 = BashOperator(
    task_id='generate_random_number',
    bash_command='echo "Random Number: $(/usr/bin/python3 -c "import random; print(random.randint(1, 100))")"',
    dag=dag1,
)
