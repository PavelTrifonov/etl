from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
import requests

default_args = {
    'owner': 'airflow',
    'email_on_failure': True,  # Отправлять письмо при ошибке выполнения DAG
    'email': 'pashok_163@mail.ru',
    'start_date': datetime(2024, 1, 10),
    'retries': 1
}
ans_mail = DAG(
    'ans_mail',
    'returns a recommendation how to dress',
    default_args=default_args,
    schedule_interval='*/1 * * * *'
)


def hello():
    return "Hello, Pavel"


email_task = EmailOperator(
    task_id='send_email',
    to='pashok_163@mail.ru',
    subject='Weather now, answer from Airflow',
    html_content='Привет! Это просто текст внутри HTML-письма.',
    dag=ans_mail
)
email_task
