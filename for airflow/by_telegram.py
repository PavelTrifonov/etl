from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.telegram.operators.telegram import TelegramOperator


# Замените 'YOUR_BOT_TOKEN' и 'YOUR_CHAT_ID' на свои значения
BOT_TOKEN = '5738351665:AAHvY5xb6nALwxFYZxOXVdo5iH1FSmTCwFg'
CHAT_ID = '5615077468'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

by_telegram = DAG(
    'telegram_notification_dag',
    default_args=default_args,
    description='A simple DAG to send a message to Telegram',
    schedule_interval=timedelta(days=1),
)


telegram_message_task = TelegramOperator(
    task_id='send_telegram_message',
    text='Hello from Airflow!',
    chat_id=CHAT_ID,
    token=BOT_TOKEN,
    dag=by_telegram,
)

telegram_message_task