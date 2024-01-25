from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Параметры подключения к PostgreSQL
POSTGRES_CONNECTION = {
    'host': 'localhost',
    'port': '5432',
    'database': 'pavel_airflow_db',
    'user': 'pavel_airflow',
    'password': '6161',
}

# Проверка наличия базы данных
check_database_query = f"SELECT 1 FROM pg_database WHERE datname='{POSTGRES_CONNECTION['database']}'"

# Создание базы данных, если ее нет
create_database_query = f"CREATE DATABASE {POSTGRES_CONNECTION['database']}"

# Определение параметров DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_postgres= DAG(
    'check_and_create_database_with_operator',
    default_args=default_args,
    schedule_interval=None,
)

# Определение задачи с использованием PostgresOperator для проверки наличия базы данных
check_database_task = PostgresOperator(
    task_id='check_database',
    sql=check_database_query,
    postgres_conn_id='pavel_a_s',  # Укажите свой идентификатор соединения с PostgreSQL
    dag=dag_postgres,
)

# Определение задачи с использованием PostgresOperator для создания базы данных
create_database_task = PostgresOperator(
    task_id='create_database',
    sql=create_database_query,
    postgres_conn_id='pavel_a_s',  # Укажите свой идентификатор соединения с PostgreSQL
    dag=dag_postgres,
)

# Установка порядка выполнения задач
check_database_task >> create_database_task

if __name__ == "__main__":
    dag_postgres.cli()
