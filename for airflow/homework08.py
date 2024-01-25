import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator


# создадим словарь с аргументами для будущего Dag
default_arguments = {
    'owner': 'airflow',
    'email_on_failure': True,
    'email': 'pashok_163@mail.ru',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# создаем экземпляр DAG
etl_dag = DAG(
    'elt_data',
    'Returns a changed table',
    default_args=default_arguments,
    schedule_interval=None
)


def load(path: str):
    return pd.read_csv(path)


# создадим dataframes из загруженных csv файлов
booking_task = PythonOperator(
    task_id='load_booking',
    python_callable=load,
    op_args=["/root/airflow/dags/booking.csv"],
    provide_context=True,
    dag=etl_dag
)
client_task = PythonOperator(
    task_id='load_client',
    python_callable=load,
    op_args=["/root/airflow/dags/client.csv"],
    provide_context=True,
    dag=etl_dag
)
hotel_task = PythonOperator(
    task_id='load_hotel',
    python_callable=load,
    op_args=["/root/airflow/dags/hotel.csv"],
    provide_context=True,
    dag=etl_dag
)


# создаем функцию для трансформации полученных дата фреймов
def transformation(**kwargs):
    # переведем результат в дата фрейм
    ti = kwargs['ti']
    task_instances = ti.xcom_pull(task_ids=['load_booking', 'load_client', 'load_hotel'])
    booking_df, client_df, hotel_df = task_instances

    union_table = pd.merge(booking_df, client_df, on="client_id", how="outer")
    union_table = pd.merge(union_table, hotel_df, on="hotel_id", how="outer")

    union_table = union_table.drop_duplicates()
    union_table = union_table.dropna()

    union_table["booking_date"] = union_table["booking_date"].apply(
        lambda x: str(x).replace("/", "-"))

    union_table.loc[union_table['currency'] == "GBP", 'booking_cost'] = \
        union_table['booking_cost'] * 1.16
    union_table["currency"] = union_table["currency"].apply(
        lambda x: str(x).replace("GBP", "EUR"))

    return union_table


transform_frame = PythonOperator(
    task_id="transform_frame",
    python_callable=transformation,
    provide_context=True,
    dag=etl_dag
)


# Параметры подключения к PostgreSQL
POSTGRES_CONNECTION = {
    'host': 'localhost',
    'port': '5432',
    'database': 'pavel_airflow_db',
    'user': 'pavel_airflow',
    'password': '6161',
}


def load_dataframe_to_postgres(**kwargs):
    ti = kwargs['ti']
    transform_result = ti.xcom_pull(task_ids='transform_frame')

    # переведем результат в DataFrame
    df = transform_result

    # Подключение к PostgreSQL
    engine = f"postgresql+psycopg2://{POSTGRES_CONNECTION['user']}:{POSTGRES_CONNECTION['password']}@{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}"

    # Загрузка данных в базу данных с использованием Pandas
    df.to_sql('union_table', con=engine, index=False, if_exists='replace')


load_dataframe_task = PythonOperator(
    task_id='load_dataframe_to_postgres',
    python_callable=load_dataframe_to_postgres,
    provide_context=True,
    dag=etl_dag,
)
sql_query = "SELECT * FROM union_table;"

# Create a PostgresOperator to execute the SQL query
print_union_table = PostgresOperator(
    task_id='print_union_table',
    sql=sql_query,
    postgres_conn_id='my_postgres_conn',
    autocommit=True,
    dag=etl_dag
)

# Задачи могут быть определены и связаны друг с другом по логике выполнения
[booking_task, client_task, hotel_task] >> transform_frame
transform_frame >> load_dataframe_task
load_dataframe_task >> print_union_table
