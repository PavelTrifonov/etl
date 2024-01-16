from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import random
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}
union_dag = DAG(
    'union_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
)


def generate_random_number():
    return random.randint(1, 100)


random_number = BashOperator(
    task_id='generate_random_number',
    bash_command='echo "Random Number: $(/usr/bin/python3 -c "import random; print(random.randint(1, 100))")"',
    dag=union_dag,
)


def generate_and_square_random_number():
    random_number = random.randint(1, 100)
    squared_number = random_number ** 2
    print(f"Random Number: {random_number}, Squared Number: {squared_number}")


square_random_number = PythonOperator(
    task_id='generate_and_square_random_number',
    python_callable=generate_and_square_random_number,
    dag=union_dag,
)


def kelvin_to_celsius(kelvin):
    return kelvin - 273.15


def get_weather(api_key, city):
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'lang': 'ru'
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        weather_data = response.json()
        return weather_data
    else:
        print(f"Failed to get weather data. Status code: {response.status_code}")
        return None


api_key = 'b1cf8a29257c7e3105c2b21808038f21'
city = 'Samara'

weather_data = get_weather(api_key, city)

if weather_data:
    temperature_celsius = kelvin_to_celsius(weather_data['main']['temp'])
    res = f'''Погода в {city}: {weather_data['weather'][0]['description']}
    Температура: {temperature_celsius:.2f}°C
    Ветер около: {weather_data['wind']['speed']} м/с
    Давление: {int(weather_data['main']['pressure']/1.333)} мм рт.ст.'''
else:
    res = "Unable to fetch weather data."


def print_weather(**kwargs):
    # Print weather information to the console
    print(res)


info_weather = PythonOperator(
    task_id='send_telegram_message',
    python_callable=print_weather,
    provide_context=True,
    dag=union_dag,
)

info_weather >> square_random_number
square_random_number >> random_number
