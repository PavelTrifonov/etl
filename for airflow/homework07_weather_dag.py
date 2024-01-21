from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
import requests

default_args = {
    'owner': 'airflow',
    'email_on_failure': True,
    'email': 'pashok_163@mail.ru',
    'start_date': datetime(2024, 1, 10),
    'retries': 1
}

temperature = DAG(
    'temperature',
    'returns a recommendation on how to dress',
    default_args=default_args,
    schedule_interval='*/30 * * * *'
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
        return response.json()
    else:
        print(f"Failed to get weather data. Status code: {response.status_code}")
        return None


api_key = 'b1cf8a29257c7e3105c2b21808038f21'
city = 'Samara'

weather_data = get_weather(api_key, city)

if weather_data:
    temperature_celsius = kelvin_to_celsius(weather_data['main']['temp'])
    res = f'''
            Погода в {city}: {weather_data['weather'][0]['description']} <br>
            Температура: {temperature_celsius:.2f}°C<br>
            Ветер около: {weather_data['wind']['speed']} м/с <br>
            Давление: {int(weather_data['main']['pressure']/1.333)} мм рт.ст.'''
else:
    res = "Unable to fetch weather data."


def print_weather(**kwargs):
    # Print weather information to the console
    print(res)
    return res


def print_warm(**kwargs):
    print("На улице тепло, можно одеться полегче 😉")
    return "warm"


def print_cold(**kwargs):
    print("На улице холодно, оденься теплее!!! 🥶")
    return "cold"


def decide_branch(**kwargs):
    # Function to determine the branch based on conditions
    if temperature_celsius > 15:
        return 'warm'
    else:
        return 'cold'


def recommendation():
    if temperature_celsius > 15:
        return "На улице тепло, можно одеться полегче 😉"
    return "На улице холодно, оденься теплее!!! 🥶"


weather_now = PythonOperator(
    task_id='weather_now',
    python_callable=print_weather,
    provide_context=True,
    dag=temperature
)

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_branch,
    provide_context=True,
    dag=temperature
)

warm = PythonOperator(
    task_id='warm',
    python_callable=print_warm,
    provide_context=True,
    dag=temperature
)

cold = PythonOperator(
    task_id='cold',
    python_callable=print_cold,
    provide_context=True,
    dag=temperature
)

email_task = EmailOperator(
    task_id='send_email',
    to=['pashok_163@mail.ru'],
    subject='Weather now, answer from Airflow',
    html_content=f'{res}<br> {recommendation()}',
    dag=temperature,
    trigger_rule='none_failed_min_one_success'
)

# Define the order of task execution by setting dependencies
weather_now >> branch_task
branch_task >> [warm, cold]
warm >> email_task
cold >> email_task
