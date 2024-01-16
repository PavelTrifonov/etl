import requests

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
    print(f"Погода в {city}: {weather_data['weather'][0]['description']}")
    print(f"Температура: {temperature_celsius:.2f}°C")
    print(f"Ветер около: {weather_data['wind']['speed']} м/с")
    print(f"Давление: {int(weather_data['main']['pressure']/1.333)} мм рт.ст.")
else:
    print("Unable to fetch weather data.")
