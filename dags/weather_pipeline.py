from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import psycopg2

API_KEY = "..."

CITIES = [
    {"name": "Tokyo", "country": "Japan", "lat": 35.6895, "lon": 139.6917},
    {"name": "New York", "country": "USA", "lat": 40.7128, "lon": -74.0060},
    {"name": "Paris", "country": "France", "lat": 48.8566, "lon": 2.3522}
]

def fetch_and_insert_weather():

    for city_info in CITIES:
        name = city_info["name"]
        country = city_info["country"]
        latitude = city_info["lat"]
        longitude = city_info["lon"]

        url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={latitude},{longitude}"
        response = requests.get(url)
        data = response.json()

        location = data["location"]
        current = data["current"]

        city = location["name"]
        country = location["country"]
        latitude = location["lat"]
        longitude = location["lon"]
        observation_time = location["localtime"]

        temp_c = current["temp_c"]
        feelslike_c = current["feelslike_c"]
        wind_kph = current["wind_kph"]
        wind_dir = current["wind_dir"]
        humidity = current["humidity"]
        cloud = current["cloud"]
        is_day = current["is_day"]
        weather_description = current["condition"]["text"]

        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        insert_query = """
            INSERT INTO weather_data
            (city, country, latitude, longitude, observation_time, temp_c, feelslike_c,
            wind_kph, wind_dir, humidity, cloud, is_day, weather_description)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(insert_query, (
            city, country, latitude, longitude, observation_time,
            temp_c, feelslike_c, wind_kph, wind_dir, humidity,
            cloud, is_day, weather_description
        ))
        conn.commit()
        cursor.close()
        conn.close()

with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2026, 2, 27),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task_fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_and_insert_weather
    )