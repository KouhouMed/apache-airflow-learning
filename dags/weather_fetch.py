from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

CITY = "Casablanca"
LATITUDE = 33.5731
LONGITUDE = -7.5898

BASE_URL = "https://api.open-meteo.com/v1/forecast"

PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "current": [
        "temperature_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
        "apparent_temperature",
    ],
    "timezone": "auto",
}

default_args = {
    "owner": "weatherflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# WMO weather code descriptions (subset)
WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 99: "Thunderstorm with hail",
}


def fetch_weather(**context):
    """Call Open-Meteo API and push raw response to XCom."""
    response = requests.get(BASE_URL, params=PARAMS, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"API response status : {response.status_code}")
    print(f"Raw current weather : {data['current']}")
    # Push to XCom so downstream tasks can read it
    context["ti"].xcom_push(key="raw_weather", value=data["current"])
    return data["current"]


def parse_weather(**context):
    """Pull raw data from XCom and extract clean fields."""
    raw = context["ti"].xcom_pull(task_ids="fetch_weather", key="raw_weather")
    parsed = {
        "city": CITY,
        "time": raw["time"],
        "temperature_c": raw["temperature_2m"],
        "feels_like_c": raw["apparent_temperature"],
        "humidity_pct": raw["relative_humidity_2m"],
        "wind_kph": raw["wind_speed_10m"],
        "condition": WMO_CODES.get(raw["weather_code"], f"Code {raw['weather_code']}"),
    }
    print(f"Parsed fields: {parsed}")
    context["ti"].xcom_push(key="parsed_weather", value=parsed)
    return parsed


def summarize_weather(**context):
    """Print a human-readable weather summary."""
    w = context["ti"].xcom_pull(task_ids="parse_weather", key="parsed_weather")
    summary = (
        f"\n{'=' * 40}\n"
        f"  Weather Report — {w['city']}\n"
        f"  Date/time  : {w['time']}\n"
        f"  Condition  : {w['condition']}\n"
        f"  Temperature: {w['temperature_c']}°C (feels like {w['feels_like_c']}°C)\n"
        f"  Humidity   : {w['humidity_pct']}%\n"
        f"  Wind speed : {w['wind_kph']} km/h\n"
        f"{'=' * 40}"
    )
    print(summary)


with DAG(
    dag_id="weather_fetch",
    default_args=default_args,
    description="Day 2 — fetch live weather from Open-Meteo and print a summary",
    schedule="@daily",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=["learning", "day-2", "weather", "api"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    parse = PythonOperator(
        task_id="parse_weather",
        python_callable=parse_weather,
    )

    summarize = PythonOperator(
        task_id="summarize_weather",
        python_callable=summarize_weather,
    )

    fetch >> parse >> summarize
