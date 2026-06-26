import os
import sqlite3
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

CITY = "Paris"
LATITUDE = 48.8566
LONGITUDE = 2.3522

BASE_URL = "https://api.open-meteo.com/v1/forecast"
DB_PATH = "/opt/airflow/data/weather.db"

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


def _ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            city         TEXT    NOT NULL,
            recorded_at  TEXT    NOT NULL,
            temperature_c  REAL,
            feels_like_c   REAL,
            humidity_pct   INTEGER,
            wind_kph       REAL,
            condition      TEXT,
            dag_run_id     TEXT
        )
    """)
    conn.commit()


def fetch_weather(**context):
    response = requests.get(BASE_URL, params=PARAMS, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"API status  : {response.status_code}")
    print(f"Raw current : {data['current']}")
    context["ti"].xcom_push(key="raw_weather", value=data["current"])


def parse_weather(**context):
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
    print(f"Parsed: {parsed}")
    context["ti"].xcom_push(key="parsed_weather", value=parsed)


def store_weather(**context):
    """Insert today's reading into SQLite — skips if timestamp already exists (idempotent)."""
    w = context["ti"].xcom_pull(task_ids="parse_weather", key="parsed_weather")
    run_id = context["run_id"]

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    _ensure_table(conn)

    # Idempotency check — re-running the DAG for the same interval is safe
    already_exists = conn.execute(
        "SELECT 1 FROM weather WHERE city = ? AND recorded_at = ?",
        (w["city"], w["time"]),
    ).fetchone()

    if already_exists:
        print(f"Record for {w['city']} at {w['time']} already exists — skipping insert.")
    else:
        conn.execute(
            """
            INSERT INTO weather
                (city, recorded_at, temperature_c, feels_like_c,
                 humidity_pct, wind_kph, condition, dag_run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                w["city"], w["time"], w["temperature_c"], w["feels_like_c"],
                w["humidity_pct"], w["wind_kph"], w["condition"], run_id,
            ),
        )
        conn.commit()
        print(f"Inserted record for {w['city']} at {w['time']}.")

    total = conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
    conn.close()
    print(f"Total rows in DB: {total}")


def report_weather(**context):
    """Print the weather summary + last 5 stored readings from the DB."""
    w = context["ti"].xcom_pull(task_ids="parse_weather", key="parsed_weather")

    print(
        f"\n{'=' * 42}\n"
        f"  Weather Report — {w['city']}\n"
        f"  Time       : {w['time']}\n"
        f"  Condition  : {w['condition']}\n"
        f"  Temperature: {w['temperature_c']}°C  (feels like {w['feels_like_c']}°C)\n"
        f"  Humidity   : {w['humidity_pct']}%\n"
        f"  Wind speed : {w['wind_kph']} km/h\n"
        f"{'=' * 42}"
    )

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT recorded_at, temperature_c, condition
        FROM weather
        ORDER BY id DESC
        LIMIT 5
        """
    ).fetchall()
    conn.close()

    print("\n  Last 5 stored readings:")
    print(f"  {'Time':<22} {'Temp':>6}  Condition")
    print(f"  {'-'*22} {'-'*6}  {'-'*20}")
    for recorded_at, temp, condition in rows:
        print(f"  {recorded_at:<22} {temp:>5}°C  {condition}")


def compute_stats():
    """Query aggregate statistics across all stored readings."""
    conn = sqlite3.connect(DB_PATH)
    stats = conn.execute(
        """
        SELECT
            COUNT(*)                        AS total_readings,
            ROUND(MIN(temperature_c), 1)    AS min_temp,
            ROUND(MAX(temperature_c), 1)    AS max_temp,
            ROUND(AVG(temperature_c), 1)    AS avg_temp,
            ROUND(AVG(humidity_pct), 1)     AS avg_humidity,
            ROUND(AVG(wind_kph), 1)         AS avg_wind
        FROM weather
        WHERE city = ?
        """,
        (CITY,),
    ).fetchone()
    conn.close()

    total, min_t, max_t, avg_t, avg_h, avg_w = stats
    print(
        f"\n{'=' * 42}\n"
        f"  All-time stats — {CITY} ({total} readings)\n"
        f"  Temperature : min {min_t}°C  /  max {max_t}°C  /  avg {avg_t}°C\n"
        f"  Humidity    : avg {avg_h}%\n"
        f"  Wind speed  : avg {avg_w} km/h\n"
        f"{'=' * 42}"
    )


with DAG(
    dag_id="weather_fetch",
    default_args=default_args,
    description="Day 3 — fetch, parse, store to SQLite, and report",
    schedule="@daily",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=["learning", "day-3", "weather", "sqlite"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    parse = PythonOperator(
        task_id="parse_weather",
        python_callable=parse_weather,
    )

    store = PythonOperator(
        task_id="store_weather",
        python_callable=store_weather,
    )

    report = PythonOperator(
        task_id="report_weather",
        python_callable=report_weather,
    )

    stats = PythonOperator(
        task_id="compute_stats",
        python_callable=compute_stats,
    )

    fetch >> parse >> store >> report >> stats
