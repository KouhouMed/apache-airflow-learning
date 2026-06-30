import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

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

REQUIRED_FIELDS = [
    "temperature_2m", "apparent_temperature",
    "relative_humidity_2m", "wind_speed_10m", "weather_code", "time",
]


# ---------------------------------------------------------------------------
# Callbacks — applied to every task via default_args
# ---------------------------------------------------------------------------

def notify_on_failure(context):
    ti = context["task_instance"]
    print(
        f"\n{'!' * 46}\n"
        f"  TASK FAILED\n"
        f"  DAG    : {ti.dag_id}\n"
        f"  Task   : {ti.task_id}\n"
        f"  Run ID : {context['run_id']}\n"
        f"  Date   : {context.get('logical_date')}\n"
        f"  Error  : {context.get('exception')}\n"
        f"{'!' * 46}\n"
        f"  → In production: trigger Slack / email / PagerDuty here."
    )


def notify_on_retry(context):
    ti = context["task_instance"]
    print(
        f"  RETRY {ti.try_number}/{ti.max_tries + 1} — "
        f"Task: {ti.task_id} — next attempt in {context['task'].retry_delay}"
    )


default_args = {
    "owner": "weatherflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,   # delay doubles each retry: 1m, 2m, 4m
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_on_failure,
    "on_retry_callback": notify_on_retry,
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
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city            TEXT    NOT NULL,
            recorded_at     TEXT    NOT NULL,
            temperature_c   REAL,
            feels_like_c    REAL,
            humidity_pct    INTEGER,
            wind_kph        REAL,
            condition       TEXT,
            dag_run_id      TEXT,
            temp_category   TEXT,
            wind_category   TEXT,
            comfort_score   REAL
        )
    """)
    conn.commit()


def _migrate_table(conn):
    """Add Day 4 columns to existing DB without wiping data."""
    new_columns = [
        ("temp_category", "TEXT"),
        ("wind_category", "TEXT"),
        ("comfort_score", "REAL"),
    ]
    for col, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE weather ADD COLUMN {col} {col_type}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists


def check_if_fetched(**context):
    """Branch: skip the pipeline if today's data is already in the DB."""
    today = context["ds"]  # YYYY-MM-DD, e.g. "2026-06-28"

    if not os.path.exists(DB_PATH):
        print(f"DB not found — first run, proceeding with fetch.")
        return "fetch_weather"

    conn = sqlite3.connect(DB_PATH)
    try:
        exists = conn.execute(
            "SELECT 1 FROM weather WHERE city = ? AND recorded_at LIKE ?",
            (CITY, f"{today}%"),
        ).fetchone()
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        exists = None
    finally:
        conn.close()

    if exists:
        print(f"Data for {CITY} on {today} already in DB — skipping fetch.")
        return "already_fetched"

    print(f"No data for {CITY} on {today} — proceeding with fetch.")
    return "fetch_weather"


def already_fetched():
    print("Pipeline skipped: today's weather data already stored.")


def fetch_weather(**context):
    response = requests.get(BASE_URL, params=PARAMS, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"API status  : {response.status_code}")
    print(f"Raw current : {data['current']}")
    context["ti"].xcom_push(key="raw_weather", value=data["current"])


def validate_response(**context):
    """Raise ValueError if the API response is missing any required field."""
    raw = context["ti"].xcom_pull(task_ids="fetch_weather", key="raw_weather")

    if not raw:
        raise ValueError("XCom payload from fetch_weather is empty.")

    missing = [f for f in REQUIRED_FIELDS if f not in raw]
    if missing:
        raise ValueError(f"API response missing required fields: {missing}")

    print(f"Validation passed — all {len(REQUIRED_FIELDS)} required fields present.")


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


def transform_weather(**context):
    """Use pandas to derive temp_category, wind_category, and comfort_score."""
    parsed = context["ti"].xcom_pull(task_ids="parse_weather", key="parsed_weather")

    df = pd.DataFrame([parsed])

    df["temp_category"] = pd.cut(
        df["temperature_c"],
        bins=[-float("inf"), 0, 10, 18, 25, float("inf")],
        labels=["Freezing", "Cold", "Mild", "Warm", "Hot"],
    ).astype(str)

    df["wind_category"] = pd.cut(
        df["wind_kph"],
        bins=[-float("inf"), 5, 20, 40, float("inf")],
        labels=["Calm", "Breeze", "Windy", "Strong"],
    ).astype(str)

    # Comfort score 0–100: penalises distance from ideal temp (21°C) and excess humidity
    def _comfort(temp, humidity):
        temp_penalty = abs(temp - 21) * 3
        humidity_penalty = max(0, humidity - 60) * 0.4
        return round(max(0.0, 100 - temp_penalty - humidity_penalty), 1)

    df["comfort_score"] = df.apply(
        lambda r: _comfort(r["temperature_c"], r["humidity_pct"]), axis=1
    )

    enriched = df.iloc[0].to_dict()
    print(f"\n  Derived fields:")
    print(f"    temp_category : {enriched['temp_category']}")
    print(f"    wind_category : {enriched['wind_category']}")
    print(f"    comfort_score : {enriched['comfort_score']} / 100")

    context["ti"].xcom_push(key="enriched_weather", value=enriched)


def store_weather(**context):
    """Insert enriched reading into SQLite — idempotent on (city, recorded_at)."""
    w = context["ti"].xcom_pull(task_ids="transform_weather", key="enriched_weather")
    run_id = context["run_id"]

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    _ensure_table(conn)
    _migrate_table(conn)

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
                 humidity_pct, wind_kph, condition, dag_run_id,
                 temp_category, wind_category, comfort_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                w["city"], w["time"], w["temperature_c"], w["feels_like_c"],
                w["humidity_pct"], w["wind_kph"], w["condition"], run_id,
                w["temp_category"], w["wind_category"], w["comfort_score"],
            ),
        )
        conn.commit()
        print(f"Inserted record for {w['city']} at {w['time']}.")

    total = conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
    conn.close()
    print(f"Total rows in DB: {total}")


def report_weather(**context):
    """Print weather summary + last 5 readings including derived fields."""
    w = context["ti"].xcom_pull(task_ids="transform_weather", key="enriched_weather")

    print(
        f"\n{'=' * 46}\n"
        f"  Weather Report — {w['city']}\n"
        f"  Time         : {w['time']}\n"
        f"  Condition    : {w['condition']}\n"
        f"  Temperature  : {w['temperature_c']}°C  →  {w['temp_category']}\n"
        f"  Feels like   : {w['feels_like_c']}°C\n"
        f"  Humidity     : {w['humidity_pct']}%\n"
        f"  Wind speed   : {w['wind_kph']} km/h  →  {w['wind_category']}\n"
        f"  Comfort score: {w['comfort_score']} / 100\n"
        f"{'=' * 46}"
    )

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT recorded_at, temperature_c, temp_category, comfort_score
        FROM weather
        ORDER BY id DESC
        LIMIT 5
        """
    ).fetchall()
    conn.close()

    print("\n  Last 5 stored readings:")
    print(f"  {'Time':<22} {'Temp':>6}  {'Category':<10}  Comfort")
    print(f"  {'-'*22} {'-'*6}  {'-'*10}  {'-'*7}")
    for recorded_at, temp, category, comfort in rows:
        print(f"  {recorded_at:<22} {temp:>5}°C  {str(category):<10}  {comfort}")


def compute_stats():
    """Aggregate stats across all stored readings."""
    conn = sqlite3.connect(DB_PATH)
    stats = conn.execute(
        """
        SELECT
            COUNT(*)                        AS total_readings,
            ROUND(MIN(temperature_c), 1)    AS min_temp,
            ROUND(MAX(temperature_c), 1)    AS max_temp,
            ROUND(AVG(temperature_c), 1)    AS avg_temp,
            ROUND(AVG(humidity_pct), 1)     AS avg_humidity,
            ROUND(AVG(wind_kph), 1)         AS avg_wind,
            ROUND(AVG(comfort_score), 1)    AS avg_comfort
        FROM weather
        WHERE city = ?
        """,
        (CITY,),
    ).fetchone()
    conn.close()

    total, min_t, max_t, avg_t, avg_h, avg_w, avg_c = stats
    print(
        f"\n{'=' * 46}\n"
        f"  All-time stats — {CITY} ({total} readings)\n"
        f"  Temperature  : min {min_t}°C  /  max {max_t}°C  /  avg {avg_t}°C\n"
        f"  Humidity     : avg {avg_h}%\n"
        f"  Wind speed   : avg {avg_w} km/h\n"
        f"  Comfort score: avg {avg_c} / 100\n"
        f"{'=' * 46}"
    )


with DAG(
    dag_id="weather_fetch",
    default_args=default_args,
    description="Day 5 — BranchPythonOperator skips pipeline if today's data exists",
    schedule="@daily",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=["learning", "day-5", "weather", "branching"],
) as dag:

    check = BranchPythonOperator(
        task_id="check_if_fetched",
        python_callable=check_if_fetched,
    )

    skip = PythonOperator(
        task_id="already_fetched",
        python_callable=already_fetched,
    )

    fetch = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    validate = PythonOperator(
        task_id="validate_response",
        python_callable=validate_response,
    )

    parse = PythonOperator(
        task_id="parse_weather",
        python_callable=parse_weather,
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
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

    check >> [skip, fetch]
    fetch >> validate >> parse >> transform >> store >> report >> stats
