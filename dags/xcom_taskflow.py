"""
Day 9 — TaskFlow API and implicit XComs
========================================
Classic pattern (Days 2-8):
    context["ti"].xcom_push(key="raw", value=data)
    raw = context["ti"].xcom_pull(task_ids="fetch", key="raw")

TaskFlow pattern (Day 9):
    @task
    def fetch() -> dict:
        return data           # auto-pushed to XCom

    @task
    def process(raw: dict):   # auto-pulled from XCom
        ...

    raw = fetch()
    process(raw)              # Airflow wires the dependency + data transfer
"""

from datetime import datetime, timedelta

import requests

from airflow.decorators import dag, task

CITY = "Paris"
LATITUDE = 48.8566
LONGITUDE = 2.3522

BASE_URL = "https://api.open-meteo.com/v1/forecast"

PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "current": [
        "temperature_2m",
        "apparent_temperature",
        "relative_humidity_2m",
        "wind_speed_10m",
        "weather_code",
    ],
    "timezone": "auto",
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


@dag(
    dag_id="xcom_taskflow",
    description="Day 9 — @task decorator: implicit XComs and TaskFlow API",
    schedule=None,  # manual trigger only — run it whenever to compare with weather_fetch
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={
        "owner": "weatherflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["learning", "day-9", "taskflow", "xcom"],
)
def weather_taskflow():

    @task
    def fetch_current() -> dict:
        """Return value is automatically pushed to XCom — no xcom_push needed."""
        response = requests.get(BASE_URL, params=PARAMS, timeout=10)
        response.raise_for_status()
        raw = response.json()["current"]
        print(f"Raw API payload: {raw}")
        return raw

    @task
    def clean(raw: dict) -> dict:
        """
        Receives the return value of fetch_current as an argument automatically.
        Airflow serialises it through XCom — no xcom_pull needed.
        """
        return {
            "city": CITY,
            "time": raw["time"],
            "temperature": raw["temperature_2m"],
            "feels_like": raw["apparent_temperature"],
            "humidity": raw["relative_humidity_2m"],
            "wind": raw["wind_speed_10m"],
            "condition": WMO_CODES.get(raw["weather_code"], f"Code {raw['weather_code']}"),
        }

    @task
    def comfort_score(cleaned: dict) -> float:
        """Compute and return a single float — also stored in XCom."""
        temp_penalty = abs(cleaned["temperature"] - 21) * 3
        humidity_penalty = max(0, cleaned["humidity"] - 60) * 0.4
        score = round(max(0.0, 100 - temp_penalty - humidity_penalty), 1)
        print(f"Comfort score: {score} / 100")
        return score

    @task
    def summarize(cleaned: dict, score: float) -> None:
        """
        Receives outputs from TWO upstream tasks as named arguments.
        This is the key TaskFlow advantage: multiple XComs, zero boilerplate.
        """
        print(
            f"\n{'=' * 44}\n"
            f"  TaskFlow Weather Summary — {cleaned['city']}\n"
            f"  Time      : {cleaned['time']}\n"
            f"  Condition : {cleaned['condition']}\n"
            f"  Temp      : {cleaned['temperature']}°C  "
            f"(feels like {cleaned['feels_like']}°C)\n"
            f"  Humidity  : {cleaned['humidity']}%\n"
            f"  Wind      : {cleaned['wind']} km/h\n"
            f"  Comfort   : {score} / 100\n"
            f"{'=' * 44}\n"
            f"\n  XCom values written this run:\n"
            f"    fetch_current  → raw dict (all API fields)\n"
            f"    clean          → cleaned dict (7 fields)\n"
            f"    comfort_score  → float ({score})\n"
            f"  All visible in UI: DAG run → task → XCom tab"
        )

    # -------------------------------------------------------------------------
    # Wiring — looks like plain Python function calls.
    # Airflow sees the task objects and builds the dependency graph + XCom links.
    # -------------------------------------------------------------------------
    raw = fetch_current()
    cleaned = clean(raw)
    score = comfort_score(cleaned)
    summarize(cleaned=cleaned, score=score)


# Assign to a module-level variable so Airflow's DagBag can discover it
dag_instance = weather_taskflow()
