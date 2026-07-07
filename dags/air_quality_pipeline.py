import os
import sqlite3
from datetime import datetime, timedelta

import requests

from airflow.decorators import dag, task

CITY = "Paris"
LATITUDE = 48.8566
LONGITUDE = 2.3522

AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
DB_PATH = "/opt/airflow/data/weather.db"

AQ_PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "current": [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone",
        "european_aqi",
        "us_aqi",
    ],
    "timezone": "auto",
}

REQUIRED_AQ_FIELDS = ["pm10", "pm2_5", "ozone", "european_aqi", "us_aqi", "time"]

# European AQI scale (EAQI)
EU_AQI_CATEGORIES = [
    (20,  "Good"),
    (40,  "Fair"),
    (60,  "Moderate"),
    (80,  "Poor"),
    (100, "Very Poor"),
    (float("inf"), "Extremely Poor"),
]

# US AQI scale
US_AQI_CATEGORIES = [
    (50,  "Good"),
    (100, "Moderate"),
    (150, "Unhealthy for Sensitive Groups"),
    (200, "Unhealthy"),
    (300, "Very Unhealthy"),
    (float("inf"), "Hazardous"),
]


def _categorize(value, scale):
    for threshold, label in scale:
        if value <= threshold:
            return label
    return "Unknown"


def _ensure_aq_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS air_quality (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            city                TEXT    NOT NULL,
            recorded_at         TEXT    NOT NULL,
            pm10                REAL,
            pm2_5               REAL,
            carbon_monoxide     REAL,
            nitrogen_dioxide    REAL,
            ozone               REAL,
            european_aqi        INTEGER,
            us_aqi              INTEGER,
            eu_category         TEXT,
            us_category         TEXT,
            dag_run_id          TEXT
        )
    """)
    conn.commit()


@dag(
    dag_id="air_quality_pipeline",
    description="Day 10 — second data source: air quality from Open-Meteo AQ API",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args={
        "owner": "weatherflow",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["learning", "day-10", "air-quality", "taskflow"],
)
def air_quality_pipeline():

    @task
    def fetch_air_quality() -> dict:
        """Fetch current air quality data from Open-Meteo AQ API."""
        response = requests.get(AQ_URL, params=AQ_PARAMS, timeout=10)
        response.raise_for_status()
        raw = response.json()["current"]
        print(f"API status : {response.status_code}")
        print(f"Raw AQ data: {raw}")
        return raw

    @task
    def validate_air_quality(raw: dict) -> dict:
        """Raise ValueError if any required field is missing."""
        missing = [f for f in REQUIRED_AQ_FIELDS if f not in raw]
        if missing:
            raise ValueError(f"AQ response missing required fields: {missing}")
        print(f"Validation passed — {len(REQUIRED_AQ_FIELDS)} required fields present.")
        return raw

    @task
    def transform_air_quality(raw: dict) -> dict:
        """Enrich raw data with human-readable AQI categories."""
        eu_aqi = raw.get("european_aqi") or 0
        us_aqi = raw.get("us_aqi") or 0

        transformed = {
            "city": CITY,
            "time": raw["time"],
            "pm10": raw.get("pm10"),
            "pm2_5": raw.get("pm2_5"),
            "carbon_monoxide": raw.get("carbon_monoxide"),
            "nitrogen_dioxide": raw.get("nitrogen_dioxide"),
            "ozone": raw.get("ozone"),
            "european_aqi": eu_aqi,
            "us_aqi": us_aqi,
            "eu_category": _categorize(eu_aqi, EU_AQI_CATEGORIES),
            "us_category": _categorize(us_aqi, US_AQI_CATEGORIES),
        }
        print(f"EU AQI: {eu_aqi} → {transformed['eu_category']}")
        print(f"US AQI: {us_aqi} → {transformed['us_category']}")
        return transformed

    @task
    def store_air_quality(data: dict, **context) -> dict:
        """Persist reading to air_quality table — idempotent on (city, recorded_at)."""
        run_id = context["run_id"]

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        _ensure_aq_table(conn)

        exists = conn.execute(
            "SELECT 1 FROM air_quality WHERE city = ? AND recorded_at = ?",
            (data["city"], data["time"]),
        ).fetchone()

        if exists:
            print(f"AQ record for {data['city']} at {data['time']} already exists — skipping.")
        else:
            conn.execute(
                """
                INSERT INTO air_quality
                    (city, recorded_at, pm10, pm2_5, carbon_monoxide,
                     nitrogen_dioxide, ozone, european_aqi, us_aqi,
                     eu_category, us_category, dag_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data["city"], data["time"], data["pm10"], data["pm2_5"],
                    data["carbon_monoxide"], data["nitrogen_dioxide"], data["ozone"],
                    data["european_aqi"], data["us_aqi"],
                    data["eu_category"], data["us_category"], run_id,
                ),
            )
            conn.commit()
            print(f"Inserted AQ record for {data['city']} at {data['time']}.")

        total = conn.execute("SELECT COUNT(*) FROM air_quality").fetchone()[0]
        conn.close()
        print(f"Total AQ rows in DB: {total}")
        return data  # pass through so report_air_quality chains on this task

    @task
    def report_air_quality(data: dict) -> None:
        """Print a formatted air quality report and historical summary."""
        print(
            f"\n{'=' * 46}\n"
            f"  Air Quality Report — {data['city']}\n"
            f"  Time             : {data['time']}\n"
            f"{'─' * 46}\n"
            f"  European AQI : {data['european_aqi']:>4}  →  {data['eu_category']}\n"
            f"  US AQI       : {data['us_aqi']:>4}  →  {data['us_category']}\n"
            f"{'─' * 46}\n"
            f"  PM10           : {data['pm10']} µg/m³\n"
            f"  PM2.5          : {data['pm2_5']} µg/m³\n"
            f"  Ozone          : {data['ozone']} µg/m³\n"
            f"  NO₂            : {data['nitrogen_dioxide']} µg/m³\n"
            f"  CO             : {data['carbon_monoxide']} µg/m³\n"
            f"{'=' * 46}"
        )

        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            """
            SELECT recorded_at, european_aqi, eu_category, us_aqi
            FROM   air_quality
            WHERE  city = ?
            ORDER  BY id DESC
            LIMIT  5
            """,
            (CITY,),
        ).fetchall()
        conn.close()

        print("\n  Last 5 AQ readings:")
        print(f"  {'Time':<22} {'EU AQI':>6}  {'Category':<25} {'US AQI':>6}")
        print(f"  {'-'*22} {'-'*6}  {'-'*25} {'-'*6}")
        for recorded_at, eu, category, us in rows:
            print(f"  {recorded_at:<22} {eu:>6}  {str(category):<25} {us:>6}")

    # ── Pipeline ──────────────────────────────────────────────────────────────
    raw = fetch_air_quality()
    validated = validate_air_quality(raw)
    transformed = transform_air_quality(validated)
    stored = store_air_quality(transformed)
    report_air_quality(stored)


dag_instance = air_quality_pipeline()
