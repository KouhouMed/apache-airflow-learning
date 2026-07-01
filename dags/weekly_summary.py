import sqlite3
from collections import Counter
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

CITY = "Paris"
DB_PATH = "/opt/airflow/data/weather.db"

default_args = {
    "owner": "weatherflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_weekly_data(**context):
    """Query the last 7 days of weather readings from SQLite."""
    end_date = context["ds"]                                          # e.g. 2026-07-07
    start_date = (
        datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=7)
    ).strftime("%Y-%m-%d")                                            # e.g. 2026-06-30

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """
        SELECT recorded_at, temperature_c, feels_like_c, humidity_pct,
               wind_kph, condition, comfort_score, temp_category
        FROM   weather
        WHERE  city = ?
          AND  recorded_at >= ?
          AND  recorded_at <  ?
        ORDER  BY recorded_at
        """,
        (CITY, start_date, end_date),
    ).fetchall()
    conn.close()

    columns = [
        "recorded_at", "temperature_c", "feels_like_c", "humidity_pct",
        "wind_kph", "condition", "comfort_score", "temp_category",
    ]
    records = [dict(zip(columns, row)) for row in rows]

    print(f"Loaded {len(records)} records for {CITY} ({start_date} → {end_date})")

    if not records:
        raise ValueError(
            f"No data found for {CITY} between {start_date} and {end_date}. "
            "Run weather_fetch at least once before triggering weekly_summary."
        )

    context["ti"].xcom_push(key="weekly_records", value=records)
    context["ti"].xcom_push(key="date_range", value={"start": start_date, "end": end_date})


def compute_weekly_stats(**context):
    """Use pandas to aggregate the week's readings."""
    records = context["ti"].xcom_pull(task_ids="load_weekly_data", key="weekly_records")
    dates = context["ti"].xcom_pull(task_ids="load_weekly_data", key="date_range")

    df = pd.DataFrame(records)
    df["temperature_c"] = pd.to_numeric(df["temperature_c"])
    df["comfort_score"] = pd.to_numeric(df["comfort_score"])
    df["humidity_pct"] = pd.to_numeric(df["humidity_pct"])
    df["wind_kph"] = pd.to_numeric(df["wind_kph"])

    hottest = df.loc[df["temperature_c"].idxmax()]
    coldest = df.loc[df["temperature_c"].idxmin()]
    most_comfortable = df.loc[df["comfort_score"].idxmax()]
    most_common_condition = Counter(df["condition"]).most_common(1)[0][0]

    stats = {
        "start": dates["start"],
        "end": dates["end"],
        "total_readings": len(df),
        "avg_temp": round(df["temperature_c"].mean(), 1),
        "min_temp": round(df["temperature_c"].min(), 1),
        "max_temp": round(df["temperature_c"].max(), 1),
        "avg_humidity": round(df["humidity_pct"].mean(), 1),
        "avg_wind": round(df["wind_kph"].mean(), 1),
        "avg_comfort": round(df["comfort_score"].mean(), 1),
        "hottest_day": hottest["recorded_at"],
        "hottest_temp": hottest["temperature_c"],
        "coldest_day": coldest["recorded_at"],
        "coldest_temp": coldest["temperature_c"],
        "best_comfort_day": most_comfortable["recorded_at"],
        "best_comfort_score": most_comfortable["comfort_score"],
        "most_common_condition": most_common_condition,
        "temp_categories": df["temp_category"].value_counts().to_dict(),
    }

    print(f"Weekly stats computed: {stats}")
    context["ti"].xcom_push(key="weekly_stats", value=stats)


def generate_weekly_report(**context):
    """Print a formatted weekly weather report."""
    s = context["ti"].xcom_pull(task_ids="compute_weekly_stats", key="weekly_stats")

    categories = "  ".join(
        f"{cat}: {count}x" for cat, count in s["temp_categories"].items()
    )

    print(
        f"\n{'=' * 50}\n"
        f"  WEEKLY WEATHER REPORT — {CITY}\n"
        f"  Period  : {s['start']}  →  {s['end']}\n"
        f"  Readings: {s['total_readings']}\n"
        f"{'─' * 50}\n"
        f"  Temperature\n"
        f"    avg {s['avg_temp']}°C  |  min {s['min_temp']}°C  |  max {s['max_temp']}°C\n"
        f"  Humidity    : avg {s['avg_humidity']}%\n"
        f"  Wind speed  : avg {s['avg_wind']} km/h\n"
        f"  Comfort     : avg {s['avg_comfort']} / 100\n"
        f"{'─' * 50}\n"
        f"  Highlights\n"
        f"    Hottest   : {s['hottest_temp']}°C  on {s['hottest_day']}\n"
        f"    Coldest   : {s['coldest_temp']}°C  on {s['coldest_day']}\n"
        f"    Best day  : comfort {s['best_comfort_score']} on {s['best_comfort_day']}\n"
        f"    Dominant condition : {s['most_common_condition']}\n"
        f"  Categories  : {categories}\n"
        f"{'=' * 50}"
    )


with DAG(
    dag_id="weekly_summary",
    default_args=default_args,
    description="Day 7 — weekly aggregation DAG triggered by weather_fetch",
    schedule="@weekly",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=["learning", "day-7", "weekly", "aggregation"],
) as dag:

    load = PythonOperator(
        task_id="load_weekly_data",
        python_callable=load_weekly_data,
    )

    compute = PythonOperator(
        task_id="compute_weekly_stats",
        python_callable=compute_weekly_stats,
    )

    report = PythonOperator(
        task_id="generate_weekly_report",
        python_callable=generate_weekly_report,
    )

    load >> compute >> report
