import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

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

# Physically plausible bounds for each numeric field (world-record extremes)
FIELD_RANGES = {
    "temperature_2m":       (-90.0, 60.0),
    "apparent_temperature": (-90.0, 70.0),
    "relative_humidity_2m": (0.0,  100.0),
    "wind_speed_10m":       (0.0,  500.0),
}


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
        return "ingestion.check_api_available"

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
    return "ingestion.check_api_available"


def already_fetched():
    print("Pipeline skipped: today's weather data already stored.")


# ---------------------------------------------------------------------------
# Data quality checks (Day 12)
# ---------------------------------------------------------------------------

def check_nulls(**context):
    """Fail if any required field in the raw API payload is None."""
    raw = context["ti"].xcom_pull(task_ids="ingestion.fetch_weather", key="raw_weather")
    if not raw:
        raise ValueError("No raw payload in XCom — fetch_weather may have failed silently.")
    nulls = [f for f in REQUIRED_FIELDS if raw.get(f) is None]
    if nulls:
        raise ValueError(f"Quality check FAILED — null values in required fields: {nulls}")
    print(f"Null check passed — all {len(REQUIRED_FIELDS)} required fields are non-null.")


def check_ranges(**context):
    """Fail if any numeric field falls outside its physically plausible range."""
    raw = context["ti"].xcom_pull(task_ids="ingestion.fetch_weather", key="raw_weather")
    violations = []
    for field, (lo, hi) in FIELD_RANGES.items():
        value = raw.get(field)
        if value is not None and not (lo <= value <= hi):
            violations.append(f"{field}={value!r}  (expected {lo}–{hi})")
    if violations:
        raise ValueError(f"Quality check FAILED — out-of-range values:\n  " + "\n  ".join(violations))
    print(f"Range check passed — all {len(FIELD_RANGES)} numeric fields within expected bounds.")


def check_row_count(**context):
    """Assert at least one row exists for today after the storage step."""
    today = context["ds"]
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"DB not found at {DB_PATH} after store_weather ran.")
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute(
        "SELECT COUNT(*) FROM weather WHERE city = ?", (CITY,)
    ).fetchone()[0]
    today_count = conn.execute(
        "SELECT COUNT(*) FROM weather WHERE city = ? AND recorded_at LIKE ?",
        (CITY, f"{today}%"),
    ).fetchone()[0]
    conn.close()
    print(f"Row count — total for {CITY}: {total}, today ({today}): {today_count}")
    if today_count == 0:
        raise AssertionError(
            f"Quality check FAILED — 0 rows for {CITY} on {today} after storage."
        )
    print("Row count check passed.")


def fetch_weather(**context):
    response = requests.get(BASE_URL, params=PARAMS, timeout=10)
    response.raise_for_status()
    data = response.json()
    print(f"API status  : {response.status_code}")
    print(f"Raw current : {data['current']}")
    context["ti"].xcom_push(key="raw_weather", value=data["current"])


def validate_response(**context):
    """Raise ValueError if the API response is missing any required field."""
    raw = context["ti"].xcom_pull(task_ids="ingestion.fetch_weather", key="raw_weather")

    if not raw:
        raise ValueError("XCom payload from ingestion.fetch_weather is empty.")

    missing = [f for f in REQUIRED_FIELDS if f not in raw]
    if missing:
        raise ValueError(f"API response missing required fields: {missing}")

    print(f"Validation passed — all {len(REQUIRED_FIELDS)} required fields present.")


def parse_weather(**context):
    raw = context["ti"].xcom_pull(task_ids="ingestion.fetch_weather", key="raw_weather")
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
    parsed = context["ti"].xcom_pull(task_ids="processing.parse_weather", key="parsed_weather")

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
    w = context["ti"].xcom_pull(task_ids="processing.transform_weather", key="enriched_weather")
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
    w = context["ti"].xcom_pull(task_ids="processing.transform_weather", key="enriched_weather")

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


# ---------------------------------------------------------------------------
# HTML report generator (Day 13)
# ---------------------------------------------------------------------------

# Maps temp_category labels to hex colours used in the report
_CAT_COLOR = {
    "Freezing": "#74b9ff",
    "Cold":     "#0984e3",
    "Mild":     "#00b894",
    "Warm":     "#e17055",
    "Hot":      "#d63031",
}


def generate_html_report(**context):
    """Write a self-contained HTML report to /opt/airflow/reports/weather_YYYY-MM-DD.html.

    Reads today's current reading directly from SQLite so it runs on BOTH branches:
    - normal run  → data was just fetched and stored this execution
    - skip branch → data was already in the DB from an earlier run today
    """
    today = context["ds"]

    conn = sqlite3.connect(DB_PATH)
    # Fetch today's most recent reading for the "current conditions" section
    row = conn.execute(
        """
        SELECT temperature_c, feels_like_c, humidity_pct, wind_kph,
               condition, temp_category, wind_category, comfort_score, recorded_at
        FROM   weather
        WHERE  city = ? AND recorded_at LIKE ?
        ORDER  BY id DESC LIMIT 1
        """,
        (CITY, f"{today}%"),
    ).fetchone()

    if not row:
        print(f"No data found for {CITY} on {today} — skipping HTML report generation.")
        return

    temp_c, feels_c, hum, wind, condition, temp_cat, wind_cat, comfort, _ = row
    w = {
        "temperature_c": temp_c,
        "feels_like_c":  feels_c,
        "humidity_pct":  hum,
        "wind_kph":      wind,
        "condition":     condition,
        "temp_category": temp_cat,
        "wind_category": wind_cat,
        "comfort_score": comfort,
    }

    history = conn.execute(
        """
        SELECT recorded_at, temperature_c, feels_like_c, humidity_pct,
               wind_kph, condition, temp_category, comfort_score
        FROM   weather
        WHERE  city = ?
        ORDER  BY id DESC
        LIMIT  10
        """,
        (CITY,),
    ).fetchall()
    agg = conn.execute(
        """
        SELECT COUNT(*),
               ROUND(MIN(temperature_c), 1), ROUND(MAX(temperature_c), 1),
               ROUND(AVG(temperature_c), 1), ROUND(AVG(humidity_pct), 1),
               ROUND(AVG(wind_kph), 1),      ROUND(AVG(comfort_score), 1)
        FROM   weather WHERE city = ?
        """,
        (CITY,),
    ).fetchone()
    conn.close()

    total, min_t, max_t, avg_t, avg_h, avg_w, avg_c = agg

    comfort_color = (
        "#00b894" if w["comfort_score"] >= 70
        else "#fdcb6e" if w["comfort_score"] >= 40
        else "#d63031"
    )
    cat_color = _CAT_COLOR.get(str(w["temp_category"]), "#636e72")

    def _badge(cat):
        color = _CAT_COLOR.get(str(cat), "#636e72")
        return (
            f'<span style="background:{color};color:#fff;'
            f'padding:2px 8px;border-radius:12px;font-size:.8em">{cat}</span>'
        )

    rows_html = "".join(
        f"<tr>"
        f"<td>{r[0]}</td>"
        f"<td style='text-align:right'>{r[1]}°C</td>"
        f"<td style='text-align:right'>{r[2]}°C</td>"
        f"<td style='text-align:right'>{r[3]}%</td>"
        f"<td style='text-align:right'>{r[4]}&nbsp;km/h</td>"
        f"<td>{r[5]}</td>"
        f"<td>{_badge(r[6])}</td>"
        f"<td style='text-align:right'>{r[7]}</td>"
        f"</tr>"
        for r in history
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WeatherFlow · {CITY} · {today}</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#f4f6f9;color:#2d3436}}
  header{{background:linear-gradient(135deg,#0984e3,#6c5ce7);color:#fff;padding:2rem;text-align:center}}
  header h1{{font-size:1.8rem;font-weight:700;letter-spacing:1px}}
  header p{{opacity:.85;margin-top:.4rem;font-size:.9rem}}
  main{{max-width:920px;margin:2rem auto;padding:0 1rem}}
  h2{{color:#0984e3;font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:1rem}}
  .section{{background:#fff;border-radius:12px;padding:1.5rem;box-shadow:0 2px 8px rgba(0,0,0,.07);margin-bottom:1.5rem}}
  .card-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:1rem}}
  .card{{background:#f8f9fa;border-radius:10px;padding:1rem;text-align:center}}
  .card .label{{font-size:.7rem;color:#636e72;text-transform:uppercase;letter-spacing:.5px}}
  .card .value{{font-size:1.6rem;font-weight:700;margin-top:.3rem;line-height:1}}
  .card .sub{{font-size:.75rem;color:#636e72;margin-top:.3rem}}
  .overflow{{overflow-x:auto}}
  table{{width:100%;border-collapse:collapse;font-size:.85rem;white-space:nowrap}}
  th{{background:#f4f6f9;padding:.55rem .75rem;text-align:left;font-size:.72rem;font-weight:700;
      color:#636e72;text-transform:uppercase;letter-spacing:.5px}}
  td{{padding:.55rem .75rem;border-bottom:1px solid #f4f6f9}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#f8f9fa}}
  .stat-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:1rem}}
  .stat{{text-align:center;padding:.5rem}}
  .stat .sval{{font-size:1.4rem;font-weight:700;color:#0984e3}}
  .stat .slabel{{font-size:.72rem;color:#636e72;margin-top:.25rem;text-transform:uppercase;letter-spacing:.5px}}
  footer{{text-align:center;color:#b2bec3;font-size:.72rem;margin:1rem 0 2rem}}
</style>
</head>
<body>
<header>
  <h1>WeatherFlow</h1>
  <p>{CITY} &nbsp;·&nbsp; {today} &nbsp;·&nbsp; Generated by Apache Airflow</p>
</header>
<main>

  <section class="section">
    <h2>Today’s conditions</h2>
    <div class="card-grid">
      <div class="card">
        <div class="label">Condition</div>
        <div class="value" style="font-size:1rem;margin-top:.5rem">{w['condition']}</div>
      </div>
      <div class="card">
        <div class="label">Temperature</div>
        <div class="value">{w['temperature_c']}&deg;C</div>
        <div class="sub">feels like {w['feels_like_c']}&deg;C</div>
      </div>
      <div class="card">
        <div class="label">Humidity</div>
        <div class="value">{w['humidity_pct']}%</div>
      </div>
      <div class="card">
        <div class="label">Wind</div>
        <div class="value" style="font-size:1.3rem">{w['wind_kph']}</div>
        <div class="sub">km/h &middot; {w['wind_category']}</div>
      </div>
      <div class="card">
        <div class="label">Category</div>
        <div class="value" style="font-size:1rem;color:{cat_color}">{w['temp_category']}</div>
      </div>
      <div class="card">
        <div class="label">Comfort score</div>
        <div class="value" style="color:{comfort_color}">{w['comfort_score']}</div>
        <div class="sub">out of 100</div>
      </div>
    </div>
  </section>

  <section class="section">
    <h2>Recent readings (last 10)</h2>
    <div class="overflow">
    <table>
      <thead><tr>
        <th>Time</th><th>Temp</th><th>Feels like</th>
        <th>Humidity</th><th>Wind</th><th>Condition</th>
        <th>Category</th><th>Comfort</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    </div>
  </section>

  <section class="section">
    <h2>All-time statistics &mdash; {CITY} &nbsp;({total} readings)</h2>
    <div class="stat-grid">
      <div class="stat"><div class="sval">{min_t}&deg;C</div><div class="slabel">Min temp</div></div>
      <div class="stat"><div class="sval">{max_t}&deg;C</div><div class="slabel">Max temp</div></div>
      <div class="stat"><div class="sval">{avg_t}&deg;C</div><div class="slabel">Avg temp</div></div>
      <div class="stat"><div class="sval">{avg_h}%</div><div class="slabel">Avg humidity</div></div>
      <div class="stat"><div class="sval">{avg_w}</div><div class="slabel">Avg wind km/h</div></div>
      <div class="stat"><div class="sval">{avg_c}</div><div class="slabel">Avg comfort</div></div>
    </div>
  </section>

</main>
<footer>WeatherFlow &nbsp;&middot;&nbsp; Apache Airflow learning project &nbsp;&middot;&nbsp; data from Open-Meteo (open-meteo.com)</footer>
</body>
</html>"""

    out_path = f"/opt/airflow/reports/weather_{today}.html"
    os.makedirs("/opt/airflow/reports", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"HTML report written → {out_path}  ({len(html):,} bytes)")


with DAG(
    dag_id="weather_fetch",
    default_args=default_args,
    description="Day 13 — HTML report generator: daily artifact saved to reports/",
    schedule="@daily",
    start_date=datetime(2026, 6, 25),
    catchup=False,
    tags=["learning", "day-13", "weather", "html-report"],
) as dag:

    check = BranchPythonOperator(
        task_id="check_if_fetched",
        python_callable=check_if_fetched,
    )

    skip = PythonOperator(
        task_id="already_fetched",
        python_callable=already_fetched,
    )

    # ── Group 1: check API is up, fetch raw data, validate schema ────────────
    with TaskGroup("ingestion", tooltip="Check API availability, fetch, validate") as ingestion:
        sense = HttpSensor(
            task_id="check_api_available",
            http_conn_id="open_meteo_api",
            endpoint="v1/forecast",
            request_params={
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "current": "temperature_2m",
            },
            response_check=lambda response: response.status_code == 200,
            poke_interval=30,
            timeout=300,
            mode="reschedule",
        )

        fetch = PythonOperator(
            task_id="fetch_weather",
            python_callable=fetch_weather,
        )

        validate = PythonOperator(
            task_id="validate_response",
            python_callable=validate_response,
        )

        sense >> fetch >> validate

    # ── Group 2: data quality gate ────────────────────────────────────────────
    with TaskGroup("quality", tooltip="Null checks, range validation, row count assertion") as quality:
        null_check = PythonOperator(
            task_id="check_nulls",
            python_callable=check_nulls,
        )

        range_check = PythonOperator(
            task_id="check_ranges",
            python_callable=check_ranges,
        )

        null_check >> range_check

    # ── Group 3: parse fields, enrich with pandas ─────────────────────────────
    with TaskGroup("processing", tooltip="Parse fields and derive categories + comfort score") as processing:
        parse = PythonOperator(
            task_id="parse_weather",
            python_callable=parse_weather,
        )

        transform = PythonOperator(
            task_id="transform_weather",
            python_callable=transform_weather,
        )

        parse >> transform

    # ── Group 4: persist to SQLite, assert row count ──────────────────────────
    with TaskGroup("storage", tooltip="Idempotent insert into SQLite + row count assertion") as storage:
        store = PythonOperator(
            task_id="store_weather",
            python_callable=store_weather,
        )

        row_count = PythonOperator(
            task_id="check_row_count",
            python_callable=check_row_count,
        )

        store >> row_count

    # ── Group 5: console report, stats, trigger weekly ───────────────────────
    with TaskGroup("reporting", tooltip="Console report, all-time stats, weekly trigger") as reporting:
        report = PythonOperator(
            task_id="report_weather",
            python_callable=report_weather,
        )

        stats = PythonOperator(
            task_id="compute_stats",
            python_callable=compute_stats,
        )

        trigger_weekly = TriggerDagRunOperator(
            task_id="trigger_weekly_summary",
            trigger_dag_id="weekly_summary",
            trigger_run_id="weekly_{{ dag_run.logical_date.strftime('%Y-W%W') }}",
            reset_dag_run=True,
            wait_for_completion=False,
        )

        report >> stats >> trigger_weekly

    # ── HTML report — top-level, runs on BOTH branches ────────────────────────
    # trigger_rule="none_failed_min_one_success":
    #   • full pipeline path  → storage group succeeds  → html_report runs
    #   • already-fetched path → already_fetched succeeds → html_report runs
    #     (reads today's row from DB that was stored in a prior run)
    html_report = PythonOperator(
        task_id="generate_html_report",
        python_callable=generate_html_report,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Top-level wiring ──────────────────────────────────────────────────────
    check >> [skip, ingestion]
    ingestion >> quality >> processing >> storage >> reporting
    [skip, storage] >> html_report
