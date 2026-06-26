# WeatherFlow — Automated Weather Analytics Pipeline

An Apache Airflow project built incrementally over 14 days as part of a structured learning plan. The pipeline fetches weather and air quality data from [Open-Meteo](https://open-meteo.com/) (free, no API key required), transforms it, validates it, and generates daily reports.

## Project goals

- Learn Apache Airflow 2.x from first principles
- Cover: DAGs, operators, sensors, XComs, hooks, custom operators, branching, task groups
- Build a real, runnable data pipeline — not just toy examples

## Quick start

```bash
# 1. Clone the repo
git clone https://github.com/KouhouMed/apache-airflow-learning.git
cd apache-airflow-learning

# 2. Start Airflow
docker compose up airflow-init   # run once to initialise the DB
docker compose up -d             # start webserver + scheduler

# 3. Open the UI
# http://localhost:8080  (admin / admin)
```

## What's built so far

| Day | DAG / Feature | Concept covered |
|-----|--------------|-----------------|
| 1   | `hello_world` — project skeleton + first DAG | DAG structure, PythonOperator, BashOperator |
| 2   | `weather_fetch` — live weather from Open-Meteo API | HTTP requests, XCom push/pull, WMO weather codes |
| 3   | `weather_fetch` + SQLite storage layer | `sqlite3`, `CREATE TABLE IF NOT EXISTS`, INSERT, SELECT |

## Folder structure

```
dags/          — all DAG definitions
plugins/       — custom operators and hooks
data/          — SQLite database (gitignored)
reports/       — generated HTML reports (gitignored)
tests/         — unit tests
```

## Tech stack

- Apache Airflow 2.9.1
- Docker Compose (LocalExecutor + PostgreSQL)
- Python 3.11
- Open-Meteo API (no API key needed)
- SQLite / Pandas
