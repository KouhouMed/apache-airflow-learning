# WeatherFlow — Automated Weather Analytics Pipeline

An Apache Airflow project built incrementally over 14 days as part of a structured learning plan. The pipeline fetches weather data from [Open-Meteo](https://open-meteo.com/) (free, no API key required), transforms it, validates it, and generates daily reports.

## Project goals

- Learn Apache Airflow 3.x from first principles
- Cover: DAGs, operators, sensors, XComs, hooks, custom operators, branching, task groups
- Build a real, runnable data pipeline — not just toy examples

## Quick start

```bash
# 1. Clone the repo
git clone https://github.com/KouhouMed/apache-airflow-learning.git
cd apache-airflow-learning

# 2. Copy env file
cp .env.example .env

# 3. Build the custom image and initialise the DB (run once)
docker compose build
docker compose up airflow-init

# 4. Start all services
docker compose up -d

# 5. Open the UI
# http://localhost:8080  (admin / admin)
```

## Services

Airflow 3 splits the old single webserver into dedicated processes:

| Container | Role |
|-----------|------|
| `airflow-api-server` | REST API + UI (replaces webserver) |
| `airflow-scheduler` | Schedules and triggers DAG runs |
| `airflow-dag-processor` | Parses DAG files (separated from scheduler in v3) |
| `airflow-triggerer` | Handles deferrable operators |
| `postgres` | Airflow metadata database |

## What's built so far

| Day | DAG / Feature | Concept covered |
|-----|--------------|-----------------|
| 1   | `hello_world` — project skeleton + first DAG | DAG structure, PythonOperator, BashOperator |
| 2   | `weather_fetch` — live weather from Open-Meteo API | HTTP requests, XCom push/pull, WMO weather codes |
| 3   | `weather_fetch` + SQLite storage layer | `sqlite3`, `CREATE TABLE IF NOT EXISTS`, INSERT, SELECT |
| 4   | `transform_weather` task — pandas enrichment | `pd.cut`, derived columns, DB migration, custom Dockerfile |
| 5   | `BranchPythonOperator` — skip pipeline if today's data exists | branching, `context["ds"]`, task skipping |
| 6   | Retry logic, exponential backoff, failure/retry callbacks, response validation | `on_failure_callback`, `on_retry_callback`, `retry_exponential_backoff` |
| 7   | `weekly_summary` DAG — weekly aggregation triggered by `weather_fetch` | `TriggerDagRunOperator`, cross-DAG data sharing, `@weekly` schedule |

## Folder structure

```
dags/          — all DAG definitions
plugins/       — custom operators and hooks
data/          — SQLite database (gitignored)
reports/       — generated HTML reports (gitignored)
tests/         — unit tests
```

## Tech stack

- Apache Airflow 3.0.2
- Docker Compose (LocalExecutor + PostgreSQL 15)
- Python 3.12
- Open-Meteo API (no API key needed)
- SQLite / Pandas
