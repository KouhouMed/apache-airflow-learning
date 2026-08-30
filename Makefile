.PHONY: build init up down logs ps shell test clean

# ── Docker Compose helpers ─────────────────────────────────────────────────────

build:          ## Build the custom Airflow image
	docker compose build

init: build     ## First-time setup: build image, run DB migrations, create admin user
	docker compose up airflow-init

up:             ## Start all services in the background
	docker compose up -d

down:           ## Stop all services (volumes preserved)
	docker compose down

logs:           ## Tail logs from all containers
	docker compose logs -f --tail=100

ps:             ## Show running containers and their health status
	docker compose ps

shell:          ## Open a bash shell inside the scheduler container
	docker compose exec airflow-scheduler bash

# ── Local dev ──────────────────────────────────────────────────────────────────

test:           ## Run unit tests with pytest (uses local venv, not Docker)
	pytest tests/ -v

lint:           ## Lint DAG files with ruff
	ruff check dags/

# ── Cleanup ────────────────────────────────────────────────────────────────────

clean:          ## Stop containers, remove all volumes, delete generated files
	docker compose down -v --remove-orphans
	rm -f data/*.db reports/*.html

help:           ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
