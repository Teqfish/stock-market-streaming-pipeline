.PHONY: \
	start stop restart open \
	up down build rebuild logs ps reset \
	flink-build flink-up flink-down flink-restart flink-logs flink-submit flink-jobs \
	airflow-up airflow-down airflow-restart airflow-logs \
	test

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FLINK_URL := http://localhost:8081
REDPANDA_URL := http://localhost:8080
AIRFLOW_URL := http://localhost:8082
DASHBOARD_URL := http://localhost:8501


# ---------------------------------------------------------------------------
# User-facing pipeline commands
# ---------------------------------------------------------------------------

# Build the complete pipeline, start all services, submit the continuous
# Flink streaming job, and open the web interfaces.
start: build up flink-submit open
	@echo ""
	@echo "Streams of GAMMAN is running."
	@echo ""
	@echo "Dashboard:        $(DASHBOARD_URL)"
	@echo "Airflow:          $(AIRFLOW_URL)"
	@echo "Flink:            $(FLINK_URL)"
	@echo "Redpanda Console: $(REDPANDA_URL)"

# Stop the complete pipeline while preserving persistent volumes.
stop: down

# Restart without rebuilding images or deleting volumes.
restart: down up flink-submit open


# ---------------------------------------------------------------------------
# Full stack
# ---------------------------------------------------------------------------

build:
	docker compose build

rebuild:
	docker compose build --no-cache

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

# Destroy all runtime state, including persistent Docker volumes.
reset:
	docker compose down -v --remove-orphans


# ---------------------------------------------------------------------------
# Browser
# ---------------------------------------------------------------------------

open:
	@echo "Opening Streams of GAMMAN interfaces..."
	@OS="$$(uname -s)"; \
	if [ "$$OS" = "Darwin" ]; then \
		open \
			"$(DASHBOARD_URL)" \
			"$(AIRFLOW_URL)" \
			"$(FLINK_URL)" \
			"$(REDPANDA_URL)"; \
	elif [ "$$OS" = "Linux" ]; then \
		if command -v xdg-open >/dev/null 2>&1; then \
			xdg-open "$(DASHBOARD_URL)" >/dev/null 2>&1 & \
			xdg-open "$(AIRFLOW_URL)" >/dev/null 2>&1 & \
			xdg-open "$(FLINK_URL)" >/dev/null 2>&1 & \
			xdg-open "$(REDPANDA_URL)" >/dev/null 2>&1 & \
		else \
			echo "xdg-open not found; open these URLs manually:"; \
			echo "$(DASHBOARD_URL)"; \
			echo "$(AIRFLOW_URL)"; \
			echo "$(FLINK_URL)"; \
			echo "$(REDPANDA_URL)"; \
		fi; \
	else \
		echo "Automatic browser opening is not supported on $$OS."; \
		echo "Open these URLs manually:"; \
		echo "$(DASHBOARD_URL)"; \
		echo "$(AIRFLOW_URL)"; \
		echo "$(FLINK_URL)"; \
		echo "$(REDPANDA_URL)"; \
	fi


# ---------------------------------------------------------------------------
# Flink
# ---------------------------------------------------------------------------

flink-build:
	docker compose build \
		jobmanager \
		taskmanager \
		flink-submitter

flink-up:
	docker compose up -d \
		flink-checkpoint-init \
		jobmanager \
		taskmanager \
		flink-submitter

flink-down:
	docker compose stop \
		jobmanager \
		taskmanager \
		flink-submitter

flink-restart: flink-down flink-up

flink-logs:
	docker compose logs -f \
		jobmanager \
		taskmanager \
		flink-submitter

flink-submit:
	@echo "Waiting for Flink JobManager..."
	@until curl --silent --fail \
		$(FLINK_URL)/overview > /dev/null; do \
		sleep 2; \
	done
	@echo "Flink JobManager is ready."
	@echo "Submitting continuous streaming job..."
	docker compose exec jobmanager \
		flink run -d \
		--python /opt/flink/jobs/candles.py \
		--pyFiles /opt/flink/jobs/processing.py

flink-jobs:
	@curl --silent $(FLINK_URL)/jobs/overview | \
		jq -r '.jobs[] | \
		"Job: \(.name)\nState: \(.state)\nTasks: \(.tasks.running)/\(.tasks.total) running\nID: \(.jid)\n"'


# ---------------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------------

airflow-up:
	docker compose up -d \
		airflow-postgres \
		airflow-init \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor

airflow-down:
	docker compose stop \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor \
		airflow-postgres

airflow-restart: airflow-down airflow-up

airflow-logs:
	docker compose logs -f \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test:
	docker compose run --rm --no-deps \
		-v "$(CURDIR)/tests:/app/tests:ro" \
		-v "$(CURDIR)/airflow/dags:/app/airflow/dags:ro" \
		-e PYTHONPATH=/app/src \
		producer \
		python -m unittest discover -s /app/tests -v
