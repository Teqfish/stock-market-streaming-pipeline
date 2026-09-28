.PHONY: \
	up down build logs ps reset \
	flink-build flink-up flink-down flink-logs flink-submit \
	airflow-up airflow-down airflow-logs \
	backfill test

# ---------------------------------------------------------------------------
# Full stack
# ---------------------------------------------------------------------------

up:
	docker compose up -d

down:
	docker compose down

build:
	docker compose build

logs:
	docker compose logs -f

ps:
	docker compose ps

reset:
	docker compose down -v


# ---------------------------------------------------------------------------
# Flink
# ---------------------------------------------------------------------------

flink-build:
	docker compose build jobmanager taskmanager flink-submitter

flink-up:
	docker compose up -d jobmanager taskmanager flink-submitter

flink-down:
	docker compose stop jobmanager taskmanager flink-submitter

flink-logs:
	docker compose logs -f jobmanager taskmanager flink-submitter

flink-submit:
	docker compose exec jobmanager \
		flink run \
		--python /opt/flink/jobs/candles.py \
		--pyFiles /opt/flink/jobs/processing.py


# ---------------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------------

airflow-up:
	docker compose up -d \
		airflow-postgres \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor

airflow-down:
	docker compose stop \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor \
		airflow-postgres

airflow-logs:
	docker compose logs -f \
		airflow-api-server \
		airflow-scheduler \
		airflow-dag-processor


# ---------------------------------------------------------------------------
# Historical data
# ---------------------------------------------------------------------------

backfill:
	@test -n "$(SYMBOLS)" || (echo "SYMBOLS is required"; exit 1)
	@test -n "$(START)" || (echo "START is required"; exit 1)
	@test -n "$(END)" || (echo "END is required"; exit 1)
	docker compose run --rm producer \
		python src/historical.py \
		--symbols $(SYMBOLS) \
		--start "$(START)" \
		--end "$(END)"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

test:
	docker compose run --rm \
		-v "$(CURDIR)/tests:/app/tests:ro" \
		-v "$(CURDIR)/airflow/dags:/app/airflow/dags:ro" \
		-e PYTHONPATH=/app/src \
		producer \
		python -m unittest discover -s /app/tests -v
