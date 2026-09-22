.PHONY: \
	up down build logs ps reset \
	flink-build flink-up flink-down flink-logs flink-submit\
	backfill test

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

flink-build:
	docker compose build jobmanager taskmanager

flink-up:
	docker compose up -d jobmanager taskmanager

flink-down:
	docker compose stop jobmanager taskmanager

flink-logs:
	docker compose logs -f jobmanager taskmanager

flink-submit:
	docker compose exec jobmanager \
		flink run --python /opt/flink/jobs/candles.py

backfill:
	@test -n "$(SYMBOLS)" || (echo "SYMBOLS is required"; exit 1)
	@test -n "$(START)" || (echo "START is required"; exit 1)
	@test -n "$(END)" || (echo "END is required"; exit 1)
	docker compose run --rm producer \
		python src/historical.py \
		--symbols $(SYMBOLS) \
		--start "$(START)" \
		--end "$(END)"

test:
	python -m unittest discover -s tests -v
