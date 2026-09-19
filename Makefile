.PHONY: up down build logs reset ps

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
