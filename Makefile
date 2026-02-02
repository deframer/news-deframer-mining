.PHONY: all build clean test help lint format type-check fix start stop down logs zap run sync duckdb-ui FORCE

APP_NAME := miner
DOCKER_REPO := ghcr.io/deframer/news-deframer-mining
DOCKER_COMPOSE_FILE ?= docker-compose.yml
COMPOSE_ENV_FILE ?= .env-compose
DOCKER_ENV_FLAG := $(if $(wildcard $(COMPOSE_ENV_FILE)),--env-file $(COMPOSE_ENV_FILE),--env-file /dev/null)

ifneq ("$(wildcard .env)","")
  include .env
  export $(shell sed 's/=.*//' .env)
endif

all:
	@echo all

start:
	docker compose $(DOCKER_ENV_FLAG) -f $(DOCKER_COMPOSE_FILE) up -d --build --force-recreate

stop:
	docker compose $(DOCKER_ENV_FLAG) -f $(DOCKER_COMPOSE_FILE) stop

down:
	docker compose $(DOCKER_ENV_FLAG) -f $(DOCKER_COMPOSE_FILE) down --remove-orphans --volumes

logs:
	docker compose $(DOCKER_ENV_FLAG) -f $(DOCKER_COMPOSE_FILE) logs -f

zap: down start

miner:
	uv run python -m news_deframer.cli.miner

docker-build:
	docker build -t $(DOCKER_REPO)/$(APP_NAME):latest -f build/package/mining/Dockerfile .

clean:
	docker compose $(DOCKER_ENV_FLAG) -f $(DOCKER_COMPOSE_FILE) down --rmi local --volumes

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format --check .

type-check:
	uv run mypy .

check: lint format type-check test

fix:
	uv run ruff check --fix .
	uv run ruff format .

sync:
	uv sync

SQL_DIR := sql

$(SQL_DIR)/%.sql: FORCE
	docker run --rm -i --network host \
		-v "$(CURDIR):/workspace" \
		-w /workspace \
		$(DB_IMAGE) \
		psql "postgres://$${DB_USER}:$${DB_PASSWORD}@$${DB_HOST}:$${DB_PORT}/$${DB_NAME}" \
		-f $@

FORCE:
