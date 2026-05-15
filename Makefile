.PHONY: all build clean test help lint format type-check fix start stop down logs zap run sync duckdb-ui FORCE download-models sentiment-update analyze-text

APP_NAME := miner
TEXT_LANGUAGE ?= en
DOCKER_REPO := ghcr.io/deframer/news-deframer-mining
DOCKER_COMPOSE_FILE ?= docker-compose.yml
COMPOSE_ENV_FILE ?= .env-compose
DOCKER_ENV_FLAG := $(if $(wildcard $(COMPOSE_ENV_FILE)),--env-file $(COMPOSE_ENV_FILE),--env-file /dev/null)

DB_IMAGE := pgduckdb/pgduckdb:18-main

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

# additional languages of the spaCy models - https://github.com/explosion/spacy-models
# export SPACY_MODELS="sl uk es"
# additional languages for the Memolon models - https://github.com/deframer/memolon-parquet
# export MEMOLON_MODELS="en de"
download-models:
	uv run python -m news_deframer.cli.download_models

sentiment-update:
	uv run python -m news_deframer.cli.sentiment_update

# Example: echo "This is a very happy and joyful day!" | make analyze-text | sed '1d' | jq -c
# Example German: echo "Das ist ein sehr schöner Tag!" | TEXT_LANGUAGE=de make analyze-text | sed '1d' | jq -c
analyze-text:
	uv run python -m news_deframer.cli.analyze_text -l "$(TEXT_LANGUAGE)"

docker-build:
	docker build -t $(DOCKER_REPO)/$(APP_NAME):latest -f build/package/mining/Dockerfile .
	docker images $(DOCKER_REPO)/$(APP_NAME):latest  --format "{{.Size}}"

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
