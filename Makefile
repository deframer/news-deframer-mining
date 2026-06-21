.PHONY: all build clean test help lint format type-check fix start stop down logs zap run sync duckdb-ui FORCE sentiment-update analyze-text stem-stopwords-json download-models-test

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

sentiment-update:
	uv run python -m news_deframer.cli.sentiment_update

# Example: echo "This is a very happy and joyful day!" | make analyze-text | sed '1d' | jq -c
# Example German: echo "Das ist ein sehr schöner Tag!" | TEXT_LANGUAGE=de make analyze-text | sed '1d' | jq -c
analyze-text:
	VERBOSE=1 uv run python -m news_deframer.cli.analyze_text -l "$(TEXT_LANGUAGE)"

# Example: make stem-stopwords-json STOPWORDS_JSON=../news-deframer/dummy-feeds.json
stem-stopwords-json:
	@test -n "$(STOPWORDS_JSON)" || (printf 'Set STOPWORDS_JSON=/path/to/file.json\n' >&2; exit 1)
	uv run stem-stopwords-json --input "$(STOPWORDS_JSON)"

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
