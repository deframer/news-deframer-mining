.PHONY: all build clean test help lint format type-check fix start stop down logs zap run sync duckdb-ui FORCE

APP_NAME := miner
DOCKER_REPO := deframer
DOCKER_COMPOSE_FILE ?= docker-compose.yml
COMPOSE_ENV_FILE ?= .env-compose
DOCKER_ENV_FLAG := $(if $(wildcard $(COMPOSE_ENV_FILE)),--env-file $(COMPOSE_ENV_FILE),--env-file /dev/null)

ifneq ("$(wildcard .env)","")
  include .env
  export $(shell sed 's/=.*//' .env)
endif

DUCKDB_IMAGE ?= duckdb/duckdb:latest
DUCKDB_UI_PORT ?= 4213
DUCKDB_DB_FILE ?= ./trend_docs.duckdb

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

duckdb-ui:
	@set -euo pipefail; \
	DB_PATH="/workspace/$(DUCKDB_DB_FILE)"; \
	DB_ALIAS="$$(DB_FILE="$(DUCKDB_DB_FILE)" python -c 'import os,re; path=os.path.basename(os.environ.get("DB_FILE","")); base=os.path.splitext(path)[0] or "attached_db"; alias=re.sub(r"\\W+","_",base) or "attached_db"; alias="db_"+alias if alias[0].isdigit() else alias; print(alias)')"; \
	echo "Launching DuckDB UI at http://localhost:$(DUCKDB_UI_PORT) using read-only attachment $$DB_PATH (schema $$DB_ALIAS)"; \
	echo "Press Ctrl+C to stop the UI session"; \
	docker run --rm -it --net host \
		-v "$(PWD):/workspace:ro" \
		$(DUCKDB_IMAGE) \
		duckdb "/tmp/zzz_ui_scratch.duckdb" \
		-cmd "ATTACH DATABASE '$$DB_PATH' AS $$DB_ALIAS (READ_ONLY);" \
		-cmd "USE $$DB_ALIAS.main;" \
		-cmd "SET ui_local_port=$(DUCKDB_UI_PORT);" \
		-cmd "CALL start_ui_server();"

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
	@docker run --rm \
		-v "$(PWD):/workspace" \
		-w /workspace \
		$(DUCKDB_IMAGE) \
		duckdb --readonly "$(DUCKDB_DB_FILE)" -c ".read $@"

FORCE:
