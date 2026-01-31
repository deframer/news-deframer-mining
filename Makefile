.PHONY: all build clean test help lint format type-check fix start stop down logs zap run sync duckdb-ui FORCE

APP_NAME := miner
DOCKER_REPO := deframer
DOCKER_COMPOSE_FILE ?= docker-compose.yml
COMPOSE_ENV_FILE ?= .env-compose
DOCKER_ENV_FLAG := $(if $(wildcard $(COMPOSE_ENV_FILE)),--env-file $(COMPOSE_ENV_FILE),--env-file /dev/null)
DUCKDB_IMAGE ?= duckdb/duckdb:latest
DUCKDB_UI_PORT ?= 4213
DEFAULT_DUCKDB_FILE := ./trend_docs.duckdb
DUCKDB_DB_FILE ?= $(if $(strip $(DUCK_DB_FILE)),$(strip $(DUCK_DB_FILE)),$(DEFAULT_DUCKDB_FILE))
DUCKDB_UI_DB := $(DUCKDB_DB_FILE)
ifeq ($(strip $(DUCKDB_UI_DB)),:memory)
  DUCKDB_UI_DB := $(DEFAULT_DUCKDB_FILE)
endif
ifeq ($(strip $(DUCKDB_UI_DB)),:memory:)
  DUCKDB_UI_DB := $(DEFAULT_DUCKDB_FILE)
endif

ifneq ("$(wildcard .env)","")
  #$(info using .env file)
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

duckdb-ui:
	@mkdir -p $(dir $(DUCKDB_UI_DB))
	@echo "Launching DuckDB UI at http://localhost:$(DUCKDB_UI_PORT) using $(DUCKDB_UI_DB)"
	@echo "Press Ctrl+C to stop the UI session"
	docker run --rm -it --net host \
		-v "$(PWD):/workspace" \
		-w /workspace \
		$(DUCKDB_IMAGE) \
		duckdb "$(DUCKDB_UI_DB)" \
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
SQL_DB_FILE := $(DUCKDB_DB_FILE)

$(SQL_DIR)/%.sql: FORCE
	@docker run --rm -it \
		-v "$(PWD):/workspace" \
		-w /workspace \
		$(DUCKDB_IMAGE) \
		duckdb "$(SQL_DB_FILE)" -c ".read $@"

FORCE:
