# Makefile: every command in this project runs through here.
#
# Run `make help` for the list. Everything is a thin wrapper around
# `docker compose` so that a reviewer can read what a target does without
# reading the compose files first.
#
# All targets run in WSL. Do not run them from Windows: the repo is on a WSL
# filesystem, and the containers need WSL's Docker socket.

# Run from inside infra/. COMPOSE_FILE inside infra/.env lists every compose
# file by a path relative to that directory, and Compose resolves those paths
# against the working directory. Naming a file with -f instead would override
# the list and hide every profiled service.
SHELL := /bin/bash
COMPOSE := cd infra && docker compose

.DEFAULT_GOAL := help

## ------------------------------------------------------------------ help ---

.PHONY: help
help: ## Show this help
	@echo "EDAI coursework platform"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Start here: make up-core && make bootstrap && make verify"

## ---------------------------------------------------------------- images ---

.PHONY: images
images: ## Build every image this project builds itself
	$(COMPOSE) --profile compute --profile init --profile tools --profile governance build

.PHONY: pull
pull: ## Pull the base images (takes a while on a cold machine)
	bash misc/tools/pull_images.sh all

## ------------------------------------------------------------------ core ---

.PHONY: up-core
up-core: ## Start the floor: postgres, seaweedfs, hive-metastore, trino
	$(COMPOSE) up -d postgres seaweedfs hive-metastore trino

.PHONY: bootstrap
bootstrap: ## Create the S3 buckets and Postgres databases. Idempotent
	$(COMPOSE) --profile init run --rm bootstrap

.PHONY: verify
verify: ## Run the platform smoke tests. Exit 1 if anything fails
	$(COMPOSE) --profile tools run --rm tools

.PHONY: ps
ps: ## Show what is running
	$(COMPOSE) ps

.PHONY: list-ports
list-ports: ## Print every published port and flag any collision
	bash misc/tools/list_ports.sh

.PHONY: stop
stop: ## Stop everything, keep the data
	$(COMPOSE) stop

.PHONY: down
down: ## Stop everything and delete the containers, keep the volumes
	$(COMPOSE) down --remove-orphans

.PHONY: nuke
nuke: ## Stop everything and delete the containers AND the data volumes
	$(COMPOSE) down --remove-orphans --volumes

## --------------------------------------------------------------- compute ---

.PHONY: up-compute
up-compute: ## Start the Spark cluster
	$(COMPOSE) --profile compute up -d spark-master spark-worker

.PHONY: up-dbt
up-dbt: ## Start HiveServer2, which dbt-spark connects through
	$(COMPOSE) --profile compute --profile dbt up -d spark-master spark-worker spark-thrift

.PHONY: spark-wiring
spark-wiring: ## Prove Spark writes to the lakehouse and Trino reads it back
	-docker stop edai-spark-thrift >/dev/null 2>&1 || true
	$(COMPOSE) --profile compute run --rm spark-jobs /opt/pipelines/_wiring_check.py

.PHONY: spark-conf
spark-conf: ## Print the Spark configuration a job actually resolves
	$(COMPOSE) --profile compute run --rm spark-jobs /opt/pipelines/_conf_dump.py

.PHONY: spark-state
spark-state: ## Show the Spark master's view of workers and applications
	bash misc/tools/spark_master_state.sh

## -------------------------------------------------------------- pipeline ---

.PHONY: generate
generate: ## Run the generator into misc/sample_run
	cd generator && ../.venv/bin/datagen --scale 0.02 --out ../misc/sample_run --clean

.PHONY: test
test: ## Run both test suites: the pipeline contracts and the generator
	.venv/bin/python -m pytest tests/ -q
	cd generator && ../.venv/bin/python -m pytest tests/ -q

.PHONY: test-generator
test-generator: ## Run only the generator's suite
	cd generator && ../.venv/bin/python -m pytest tests/ -q

.PHONY: check-docs
check-docs: ## Check the docs: broken links, stale phrases, claimed-but-missing paths
	bash misc/tools/check_links.sh
	bash misc/tools/check_layout.sh
	bash misc/tools/find_stale.sh

.PHONY: dp1
dp1: ## Run DP1: landing -> bronze -> silver. BATCH=Batch1
	@# A running HiveServer2 takes every core of the Spark worker and DP1 then
	@# waits forever with no error. Stop it first. See docs/IaC.md section 8.2.
	-docker stop edai-spark-thrift >/dev/null 2>&1 || true
	$(COMPOSE) --profile compute run --rm \
		spark-jobs /opt/pipelines/dp1/main.py --batch-id $(or $(BATCH),Batch1)

## -------------------------------------------------------------- one-shot ---

.PHONY: ls
ls: ## List the objects under a prefix: make ls PREFIX=lakehouse/bronze
	$(COMPOSE) --profile tools run --rm tools python -m tools.ls lakehouse $(PREFIX)

.PHONY: purge
purge: ## Delete objects under a prefix: make purge PREFIX=lakehouse/bronze
	$(COMPOSE) --profile tools run --rm tools python -m tools.purge lakehouse/$(PREFIX)

.PHONY: sql
sql: ## Run a SQL file through Trino: make sql FILE=misc/q/trino_reads_spark.sql
	$(COMPOSE) --profile tools run --rm tools python -m tools.sql_file /opt/$(FILE)

## ------------------------------------------------------------------ logs ---

.PHONY: logs
logs: ## Follow the logs of one service: make logs SERVICE=trino
	$(COMPOSE) logs -f --tail 100 $(SERVICE)

.PHONY: state
state: ## Show restart counts and exit reasons for every container
	bash misc/tools/container_state.sh