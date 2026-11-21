# =====================================================================
# Airflow Medallion - automatización
#   Primera vez:  make all     (construye, inicializa, levanta y corre el DAG)
#   Día a día:    make up  /  make run  /  make down  /  make status
# =====================================================================

SHELL   := /bin/bash
DAG_ID  := medallon_etl_drybean
PORT    := 8081
URL     := http://localhost:$(PORT)
AIRFLOW := docker compose exec -T airflow-scheduler airflow

# Carpeta del proyecto ETL que orquesta Airflow (se monta en solo lectura)
ETL_PROJECT_DIR ?= ../laboratorio1/laboratorio_drybean_ml

.DEFAULT_GOAL := help
.PHONY: help all setup build up down restart status run runs errors test logs open shell clean

help: ## Muestra esta ayuda
	@echo "Uso: make <comando>"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

all: up run open ## Primera vez: construye, levanta Airflow, corre el DAG y abre el navegador

setup: ## Crea .env y carpetas; verifica Docker y el proyecto ETL
	@docker info >/dev/null 2>&1 || { echo "  ✗ Docker no está corriendo. Abre Docker Desktop."; exit 1; }
	@test -f "$(ETL_PROJECT_DIR)/config/config.yaml" || { \
		echo "  ✗ No encuentro el proyecto ETL en $(ETL_PROJECT_DIR)"; \
		echo "    Clónalo:  git clone https://github.com/2203juan/dry-bean-classifier.git ../laboratorio1/laboratorio_drybean_ml"; \
		echo "    o indica otra ruta:  make up ETL_PROJECT_DIR=/ruta/al/proyecto"; exit 1; }
	@mkdir -p dags logs plugins config data
	@if [ ! -f .env ]; then \
		printf "AIRFLOW_UID=%s\nAIRFLOW_PORT=$(PORT)\nETL_PROJECT_DIR=$(ETL_PROJECT_DIR)\nFERNET_KEY=\n" "$$(id -u)" > .env; \
		echo "  ✓ .env creado"; \
	fi

build: setup ## Construye la imagen de Airflow con las dependencias del pipeline
	@echo "→ Construyendo imagen (la primera vez tarda unos minutos)..."
	@docker compose build -q && echo "  ✓ Imagen airflow-medallon:3.3.2 lista"

up: build ## Inicializa (si hace falta) y levanta todos los servicios de Airflow
	@echo "→ Levantando Airflow..."
	@docker compose up -d --quiet-pull >/dev/null 2>&1 || { docker compose up -d; exit 1; }
	@for i in $$(seq 1 120); do \
		curl -sf -o /dev/null $(URL)/api/v2/monitor/health && break; \
		[ $$i -eq 120 ] && { echo "  ✗ Airflow no respondió. Revisa: make logs"; exit 1; }; \
		sleep 2; \
	done
	@echo "  ✓ Airflow listo en $(URL)  (usuario airflow / contraseña airflow)"
	@echo "→ Esperando a que se procese el DAG..."
	@for i in $$(seq 1 60); do \
		$(AIRFLOW) dags list 2>/dev/null | grep -q $(DAG_ID) && { echo "  ✓ DAG $(DAG_ID) cargado"; exit 0; }; \
		sleep 3; \
	done; echo "  ✗ El DAG no aparece. Revisa: make errors"; exit 1

down: ## Apaga Airflow (conserva la base de datos y el historial)
	@docker compose down >/dev/null 2>&1; echo "✓ Airflow apagado"

restart: down up ## Reinicia Airflow

status: ## Estado de los contenedores y de la API
	@docker compose ps --format 'table {{.Service}}\t{{.Status}}'
	@echo
	@curl -sf $(URL)/api/v2/monitor/health >/dev/null \
		&& echo -e "  \033[32m● Airflow arriba\033[0m  $(URL)" \
		|| echo -e "  \033[31m○ Airflow abajo\033[0m"

run: ## Dispara el DAG y espera a que termine mostrando el estado de cada tarea
	@$(AIRFLOW) dags unpause $(DAG_ID) >/dev/null 2>&1 || true
	@RUN_ID=$$($(AIRFLOW) dags trigger $(DAG_ID) -o json 2>/dev/null | grep '^\[' | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["dag_run_id"])') \
		&& echo "→ Ejecutando $(DAG_ID)  (run: $$RUN_ID)" \
		&& for i in $$(seq 1 120); do \
			STATE=$$($(AIRFLOW) dags list-runs $(DAG_ID) -o json 2>/dev/null | grep '^\[' | python3 -c "import sys,json; print(next(r['state'] for r in json.load(sys.stdin) if r['run_id']=='$$RUN_ID'))"); \
			case $$STATE in success|failed) break;; esac; \
			sleep 5; \
		done \
		&& echo "  Estado final: $$STATE" \
		&& $(AIRFLOW) tasks states-for-dag-run $(DAG_ID) "$$RUN_ID" -o table 2>/dev/null | grep -E "task_id|^ *-|$(DAG_ID)" | awk '{print "  " $$0}' \
		&& [ "$$STATE" = success ]

runs: ## Historial de ejecuciones del DAG
	@$(AIRFLOW) dags list-runs $(DAG_ID) 2>/dev/null

errors: ## Muestra errores de importación de DAGs
	@$(AIRFLOW) dags list-import-errors 2>/dev/null

test: ## Corre los tests del DAG dentro del contenedor
	@docker compose exec -T -w /opt/airflow airflow-scheduler python -m pytest -q tests

logs: ## Sigue los logs del scheduler
	@docker compose logs -f --tail 50 airflow-scheduler

open: ## Abre la interfaz web de Airflow
	@open $(URL)/dags/$(DAG_ID)

shell: ## Abre una terminal dentro del scheduler
	@docker compose exec airflow-scheduler bash

clean: ## Apaga todo, borra la base de datos de Airflow, logs y salidas del pipeline
	@docker compose down -v >/dev/null 2>&1
	@rm -rf ./logs ./data/bronze ./data/silver ./data/gold ./data/models ./data/reports ./data/warehouse
	@echo "✓ Limpio. Corre 'make up' para empezar de cero."
