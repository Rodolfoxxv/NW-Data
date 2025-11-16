PYTHON ?= python
PIP ?= pip

.PHONY: setup run-loader run-app bootstrap test docker-build docker-up docker-down

setup:
	$(PIP) install -r requirements.txt

run-loader:
	$(PYTHON) scripts/incremental_loader.py

run-app:
	streamlit run app/duckdb_app.py

bootstrap:
	$(PYTHON) scripts/bootstrap_duckdb.py

test:
	pytest -q

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down
