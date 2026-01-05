PYTHON ?= python
PIP ?= pip

.PHONY: setup run-loader run-app bootstrap test docker-build docker-up docker-down test-supabase help

# --- Instalação e Configuração ---
setup:
	$(PIP) install -r requirements.txt

bootstrap:
	$(PYTHON) scripts/bootstrap_duckdb.py

# --- Execução ---
run-app:
	$(PYTHON) -m streamlit run app/duckdb_app.py

run-loader:
	$(PYTHON) scripts/incremental_loader.py

# --- Testes e Verificação ---
test:
	pytest -q

test-connection:
	$(PYTHON) scripts/test_supabase_connection.py

# --- Docker ---
docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

# --- Ajuda ---
help:
	@echo "Comandos Disponíveis:"
	@echo "  make setup           - Instalar dependências"
	@echo "  make bootstrap       - Criar banco de dados local (DuckDB)"
	@echo "  make run-app         - Rodar a aplicação Streamlit"
	@echo "  make run-loader      - Rodar sincronização (Supabase -> DuckDB)"
	@echo "  make test            - Rodar testes unitários"
	@echo "  make test-connection - Testar conexão com Supabase"
	@echo "  make docker-up       - Subir ambiente Docker" 