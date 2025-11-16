# 🦆 NW-DATA – Mini Data Lake

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-1.1.x-fff000?style=for-the-badge&logo=duckdb&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL/Supabase-13+-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI/CD-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)

Pipeline completo que sincroniza tabelas do DuckDB com Postgres/Supabase e expõe um dashboard Streamlit para explorar o data lake. Ideal para prototipagem, demonstrações ou projetos pessoais em ambientes de ingestão controlada.

---

## 📋 Índice
- [Características](#-características)
- [Tecnologias](#-tecnologias)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
  - [Desenvolvimento Local](#desenvolvimento-local)
  - [Containers Docker](#containers-docker)
- [.env e Configuração](#-env-e-configuração)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Execução dos Serviços](#-execução-dos-serviços)
- [Fluxo com Docker](#-fluxo-com-docker)
- [CI/CD](#-cicd)
- [Dados de Exemplo](#-dados-de-exemplo)
- [Testes](#-testes)
- [Troubleshooting](#-troubleshooting)
- [Licença](#-licença)

---

## ✨ Características

### 🧱 Arquitetura modular
- DuckDB atua como zona de preparo, armazenando tabelas, metadados e relacionamentos.
- Loader incremental lê os schemas, detecta PK/FK e replica para Postgres/Supabase com controle em `controle_cargas`.
- Dashboard Streamlit consulta diretamente o DuckDB para inspeção rápida das tabelas e metadados.

### ⚡ Loader incremental resiliente
- Detecta colunas de timestamp automaticamente e realiza cargas incrementais.
- Ordena as tabelas via ordenação topológica para respeitar dependências.
- Sincroniza PK/FK no destino e suporta truncamento total opcional.
- Tratamento de lotes via `execute_values` e logging consolidado em `pipeline.log`.

### 🖥️ Dashboard de exploração
- Visualiza tabelas, schemas, relacionamentos e dados amostrais.
- Compartilha as mesmas variáveis de ambiente do loader.

### 🔁 Automação completa
- Makefile com comandos rápidos (`bootstrap`, `run-loader`, `run-app`, `docker-*`).
- Docker Compose com perfis para app e loader em contêineres idênticos ao local.
- GitHub Actions para CI (pytest) e pipeline que dispara o loader direto no Supabase.

### 🔒 Segurança e observabilidade
- Leitura de `.env` via `python-dotenv`.
- Suporte a SSL configurável no Postgres (`DB_SSLMODE`, `DB_SSLROOTCERT`).
- Logging estruturado e histórico de execuções em `controle_cargas`.

---

## 🛠️ Tecnologias

**Core**
- Python 3.10+
- DuckDB
- PostgreSQL / Supabase
- Streamlit + Plotly

**Bibliotecas chave**
- `psycopg2` para cargas em lote
- `python-dotenv` para configuração
- `pytest` para testes

**DevOps**
- Docker & Docker Compose
- Makefile
- GitHub Actions (CI + pipeline Supabase)

---

## 📦 Pré-requisitos

### Desenvolvimento Local
- Python 3.10 ou superior
- `pip` / `venv`
- DuckDB (instalado via `pip`)
- Banco Postgres acessível (local, Supabase, RDS ou Docker)

### Containers Docker
- Docker 20.10+
- Docker Compose 2+
- Acesso às variáveis de ambiente (arquivo `.env` ou secrets)

---

## 🚀 Instalação

### Desenvolvimento Local
1. **Clone e entre no projeto**
   ```bash
   git clone <seu-fork> data_lk && cd data_lk
   ```
2. **Crie e ative o ambiente virtual**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .\.venv\Scripts\activate   # Windows
   ```
3. **Instale as dependências**
   ```bash
   pip install -r requirements.txt
   ```
4. **Configure o `.env`**
   ```bash
   cp .env.example .env
   # edite credenciais e caminhos conforme seu ambiente
   ```
5. **Gere o DuckDB local**
   ```bash
   python scripts/bootstrap_duckdb.py
   # ou make bootstrap se você tiver make instalado
   ```

### Containers Docker
1. Ajuste as variáveis em `.env` (usado pelo compose).
2. Construa as imagens:
   ```bash
   docker compose build
   ```
3. Suba o app Streamlit e o Postgres de destino (se desejar) com:
   ```bash
   docker compose up -d
   ```
4. Execute o loader dentro do contêiner:
   ```bash
   docker compose run --rm --profile loader loader
   ```
5. Pare tudo quando terminar:
   ```bash
   docker compose down
   ```

---

## ⚙️ `.env` e Configuração

| Variável | Descrição |
| --- | --- |
| `DUCKDB_PATH` | Nome do arquivo DuckDB (relativo ao `DESTINATION_PATH`). |
| `DESTINATION_PATH` | Diretório onde o arquivo DuckDB é lido/grava (ex.: `./data`). |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | Credenciais do Postgres/Supabase. |
| `DB_SSLMODE` | (Opcional) `prefer`, `require`, `verify-full` etc. |
| `DB_SSLROOTCERT` | (Opcional) Caminho para o certificado CA quando usar `verify-full`. |
| `TRUNCATE_BEFORE_LOAD` | `true` para truncar a tabela destino antes da carga. |
| `BATCH_SIZE` | Tamanho do lote enviado ao Postgres (default 5000). |
| `INCREMENTAL_TIMESTAMP_COLUMN` | (Opcional) força a coluna de timestamp usada no incremental. |

> **Dica Windows:** se não quiser instalar `make`, execute os comandos equivalentes (`python scripts/...` ou `streamlit run ...`) diretamente no PowerShell.

---

## 📂 Estrutura do Projeto

```
data_lk/
├── app/
│   └── duckdb_app.py          # Dashboard Streamlit
├── scripts/
│   ├── bootstrap_duckdb.py    # Gera as tabelas teste
│   └── incremental_loader.py  # Loader incremental DuckDB x Postgres  
├── data/                      
├── tests/
│   └── test_incremental_loader.py
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── README.md
└── .env / .env.example
```

---

## ▶️ Execução dos Serviços

| Objetivo | Comando sugerido |
| --- | --- |
| Gerar dados de exemplo | `python scripts/bootstrap_duckdb.py` |
| Rodar loader incremental | `python scripts/incremental_loader.py` |
| Abrir dashboard Streamlit | `streamlit run app/duckdb_app.py` |
| Rodar com Make (Linux/Mac ou Windows com make) | `make bootstrap`, `make run-loader`, `make run-app` |

O loader registra cada execução na tabela `controle_cargas`, criando tabelas no Postgres caso ainda não existam, sincronizando PK/FK e aplicando incrementais guiados por timestamp.

---

## 🐳 Fluxo com Docker

1. `docker compose build` – imagens atualizadas (app + loader).
2. `docker compose up -d` – sobe app Streamlit (porta 8501) compartilhando `./data`.
3. `docker compose run --rm --profile loader loader` – executa o loader no mesmo ambiente.
4. `docker compose down` – encerra os serviços.

Os contêineres montam `./data`, garantindo que o arquivo DuckDB seja o mesmo do host.

---

## 🔄 CI/CD

- `.github/workflows/ci.yml`: instala dependências, roda `scripts/bootstrap_duckdb.py` e executa `pytest` a cada push/PR.
- `.github/workflows/pipeline.yml`: executa o loader contra o Postgres/Supabase real ao fazer push na `main`. Recomenda-se armazenar o DuckDB como secret base64 (`DUCKDB_BASE64`) e reconstruí-lo durante o job:
  ```bash
  echo "$DUCKDB_BASE64" | base64 -d > "$DESTINATION_PATH/$DUCKDB_PATH"
  ```
- Secrets adicionais como `DB_SSLMODE`/`DB_SSLROOTCERT` podem ser exportadas na etapa de “Set up environment variables”.

---

## 🗃️ Dados de Exemplo

`scripts/bootstrap_duckdb.py` cria e popula:
- `clientes`
- `pedidos`
- `itens_pedido`
- `pagamentos`
- `table_metadata` e `fk_metadata`

Executar o script é seguro mesmo repetidamente: ele limpa as tabelas, recria metadados e deixa o arquivo pronto para novas demos.

---

## 🧪 Testes

```bash
pytest -q
# ou
make test
```

Os testes cobrem funções críticas como mapeamento de tipos e ordenação topológica das tabelas para garantir que as dependências sejam respeitadas antes da carga.

---

## 🆘 Troubleshooting

- **`ValueError: Variáveis de ambiente ausentes`** – confira se `.env` existe e foi preenchido.
- **`FileNotFoundError: Arquivo DuckDB não encontrado`** – execute o bootstrap ou ajuste `DESTINATION_PATH`.
- **`psycopg2.OperationalError`** – valide credenciais/IP liberado no Postgres/Supabase e, se SSL estiver habilitado, defina `DB_SSLMODE`/`DB_SSLROOTCERT`.
- **`make` não encontrado no Windows** – instale `make` via Chocolatey ou execute os comandos Python/Streamlit diretamente.

---

## 📄 Licença

Este projeto é distribuído sob a licença incluída no repositório. Consulte `LICENSE` para mais detalhes.

---

## 🙌 Autor

**Rodolfo Fonseca**. Dúvidas ou colaborações? Abra uma issue ou entre em contato via [GitHub].

---

**⭐ Fim**
