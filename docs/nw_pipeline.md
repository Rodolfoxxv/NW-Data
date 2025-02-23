# Pipeline de Migração de Dados: DuckDB para Supabase

## Descrição Geral

Este pipeline automatizado tem como objetivo migrar dados de um banco de dados DuckDB para um banco de dados PostgreSQL hospedado no Supabase. O código realiza as seguintes tarefas:

- Carrega as variáveis de ambiente necessárias.
- Estabelece conexões com o DuckDB e o Supabase.
- Converte os tipos de dados do DuckDB para os equivalentes no PostgreSQL.
- Cria as tabelas correspondentes no Supabase (incluindo uma tabela de controle para monitorar a migração).
- Insere os dados em batch no Supabase, respeitando constraints (como chaves primárias).
- Organiza a ordem de migração das tabelas com base em dependências definidas nos metadados.

## Dependências

- **duckdb**: Conexão e manipulação do banco de dados DuckDB.
- **psycopg2**: Conexão e manipulação do banco de dados PostgreSQL (Supabase).
- **logging**: Registro de logs e mensagens de erro.
- **json**: Manipulação de dados no formato JSON.
- **datetime**: Manipulação e formatação de datas.
- **dotenv**: Carregamento de variáveis de ambiente a partir do arquivo `.env`.
- **os**: Acesso a variáveis de ambiente e manipulação do sistema operacional.
- **pathlib**: Manipulação de caminhos de arquivos.
- **collections (defaultdict, deque)**: Estruturas de dados auxiliares para ordenação topológica das tabelas.

## Configuração do Ambiente e Logging

O pipeline carrega variáveis de ambiente essenciais, como URLs, credenciais e caminhos de arquivos. Se alguma variável necessária não estiver definida ou o arquivo DuckDB não for encontrado, o programa interrompe a execução.

```python
from dotenv import load_dotenv
import os
from pathlib import Path
import logging


load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DESTINATION_PATH = os.getenv("DESTINATION_PATH")

if not all([SUPABASE_URL, SUPABASE_KEY, DUCKDB_PATH, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DESTINATION_PATH]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

destination_path_obj = Path(DESTINATION_PATH)
full_duckdb_path = destination_path_obj / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()

if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

logger.info(f"DUCKDB_PATH: {full_duckdb_path}")
```

## Conexão com o DuckDB

```python
import duckdb
conn_duckdb = duckdb.connect(str(full_duckdb_path))
```

## Mapeamento de Tipos de Dados Duckdb para o PostgreSQL

```python
def mapear_tipo_duckdb_para_postgres_type(tipo_duckdb):
    tipo_duckdb = tipo_duckdb.upper()
    if 'TINYINT' in tipo_duckdb:
        return 'SMALLINT'
    elif 'BLOB' in tipo_duckdb:
        return 'BYTEA'
    elif 'DOUBLE' in tipo_duckdb:
        return 'DOUBLE PRECISION'
    elif 'DECIMAL' in tipo_duckdb:
        return 'NUMERIC' + tipo_duckdb.split('DECIMAL')[1]
    else:
        return tipo_duckdb
```

## Conexão com o Supabase

```python
import psycopg2
conn_supabase = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT
```

## Criação de Tabela de Controle

Cria ou verifica a existência da tabela de controle chamada controle_cargas no Supabase.
Esta tabela é usada para monitorar quais tabelas já foram migradas, a data da última carga e o número de linhas inseridas.

```python
def criar_tabela_controle(cursor_supabase):
    tabela_controle = "controle_cargas"
    query_criar_tabela = f"""
    CREATE TABLE IF NOT EXISTS {tabela_controle} (
        tabela_nome TEXT PRIMARY KEY,
        ultima_carga TIMESTAMP,
        linhas_carregadas INT
    );
    """
    try:
        cursor_supabase.execute(query_criar_tabela)
        logger.info(f"Tabela de controle '{tabela_controle}' criada ou verificada.")
    except psycopg2.Error as e:
        logger.error(f"Erro ao criar tabela de controle: {e}")
        raise
```

## Migração de Dados

### Ordenação Topológica das Tabelas

A função `obter_dependencias_tabelas` é responsável por determinar as dependências entre as tabelas. Ela utiliza a estrutura de dados `defaultdict` para armazenar as dependências de cada tabela.

```python
def obter_dependencias_tabelas(cursor_duckdb):
    dependencias = defaultdict(list)
    cursor_duckdb.execute("SELECT * FROM metadata")
    for row in cursor_duckdb.fetchall():
        tabela_nome = row[0]
        dependencias[tabela_nome] = [dep for dep in row[1:] if dep]
        rn dependencias
        return dependencias
```

### função processar_tabela

- Verificação na Tabela de Controle:
- Verifica se a tabela já foi migrada anteriormente.
- Obtenção do Schema do DuckDB
- Cria ou Atualização da Tabela no Supabase
- Inserção dos Dados na Tabela

```python
def processar_tabela(nome_tabela):
    try:
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                # Verifica se a tabela já foi migrada (pela presença na tabela de controle)
                cursor_supabase.execute(
                    "SELECT EXISTS(SELECT 1 FROM controle_cargas WHERE tabela_nome = %s)",
                    (nome_tabela,),
                )
                existe = cursor_supabase.fetchone()[0]

                # Obtém o schema do DuckDB via DESCRIBE
                duck_schema = conn_duckdb.execute(f"DESCRIBE {nome_tabela}").fetchall()
                duck_columns = {col[0]: col[1] for col in duck_schema}

                if not existe:
                    logger.info(f"Criando tabela {nome_tabela} no Supabase.")
                    colunas_supabase = []
                    for col in duck_schema:
                        nome_coluna = col[0]
                        tipo_duckdb = col[1]
                        tipo_postgres = mapear_tipo_duckdb_para_postgres_type(tipo_duckdb)
                        colunas_supabase.append(f'"{nome_coluna}" {tipo_postgres}')

                    # Obter as colunas da chave primária usando o PRAGMA do DuckDB
                    query_pk = f"SELECT name FROM pragma_table_info('{nome_tabela}') WHERE pk > 0 ORDER BY pk;"
                    pk_columns = conn_duckdb.execute(query_pk).fetchall()
                    pk_columns = [col[0] for col in pk_columns]
                    if pk_columns:
                        pk_columns_escaped = [f'"{col}"' for col in pk_columns]
                        pk_columns_str = ", ".join(pk_columns_escaped)
                        colunas_supabase.append(f"PRIMARY KEY ({pk_columns_str})")

                    query_criar_tabela_supabase = f"CREATE TABLE {nome_tabela} ({', '.join(colunas_supabase)})"
                    cursor_supabase.execute(query_criar_tabela_supabase)
                    conn_supabase.commit()
                    logger.info(f"Tabela {nome_tabela} criada no Supabase.")

                    # Registrar a criação da tabela na tabela de controle
                    cursor_supabase.execute(
                        "INSERT INTO controle_cargas (tabela_nome, ultima_carga, linhas_carregadas) VALUES (%s, %s, %s)",
                        (nome_tabela, datetime.now(), 0),
                    )
                    conn_supabase.commit()

                else:
                    logger.info(f"Tabela {nome_tabela} já existe no Supabase. Verificando atualizações (constraints).")
                    # Se a tabela já existe, verifica se possui PRIMARY KEY
                    query_pk_exists = """
                        SELECT COUNT(*) FROM information_schema.table_constraints 
                        WHERE table_name = %s AND constraint_type = 'PRIMARY KEY';
                    """
                    cursor_supabase.execute(query_pk_exists, (nome_tabela,))
                    pk_count = cursor_supabase.fetchone()[0]
                    if pk_count == 0:
                        # PK a partir do DuckDB
                        query_pk_duck = f"SELECT name FROM pragma_table_info('{nome_tabela}') WHERE pk > 0 ORDER BY pk;"
                        pk_columns = conn_duckdb.execute(query_pk_duck).fetchall()
                        pk_columns = [col[0] for col in pk_columns]
                        if pk_columns:
                            pk_columns_escaped = [f'"{col}"' for col in pk_columns]
                            pk_columns_str = ", ".join(pk_columns_escaped)
                            alter_pk = f"ALTER TABLE {nome_tabela} ADD CONSTRAINT pk_{nome_tabela} PRIMARY KEY ({pk_columns_str});"
                            try:
                                cursor_supabase.execute("SAVEPOINT sp_pk")
                                cursor_supabase.execute(alter_pk)
                                conn_supabase.commit()
                                logger.info(f"Constraint PRIMARY KEY adicionada em {nome_tabela}: {pk_columns}")
                            except psycopg2.Error as e:
                                cursor_supabase.execute("ROLLBACK TO SAVEPOINT sp_pk")
                                conn_supabase.commit()
                                logger.error(f"Erro ao adicionar PRIMARY KEY em {nome_tabela}: {e}")

                
                logger.info(f"Carregando dados para a tabela {nome_tabela}.")
                
                
                dados_duckdb = conn_duckdb.execute(f"SELECT * FROM {nome_tabela}").fetchall()
                if dados_duckdb:
                    
                    colunas = [f'"{col[0]}"' for col in duck_schema]

                    # Inserção no Supabase
                    query_insert = f"INSERT INTO {nome_tabela} ({', '.join(colunas)}) VALUES %s ON CONFLICT DO NOTHING"

                    # Batch no Supabase
                    from psycopg2.extras import execute_values
                    try:
                        execute_values(cursor_supabase, query_insert, dados_duckdb)
                        conn_supabase.commit()
                        logger.info(f"{len(dados_duckdb)} registros inseridos na tabela {nome_tabela}.")
                    except Exception as e:
                        conn_supabase.rollback()
                        logger.error(f"Erro ao inserir dados na tabela {nome_tabela}: {e}")
                        raise
                else:
                    logger.warning(f"Não há dados para carregar na tabela {nome_tabela}.")

    except Exception as e:
        logger.error(f"Erro ao processar tabela {nome_tabela}: {e}")
        raise

```

### Função principal

A função principal `main` é responsável por orquestrar a migração de dados.

- Conecta ao Supabase.
- Cria a tabela de controle no Supabase.
- Obtém a lista de tabelas do DuckDB.
- Processa a tabela table_metadata primeiro para carregar os metadados.
- Ordena as tabelas considerando as dependências usando a ordenação topológica.
- Processa (migra) cada tabela na ordem correta.
- Fecha a conexão com o DuckDB ao final da execução.

```python
def main():
    
        
    with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                criar_tabela_controle(cursor_supabase)
```
