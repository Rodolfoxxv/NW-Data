import duckdb
import psycopg2
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
from collections import defaultdict, deque
from psycopg2.extras import execute_values

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
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
TRUNCATE_BEFORE_LOAD = os.getenv("TRUNCATE_BEFORE_LOAD", "false").lower() == "true"

if not all([SUPABASE_URL, SUPABASE_KEY, DUCKDB_PATH, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DESTINATION_PATH]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

destination_path_obj = Path(DESTINATION_PATH)
full_duckdb_path = (destination_path_obj / DUCKDB_PATH).resolve()
if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")

logger.info(f"DUCKDB_PATH: {full_duckdb_path}")

conn_duckdb = duckdb.connect(str(full_duckdb_path))


def mapear_tipo_duckdb_para_postgres_type(tipo_duckdb):
    tipo = tipo_duckdb.upper()
    if 'TINYINT' in tipo:
        return 'SMALLINT'
    elif 'BLOB' in tipo:
        return 'BYTEA'
    elif 'DOUBLE' in tipo:
        return 'DOUBLE PRECISION'
    elif 'DECIMAL' in tipo:
        return 'NUMERIC' + tipo.split('DECIMAL')[1]
    else:
        return tipo


def criar_tabela_controle(cursor_supabase):
    tabela_controle = "controle_cargas"
    query = f"""
    CREATE TABLE IF NOT EXISTS {tabela_controle} (
        tabela_nome TEXT PRIMARY KEY,
        ultima_carga TIMESTAMP,
        linhas_carregadas INT
    );
    """
    cursor_supabase.execute(query)
    logger.info(f"Tabela de controle '{tabela_controle}' criada ou verificada.")


def get_duckdb_table_schema(nome_tabela):
    """
    Obtém o schema da tabela a partir de DESCRIBE e PRAGMA table_info.
    Retorna:
      - duck_schema: lista de tuplas com (coluna, tipo, ...)
      - pk_columns: lista de nomes de colunas que são PK.
    """
    duck_schema = conn_duckdb.execute(f"DESCRIBE {nome_tabela}").fetchall()
    pk_info = conn_duckdb.execute(f"PRAGMA table_info('{nome_tabela}')").fetchall()
    pk_columns = [row[1] for row in pk_info if row[5] > 0]  # row[5]: indicador de PK
    return duck_schema, pk_columns


def get_duckdb_fk_info(nome_tabela):
    """
    Tenta recuperar as informações de FK a partir da tabela de metadados (schema_json).
    Espera que o schema_json contenha, para cada coluna, um dicionário com a chave 'foreign_key'
    contendo {'table': <tabela_referenciada>, 'column': <coluna_referenciada>}.
    Retorna uma lista de tuplas: (coluna, tabela_referenciada, coluna_referenciada).
    """
    fk_info = []
    try:
        result = conn_duckdb.execute(f"SELECT schema_json FROM table_metadata WHERE table_name = '{nome_tabela}'").fetchone()
        if result:
            schema_json = json.loads(result[0])
            for col, definicao in schema_json.items():
                fk = definicao.get("foreign_key")
                if fk and fk.get("table") and fk.get("column"):
                    fk_info.append((col, fk["table"], fk["column"]))
    except Exception as e:
        logger.warning(f"Não foi possível obter FK do DuckDB para {nome_tabela}: {e}")
    return fk_info


def verificar_e_corrigir_constraints(cursor_supabase, nome_tabela, duck_pk_columns, duck_fk_info):
    # Verificar e atualizar PRIMARY KEY
    cursor_supabase.execute(f"""
        SELECT kcu.column_name FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
    """, (nome_tabela,))
    supabase_pk = [row[0] for row in cursor_supabase.fetchall()]
    if set(duck_pk_columns) != set(supabase_pk):
        try:
            cursor_supabase.execute(f"ALTER TABLE {nome_tabela} DROP CONSTRAINT IF EXISTS pk_{nome_tabela};")
        except Exception as e:
            logger.warning(f"Erro ao remover PK antiga em {nome_tabela}: {e}")
        if duck_pk_columns:
            pk_cols_str = ", ".join([f'\"{col}\"' for col in duck_pk_columns])
            alter_pk = f"ALTER TABLE {nome_tabela} ADD CONSTRAINT pk_{nome_tabela} PRIMARY KEY ({pk_cols_str});"
            cursor_supabase.execute(alter_pk)
            logger.info(f"PK atualizada para {nome_tabela}: {duck_pk_columns}")

    # Verificar e atualizar FOREIGN KEYS
    cursor_supabase.execute(f"""
        SELECT kcu.column_name, ccu.table_name, ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY';
    """, (nome_tabela,))
    supabase_fks = cursor_supabase.fetchall()  # tuples: (coluna, tabela_ref, coluna_ref)
    supabase_fk_set = set(supabase_fks)
    for fk in duck_fk_info:
        if fk not in supabase_fk_set:
            constraint_name = f"fk_{nome_tabela}_{fk[0]}"
            alter_fk = (f"ALTER TABLE {nome_tabela} ADD CONSTRAINT {constraint_name} "
                        f"FOREIGN KEY (\"{fk[0]}\") REFERENCES {fk[1]}(\"{fk[2]}\");")
            try:
                cursor_supabase.execute("SAVEPOINT sp_fk")
                cursor_supabase.execute(alter_fk)
                logger.info(f"FK adicionada em {nome_tabela}: {fk[0]} -> {fk[1]}({fk[2]})")
            except Exception as e:
                cursor_supabase.execute("ROLLBACK TO SAVEPOINT sp_fk")
                logger.error(f"Erro ao adicionar FK em {nome_tabela} para coluna {fk[0]}: {e}")


def processar_tabela(nome_tabela):
    try:
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                # Verifica se a tabela já foi migrada (verificando a tabela de controle)
                cursor_supabase.execute(
                    "SELECT EXISTS(SELECT 1 FROM controle_cargas WHERE tabela_nome = %s)",
                    (nome_tabela,),
                )
                existe = cursor_supabase.fetchone()[0]

                duck_schema, duck_pk_columns = get_duckdb_table_schema(nome_tabela)
                duck_fk_info = get_duckdb_fk_info(nome_tabela)

                if not existe:
                    logger.info(f"Criando tabela {nome_tabela} no Supabase.")
                    colunas_supabase = []
                    for col in duck_schema:
                        tipo_postgres = mapear_tipo_duckdb_para_postgres_type(col[1])
                        colunas_supabase.append(f"\"{col[0]}\" {tipo_postgres}")
                    if duck_pk_columns:
                        pk_cols_str = ", ".join([f"\"{col}\"" for col in duck_pk_columns])
                        colunas_supabase.append(f"PRIMARY KEY ({pk_cols_str})")
                    query_criar = f"CREATE TABLE {nome_tabela} ({', '.join(colunas_supabase)});"
                    cursor_supabase.execute(query_criar)
                    conn_supabase.commit()
                    logger.info(f"Tabela {nome_tabela} criada no Supabase.")
                    cursor_supabase.execute(
                        "INSERT INTO controle_cargas (tabela_nome, ultima_carga, linhas_carregadas) VALUES (%s, %s, %s)",
                        (nome_tabela, datetime.now(), 0),
                    )
                    conn_supabase.commit()
                else:
                    logger.info(f"Tabela {nome_tabela} já existe no Supabase. Verificando constraints...")
                    verificar_e_corrigir_constraints(cursor_supabase, nome_tabela, duck_pk_columns, duck_fk_info)

                # Se a variável de ambiente indicar, realiza TRUNCATE antes de inserir os dados
                if TRUNCATE_BEFORE_LOAD:
                    cursor_supabase.execute(f"TRUNCATE TABLE {nome_tabela} RESTART IDENTITY CASCADE;")
                    conn_supabase.commit()
                    logger.info(f"Tabela {nome_tabela} truncada com sucesso.")

                # Carrega os dados do DuckDB
                dados = conn_duckdb.execute(f"SELECT * FROM {nome_tabela}").fetchall()
                if dados:
                    col_names = [f"\"{col[0]}\"" for col in duck_schema]
                    query_insert = f"INSERT INTO {nome_tabela} ({', '.join(col_names)}) VALUES %s ON CONFLICT DO NOTHING"
                    try:
                        execute_values(cursor_supabase, query_insert, dados)
                        conn_supabase.commit()
                        logger.info(f"{len(dados)} registros inseridos na tabela {nome_tabela}.")
                    except Exception as e:
                        conn_supabase.rollback()
                        logger.error(f"Erro ao inserir dados na tabela {nome_tabela}: {e}")
                        raise
                else:
                    logger.warning(f"Não há dados para carregar na tabela {nome_tabela}.")
    except Exception as e:
        logger.error(f"Erro ao processar tabela {nome_tabela}: {e}")
        raise


def ordenar_tabelas_topologicamente(tabelas, metadados):
    """
    Ordena as tabelas com base nas dependências encontradas no schema_json da table_metadata.
    Se os metadados não estiverem disponíveis para alguma tabela, considera-se que ela não possui dependências.
    """
    grafo = defaultdict(list)
    in_degree = {tabela: 0 for tabela in tabelas}
    for tabela in tabelas:
        schema_json_str = metadados.get(tabela)
        dependencias = []
        if schema_json_str:
            try:
                schema_dict = json.loads(schema_json_str)
                for col, definicao in schema_dict.items():
                    fk = definicao.get("foreign_key")
                    if fk and fk.get("table"):
                        dependencias.append(fk.get("table"))
            except Exception as e:
                logger.error(f"Erro ao processar metadados para {tabela}: {e}")
        for dep in dependencias:
            if dep in tabelas:
                grafo[dep].append(tabela)
                in_degree[tabela] += 1
    from collections import deque
    fila = deque([tabela for tabela in tabelas if in_degree[tabela] == 0])
    ordenadas = []
    while fila:
        node = fila.popleft()
        ordenadas.append(node)
        for neighbor in grafo[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                fila.append(neighbor)
    if len(ordenadas) != len(tabelas):
        logger.warning("Ciclo detectado nas dependências. Ordem pode não ser correta.")
        return tabelas
    return ordenadas


def main():
    try:
        # Cria a tabela de controle no Supabase
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                criar_tabela_controle(cursor_supabase)

        # Obter a lista de tabelas do DuckDB
        tabelas_duckdb = conn_duckdb.execute("SHOW TABLES").fetchall()
        nomes_tabelas = [tabela[0] for tabela in tabelas_duckdb]

        # Processar a tabela de metadados primeiro, se existir
        if "table_metadata" in nomes_tabelas:
            logger.info("Processando tabela 'table_metadata' primeiro.")
            processar_tabela("table_metadata")
            nomes_tabelas.remove("table_metadata")

        # Carregar metadados do DuckDB (para ordenação) a partir da tabela table_metadata
        metadados = {}
        try:
            resultados = conn_duckdb.execute("SELECT table_name, schema_json FROM table_metadata").fetchall()
            for row in resultados:
                metadados[row[0]] = row[1]
        except Exception as e:
            logger.warning(f"Não foi possível obter metadados de todas as tabelas: {e}")

        tabelas_ordenadas = ordenar_tabelas_topologicamente(nomes_tabelas, metadados)
        for nome_tabela in tabelas_ordenadas:
            processar_tabela(nome_tabela)
    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
        raise
    finally:
        conn_duckdb.close()
        logger.info("Pipeline concluído.")


if __name__ == "__main__":
    main()
