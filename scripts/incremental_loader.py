import duckdb
import psycopg2
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
from collections import defaultdict, deque

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

if not all([SUPABASE_URL, SUPABASE_KEY, DUCKDB_PATH, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DESTINATION_PATH]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

destination_path_obj = Path(DESTINATION_PATH)

full_duckdb_path = destination_path_obj / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()

if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")

logging.info(f"DUCKDB_PATH: {full_duckdb_path}")

conn_duckdb = duckdb.connect(str(full_duckdb_path))


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


def mapear_tipo_duckdb_para_postgres(valor, tipo):
    if valor is None:
        return None
    if "VARCHAR" in tipo or "TEXT" in tipo:
        return str(valor)
    if "BOOLEAN" in tipo:
        return bool(valor)
    if "INTEGER" in tipo or "BIGINT" in tipo or "TINYINT" in tipo:
        return int(valor)
    if "DECIMAL" in tipo or "FLOAT" in tipo or "DOUBLE" in tipo or "REAL" in tipo:
        return float(valor)
    if "BLOB" in tipo:
        return bytes(valor)
    if "TIMESTAMP" in tipo or "DATETIME" in tipo:
        try:
            return datetime.strptime(str(valor), "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            return datetime.strptime(str(valor), "%Y-%m-%d %H:%M:%S")
    return valor


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


def processar_tabela(nome_tabela):
    try:
        with psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        ) as conn_supabase:
            with conn_supabase.cursor() as cursor_supabase:
                cursor_supabase.execute(
                    "SELECT EXISTS(SELECT 1 FROM controle_cargas WHERE tabela_nome = %s)",
                    (nome_tabela,),
                )
                existe = cursor_supabase.fetchone()[0]

                query_schema_pk = f"""
                    SELECT name, type, pk FROM pragma_table_info('{nome_tabela}')
                    ORDER BY pk;
                """
                duck_info = conn_duckdb.execute(query_schema_pk).fetchall()

                duck_columns = {col[0]: col[1] for col in duck_info}
                pk_columns = [col[0] for col in duck_info if col[2] > 0]

                if not existe:
                    colunas_supabase = []
                    for col in duck_info:
                        nome_coluna = col[0]
                        tipo_duckdb = col[1]
                        tipo_postgres = mapear_tipo_duckdb_para_postgres_type(tipo_duckdb)
                        colunas_supabase.append(f'"{nome_coluna}" {tipo_postgres}')

                    if pk_columns:
                        pk_columns_escaped = [f'"{col}"' for col in pk_columns]
                        colunas_supabase.append(f"PRIMARY KEY ({', '.join(pk_columns_escaped)})")

                    query_criar_tabela_supabase = f"CREATE TABLE {nome_tabela} ({', '.join(colunas_supabase)})"
                    cursor_supabase.execute(query_criar_tabela_supabase)
                    conn_supabase.commit()
                    pk_definida = True
                else:
                    query_pk_exists = """
                        SELECT COUNT(*) FROM information_schema.table_constraints 
                        WHERE table_name = %s AND constraint_type = 'PRIMARY KEY';
                    """
                    cursor_supabase.execute(query_pk_exists, (nome_tabela,))
                    pk_count = cursor_supabase.fetchone()[0]
                    if pk_count == 0 and pk_columns:
                        pk_columns_escaped = [f'"{col}"' for col in pk_columns]
                        alter_pk = f'ALTER TABLE {nome_tabela} ADD CONSTRAINT pk_{nome_tabela} PRIMARY KEY ({', '.join(pk_columns_escaped)});'
                        try:
                            cursor_supabase.execute("SAVEPOINT sp_pk")
                            cursor_supabase.execute(alter_pk)
                            conn_supabase.commit()
                            pk_definida = True
                        except psycopg2.Error:
                            cursor_supabase.execute("ROLLBACK TO SAVEPOINT sp_pk")
                            conn_supabase.commit()
                            pk_definida = False
                    else:
                        pk_definida = True

                cursor_supabase.execute(f"SELECT MAX({pk_columns[0]}) FROM {nome_tabela}")
                ultima_chave = cursor_supabase.fetchone()[0] if pk_columns else None

                dados_query = f"SELECT * FROM {nome_tabela}"
                if ultima_chave is not None:
                    dados_query += f" WHERE {pk_columns[0]} > {ultima_chave}"
                dados = conn_duckdb.execute(dados_query).fetchall()

                colunas_nomes = [f'"{col}"' for col in duck_columns.keys()]
                query_inserir = f"INSERT INTO {nome_tabela} ({', '.join(colunas_nomes)}) VALUES ({', '.join(['%s'] * len(colunas_nomes))})"

                for linha in dados:
                    linha_convertida = [
                        mapear_tipo_duckdb_para_postgres(valor, duck_columns[col.strip('"')])
                        for valor, col in zip(linha, colunas_nomes)
                    ]
                    cursor_supabase.execute(query_inserir, linha_convertida)
                conn_supabase.commit()

                cursor_supabase.execute(
                    """
                    INSERT INTO controle_cargas (tabela_nome, ultima_carga, linhas_carregadas)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (tabela_nome)
                    DO UPDATE SET ultima_carga = EXCLUDED.ultima_carga, linhas_carregadas = EXCLUDED.linhas_carregadas;
                    """,
                    (nome_tabela, datetime.now(), len(dados)),
                )
                conn_supabase.commit()
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
        # Obtém as dependências a partir dos metadados, se disponíveis
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
        nomes_tabelas_duckdb = [tabela[0] for tabela in tabelas_duckdb]
    
        # Se "table_metadata" estiver presente, processa-a primeiro
        if "table_metadata" in nomes_tabelas_duckdb:
            logger.info("Processando tabela 'table_metadata' primeiro.")
            processar_tabela("table_metadata")
            # Remover da lista para evitar processamento duplicado
            nomes_tabelas_duckdb.remove("table_metadata")
    
        # Carregar os metadados do DuckDB (usados para ordenação) a partir da própria tabela "table_metadata"
        metadados = {}
        try:
            resultados = conn_duckdb.execute("SELECT table_name, schema_json FROM table_metadata").fetchall()
            for row in resultados:
                metadados[row[0]] = row[1]
        except Exception as e:
            logger.warning(f"Não foi possível obter metadados de todas as tabelas: {e}")
    
        # Ordenar as tabelas com base nas dependências obtidas dos metadados
        tabelas_ordenadas = ordenar_tabelas_topologicamente(nomes_tabelas_duckdb, metadados)
    
        # Processar as tabelas na ordem correta
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
