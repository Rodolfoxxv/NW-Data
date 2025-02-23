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
full_duckdb_path = (destination_path_obj / DUCKDB_PATH).resolve()
if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")

logger.info(f"DUCKDB_PATH: {full_duckdb_path}")

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

                    # Obter as colunas da chave primária usando PRAGMA do DuckDB
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

                    # Registrar a criação na tabela de controle
                    cursor_supabase.execute(
                        "INSERT INTO controle_cargas (tabela_nome, ultima_carga, linhas_carregadas) VALUES (%s, %s, %s)",
                        (nome_tabela, datetime.now(), 0),
                    )
                    conn_supabase.commit()
                else:
                    logger.info(f"Tabela {nome_tabela} já existe no Supabase. Verificando atualizações (constraints).")
                    
                    # Verificar e atualizar PRIMARY KEY se necessário
                    query_pk_exists = """
                        SELECT COUNT(*) FROM information_schema.table_constraints 
                        WHERE table_name = %s AND constraint_type = 'PRIMARY KEY';
                    """
                    cursor_supabase.execute(query_pk_exists, (nome_tabela,))
                    pk_count = cursor_supabase.fetchone()[0]
                    if pk_count == 0:
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
                    
                    # Verificar e atualizar FOREIGN KEY se necessário
                    query_fk_duck = f"PRAGMA foreign_key_list('{nome_tabela}')"
                    fk_info = conn_duckdb.execute(query_fk_duck).fetchall()
                    if fk_info:
                        # Consulta para obter as FKs já definidas no Supabase
                        query_fk_exists = """
                            SELECT kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
                            FROM information_schema.table_constraints AS tc
                            JOIN information_schema.key_column_usage AS kcu
                              ON tc.constraint_name = kcu.constraint_name
                            JOIN information_schema.constraint_column_usage AS ccu
                              ON ccu.constraint_name = tc.constraint_name
                            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s;
                        """
                        cursor_supabase.execute(query_fk_exists, (nome_tabela,))
                        existing_fks = cursor_supabase.fetchall()  # (column, foreign_table, foreign_column)
                        existing_fk_set = set(existing_fks)
                        
                        for fk in fk_info:
                            # PRAGMA foreign_key_list retorna: id, seq, table, from, to, on_update, on_delete, match
                            fk_column = fk[3]
                            ref_table = fk[2]
                            ref_column = fk[4]
                            if (fk_column, ref_table, ref_column) not in existing_fk_set:
                                alter_fk = f"""ALTER TABLE {nome_tabela} 
                                    ADD CONSTRAINT fk_{nome_tabela}_{fk_column} 
                                    FOREIGN KEY ("{fk_column}") 
                                    REFERENCES {ref_table}("{ref_column}");"""
                                try:
                                    cursor_supabase.execute("SAVEPOINT sp_fk")
                                    cursor_supabase.execute(alter_fk)
                                    conn_supabase.commit()
                                    logger.info(f"Constraint FOREIGN KEY adicionada em {nome_tabela}: (\"{fk_column}\") REFERENCES {ref_table}(\"{ref_column}\")")
                                except psycopg2.Error as e:
                                    cursor_supabase.execute("ROLLBACK TO SAVEPOINT sp_fk")
                                    conn_supabase.commit()
                                    logger.error(f"Erro ao adicionar FOREIGN KEY em {nome_tabela} para coluna {fk_column}: {e}")

                # Inserção de dados após criação/atualização da tabela
                logger.info(f"Carregando dados para a tabela {nome_tabela}.")
                dados_duckdb = conn_duckdb.execute(f"SELECT * FROM {nome_tabela}").fetchall()
                if dados_duckdb:
                    # Preparar os nomes das colunas escapadas
                    colunas = [f'"{col[0]}"' for col in duck_schema]
                    query_insert = f"INSERT INTO {nome_tabela} ({', '.join(colunas)}) VALUES %s ON CONFLICT DO NOTHING"
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
        nomes_tabelas_duckdb = [tabela[0] for tabela in tabelas_duckdb]
    
        # Processa a tabela de metadados primeiro, se existir
        if "table_metadata" in nomes_tabelas_duckdb:
            logger.info("Processando tabela 'table_metadata' primeiro.")
            processar_tabela("table_metadata")
            nomes_tabelas_duckdb.remove("table_metadata")
    
        # Carregar os metadados do DuckDB (para ordenação) a partir da tabela table_metadata
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
