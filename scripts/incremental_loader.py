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

if not all([SUPABASE_URL, SUPABASE_KEY, DUCKDB_PATH, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

full_duckdb_path = Path("D:/Projetos/NW-Data/data") / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()

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

                    # Obter as colunas da chave primária usando o PRAGMA do DuckDB
                    query_pk = f"SELECT name FROM pragma_table_info('{nome_tabela}') WHERE pk > 0 ORDER BY pk;"
                    pk_columns = conn_duckdb.execute(query_pk).fetchall()
                    pk_columns = [col[0] for col in pk_columns]
                    if pk_columns:
                        pk_columns_escaped = [f'"{col}"' for col in pk_columns]
                        colunas_supabase.append(f"PRIMARY KEY ({', '.join(pk_columns_escaped)})")

                    query_criar_tabela_supabase = f"CREATE TABLE {nome_tabela} ({', '.join(colunas_supabase)})"
                    cursor_supabase.execute(query_criar_tabela_supabase)
                    conn_supabase.commit()
                    logger.info(f"Tabela {nome_tabela} criada no Supabase.")
                    pk_definida = True
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
                        # Obter a definição da PK a partir do DuckDB
                        query_pk_duck = f"SELECT name FROM pragma_table_info('{nome_tabela}') WHERE pk > 0 ORDER BY pk;"
                        pk_columns = conn_duckdb.execute(query_pk_duck).fetchall()
                        pk_columns = [col[0] for col in pk_columns]
                        if pk_columns:
                            pk_columns_escaped = [f'"{col}"' for col in pk_columns]
                            alter_pk = f'ALTER TABLE {nome_tabela} ADD CONSTRAINT pk_{nome_tabela} PRIMARY KEY ({", ".join(pk_columns_escaped)});'
                            try:
                                cursor_supabase.execute("SAVEPOINT sp_pk")
                                cursor_supabase.execute(alter_pk)
                                conn_supabase.commit()
                                logger.info(f"Constraint PRIMARY KEY adicionada em {nome_tabela}: {pk_columns}")
                                pk_definida = True
                            except psycopg2.Error as e:
                                cursor_supabase.execute("ROLLBACK TO SAVEPOINT sp_pk")
                                conn_supabase.commit()
                                logger.error(f"Erro ao adicionar PRIMARY KEY em {nome_tabela}: {e}")
                                pk_definida = False
                        else:
                            logger.warning(f"Não foram encontradas colunas de PK em {nome_tabela} no DuckDB.")
                            pk_definida = False
                    else:
                        pk_definida = True

                # --- ATUALIZAÇÃO/ADIÇÃO DE CHAVES ESTRANGEIRAS A PARTIR DE table_metadata ---
                # Se a tabela for "table_metadata", como ela já foi carregada (ou será carregada primeiro), não consultamos seus metadados
                if nome_tabela != "table_metadata":
                    query_metadados = f"SELECT schema_json FROM table_metadata WHERE table_name = %s"
                    try:
                        cursor_supabase.execute(query_metadados, (nome_tabela,))
                        resultado = cursor_supabase.fetchone()
                    except psycopg2.Error as e:
                        logger.error(f"Erro ao consultar metadados para {nome_tabela}: {e}")
                        resultado = None

                    if resultado:
                        schema_json_str = resultado[0]
                        try:
                            schema_dict = json.loads(schema_json_str)
                        except json.JSONDecodeError as e:
                            logger.error(f"Erro ao decodificar JSON dos metadados da tabela {nome_tabela}: {e}")
                            schema_dict = {}
    
                        for nome_coluna, definicao in schema_dict.items():
                            fk_def = definicao.get("foreign_key")
                            if fk_def:
                                # Tenta criar FK somente se a tabela referenciada possuir PK/unique
                                tabela_ref = fk_def.get("table")
                                coluna_ref = fk_def.get("column")
                                on_update = fk_def.get("on_update", "NO ACTION")
                                on_delete = fk_def.get("on_delete", "NO ACTION")
                                constraint_name = f"fk_{nome_tabela}_{nome_coluna}_{tabela_ref}"
    
                                # Verifica se a constraint já existe no Supabase
                                query_constraint = """
                                    SELECT 1 FROM information_schema.table_constraints 
                                    WHERE table_name = %s AND constraint_name = %s;
                                """
                                cursor_supabase.execute(query_constraint, (nome_tabela, constraint_name))
                                if cursor_supabase.fetchone():
                                    logger.info(f"Constraint {constraint_name} já existe em {nome_tabela}.")
                                    continue
    
                                # Verifica se a tabela referenciada existe no Supabase
                                query_table_exists = """
                                    SELECT EXISTS(
                                        SELECT 1 FROM information_schema.tables 
                                        WHERE table_name = %s
                                    );
                                """
                                cursor_supabase.execute(query_table_exists, (tabela_ref,))
                                if not cursor_supabase.fetchone()[0]:
                                    logger.info(
                                        f"Tabela referenciada {tabela_ref} não existe no Supabase. FK {constraint_name} não será criada."
                                    )
                                    continue
    
                                # Verifica se a coluna de referência possui constraint única ou é PK
                                query_unique = """
                                    SELECT COUNT(*) 
                                    FROM information_schema.table_constraints tc
                                    JOIN information_schema.key_column_usage kcu
                                      ON tc.constraint_name = kcu.constraint_name
                                    WHERE tc.table_name = %s 
                                      AND kcu.column_name = %s
                                      AND (tc.constraint_type = 'PRIMARY KEY' OR tc.constraint_type = 'UNIQUE');
                                """
                                cursor_supabase.execute(query_unique, (tabela_ref, coluna_ref))
                                unique_count = cursor_supabase.fetchone()[0]
                                if unique_count == 0:
                                    logger.error(
                                        f"Não foi encontrada constraint única ou primary key para a coluna '{coluna_ref}' na tabela '{tabela_ref}'. FK {constraint_name} não será criada."
                                    )
                                    continue
    
                                fk_query = f"""
                                    ALTER TABLE {nome_tabela}
                                    ADD CONSTRAINT {constraint_name}
                                    FOREIGN KEY ("{nome_coluna}")
                                    REFERENCES {tabela_ref} ("{coluna_ref}")
                                """
                                if on_update.upper() != "NO ACTION":
                                    fk_query += f" ON UPDATE {on_update}"
                                if on_delete.upper() != "NO ACTION":
                                    fk_query += f" ON DELETE {on_delete}"
                                try:
                                    cursor_supabase.execute(fk_query)
                                    conn_supabase.commit()
                                    logger.info(f"Constraint FK {constraint_name} adicionada em {nome_tabela}.")
                                except psycopg2.Error as e:
                                    logger.error(f"Erro ao adicionar FK {constraint_name} em {nome_tabela}: {e}")
                                    conn_supabase.rollback()
                    else:
                        logger.warning(f"Metadados para a tabela {nome_tabela} não encontrados em Supabase.")
                else:
                    logger.info("Tabela table_metadata: pulando etapa de metadados.")
    
                # --- INSERÇÃO DOS DADOS ---
                dados = conn_duckdb.execute(f"SELECT * FROM {nome_tabela}").fetchall()
                colunas_nomes = [f'"{col}"' for col in duck_columns.keys()]
                query_inserir = f"INSERT INTO {nome_tabela} ({', '.join(colunas_nomes)}) VALUES ({', '.join(['%s'] * len(colunas_nomes))})"
    
                for linha in dados:
                    linha_convertida = [
                        mapear_tipo_duckdb_para_postgres(valor, duck_columns[col.strip('"')])
                        for valor, col in zip(linha, colunas_nomes)
                    ]
                    cursor_supabase.execute(query_inserir, linha_convertida)
                conn_supabase.commit()
    
                # Atualiza a tabela de controle com a informação da carga
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
