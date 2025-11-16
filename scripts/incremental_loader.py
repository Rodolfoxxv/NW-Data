import duckdb
import psycopg2
import logging
import json
import time
import socket
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
from collections import defaultdict, deque
from typing import Callable, Dict, List, Optional, Sequence, Tuple
from psycopg2.extras import execute_values
from psycopg2 import sql

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()


DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DESTINATION_PATH = os.getenv("DESTINATION_PATH")
TRUNCATE_BEFORE_LOAD = os.getenv("TRUNCATE_BEFORE_LOAD", "false").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
INCREMENTAL_TIMESTAMP_COLUMN = os.getenv("INCREMENTAL_TIMESTAMP_COLUMN")
DB_SSLMODE = os.getenv("DB_SSLMODE")
DB_SSLROOTCERT = os.getenv("DB_SSLROOTCERT")
if DB_SSLROOTCERT:
    DB_SSLROOTCERT = str(Path(DB_SSLROOTCERT).expanduser())
DB_FORCE_IPV4 = os.getenv("DB_FORCE_IPV4", "false").lower() == "true"
SUPABASE_MAX_RETRIES = int(os.getenv("SUPABASE_MAX_RETRIES", "3"))
SUPABASE_RETRY_BASE_SECONDS = float(os.getenv("SUPABASE_RETRY_BASE_SECONDS", "2"))
SUPABASE_RETRY_MAX_SECONDS = float(os.getenv("SUPABASE_RETRY_MAX_SECONDS", "30"))
SUPABASE_TRANSIENT_ERRORS = (
    "connection already closed",
    "server closed the connection unexpectedly",
    "terminating connection due to administrator command",
    "network is unreachable",
    "no route to host",
)


TIMESTAMP_CANDIDATES = [
    "updated_at",
    "updated_on",
    "modified_at",
    "modified_on",
    "last_update",
    "last_updated",
    "created_at",
    "created_on",
    "ts_update",
    "dt_update",
    "ultima_atualizacao",
    "data_atualizacao",
]

REQUIRED_ENV_VARS = [
    ("DUCKDB_PATH", DUCKDB_PATH),
    ("DB_HOST", DB_HOST),
    ("DB_NAME", DB_NAME),
    ("DB_USER", DB_USER),
    ("DB_PASSWORD", DB_PASSWORD),
    ("DESTINATION_PATH", DESTINATION_PATH),
]

missing_vars = [name for name, value in REQUIRED_ENV_VARS if not value]
if missing_vars:
    raise ValueError(
        "Variáveis de ambiente ausentes. Verifique o arquivo .env: "
        + ", ".join(missing_vars)
    )

conn_duckdb: Optional[duckdb.DuckDBPyConnection] = None
RESOLVED_DB_HOST_IPV4: Optional[str] = None


def _get_or_resolve_db_host_ipv4() -> Optional[str]:
    """Resolve e armazena o IPv4 do host do banco para reutilizar posteriormente."""
    global RESOLVED_DB_HOST_IPV4
    if RESOLVED_DB_HOST_IPV4 or not DB_HOST:
        return RESOLVED_DB_HOST_IPV4
    try:
        RESOLVED_DB_HOST_IPV4 = resolve_ipv4_address(DB_HOST)
        logger.info(f"Hostname {DB_HOST} resolvido para IPv4 {RESOLVED_DB_HOST_IPV4}.")
    except Exception as e:
        logger.warning(
            f"Não foi possível resolver IPv4 para {DB_HOST}. Prosseguindo sem hostaddr: {e}"
        )
        RESOLVED_DB_HOST_IPV4 = None
    return RESOLVED_DB_HOST_IPV4


def is_transient_supabase_error(error: Exception) -> bool:
    """Verifica se o erro indica uma queda temporária na conexão com o Supabase/Postgres."""
    msg = str(error).lower()
    return any(fragment in msg for fragment in SUPABASE_TRANSIENT_ERRORS)


def resolve_ipv4_address(hostname: str) -> str:
    """Resolve um hostname para IPv4, falhando explicitamente se não for possível."""
    infos = socket.getaddrinfo(hostname, None, socket.AF_INET)
    if not infos:
        raise ValueError(f"Não foi possível resolver IPv4 para {hostname}")
    return infos[0][4][0]


def get_postgres_connection_params() -> Dict[str, str]:
    """Monta os parâmetros de conexão ao Postgres/Supabase, incluindo SSL se configurado."""
    params: Dict[str, str] = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "port": DB_PORT,
    }
    if DB_SSLMODE:
        params["sslmode"] = DB_SSLMODE
    if DB_SSLROOTCERT:
        params["sslrootcert"] = DB_SSLROOTCERT
    if DB_FORCE_IPV4 and DB_HOST:
        ipv4 = _get_or_resolve_db_host_ipv4()
        if ipv4:
            params["hostaddr"] = ipv4
    return params


def conectar_supabase() -> psycopg2.extensions.connection:
    """Abre conexão com supabase, forçando IPv4 automaticamente em caso de falha IPv6."""
    params = get_postgres_connection_params()
    try:
        return psycopg2.connect(**params)
    except psycopg2.OperationalError as e:
        msg = str(e).lower()
        if params.get("hostaddr") or not DB_HOST:
            raise
        if ("network is unreachable" not in msg) and ("no route to host" not in msg):
            raise
        ipv4 = _get_or_resolve_db_host_ipv4()
        if not ipv4:
            raise
        logger.warning(
            f"Falha ao conectar usando IPv6 ({e}). Tentando novamente com IPv4 {ipv4}."
        )
        params["hostaddr"] = ipv4
        return psycopg2.connect(**params)


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Abre (ou reutiliza) a conexão com o arquivo DuckDB."""
    global conn_duckdb
    if conn_duckdb is None:
        destination_path_obj = Path(DESTINATION_PATH)
        full_duckdb_path = (destination_path_obj / DUCKDB_PATH).resolve()
        if not full_duckdb_path.is_file():
            raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")
        logger.info(f"Conectando ao DuckDB em: {full_duckdb_path}")
        conn_duckdb = duckdb.connect(str(full_duckdb_path))
    return conn_duckdb


def close_duckdb_connection() -> None:
    """Fecha a conexão global com o DuckDB."""
    global conn_duckdb
    if conn_duckdb is not None:
        conn_duckdb.close()
        conn_duckdb = None
        logger.info("Conexão DuckDB encerrada.")


def mapear_tipo_duckdb_para_postgres_type(tipo_duckdb: str) -> str:
    """Mapeia tipos DuckDB para tipos equivalentes do Postgres."""
    tipo = tipo_duckdb.upper()
    if "TINYINT" in tipo:
        return "SMALLINT"
    if "BLOB" in tipo:
        return "BYTEA"
    if "DOUBLE" in tipo:
        return "DOUBLE PRECISION"
    if "DECIMAL" in tipo:
        return "NUMERIC" + tipo.split("DECIMAL")[1]
    return tipo


def criar_tabela_controle(cursor_supabase) -> None:
    """Cria a tabela de controle se não existir."""
    tabela_controle = sql.Identifier("controle_cargas")
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {tabela} (
            tabela_nome TEXT PRIMARY KEY,
            ultima_carga TIMESTAMP,
            linhas_carregadas INT
        );
        """
    ).format(tabela=tabela_controle)
    cursor_supabase.execute(query)
    logger.info("Tabela de controle 'controle_cargas' criada ou verificada.")


def get_duckdb_table_schema(nome_tabela: str) -> Tuple[List[Tuple], List[str]]:
    """Obtém o schema da tabela (DESCRIBE) e colunas PK (PRAGMA table_info)."""
    conn = get_duckdb_connection()
    duck_schema = conn.execute(f"DESCRIBE {nome_tabela}").fetchall()
    pk_info = conn.execute(f"PRAGMA table_info('{nome_tabela}')").fetchall()
    pk_columns = [row[1] for row in pk_info if row[5] > 0]  # row[5]: indicador de PK
    return duck_schema, pk_columns


def detectar_coluna_timestamp_incremental(nome_tabela: str) -> Optional[str]:
    """Tenta detectar automaticamente uma coluna de timestamp para carga incremental."""
    conn = get_duckdb_connection()
    info = conn.execute(f"PRAGMA table_info('{nome_tabela}')").fetchall()
    cols: List[Tuple[int, str, str]] = [(row[0], row[1], (row[2] or "").upper()) for row in info]

    def pick_by_names(candidates: Sequence[str], types_ok: Sequence[str]) -> Optional[str]:
        cand_lower = [c.lower() for c in candidates]
        for _, name, dtype in cols:
            if dtype.split("(")[0] in types_ok and name.lower() in cand_lower:
                return name
        return None

    col = pick_by_names(TIMESTAMP_CANDIDATES, ("TIMESTAMP",))
    if col:
        return col
    for _, name, dtype in cols:
        if dtype.split("(")[0] == "TIMESTAMP":
            return name
    col = pick_by_names(TIMESTAMP_CANDIDATES, ("DATE",))
    if col:
        return col
    return None


def get_duckdb_fk_info(nome_tabela: str) -> List[Tuple[str, str, str]]:
    """Recupera informações de FK a partir da tabela table_metadata, se houver."""
    fk_info: List[Tuple[str, str, str]] = []
    try:
        conn = get_duckdb_connection()
        result = conn.execute(
            f"SELECT schema_json FROM table_metadata WHERE table_name = '{nome_tabela}'"
        ).fetchone()
        if result:
            schema_json = json.loads(result[0])
            for col, definicao in schema_json.items():
                fk = definicao.get("foreign_key")
                if fk and fk.get("table") and fk.get("column"):
                    fk_info.append((col, fk["table"], fk["column"]))
    except Exception as e:
        logger.warning(f"Não foi possível obter FK do DuckDB para {nome_tabela}: {e}")
    return fk_info


def verificar_e_corrigir_constraints(
    cursor_supabase,
    nome_tabela: str,
    duck_pk_columns: List[str],
    duck_fk_info: List[Tuple[str, str, str]],
) -> None:
    """Sincroniza PK e FK no Postgres de acordo com o que existe no DuckDB."""
    cursor_supabase.execute(
        """
        SELECT kcu.column_name FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
        """,
        (nome_tabela,),
    )
    supabase_pk = [row[0] for row in cursor_supabase.fetchall()]
    if set(duck_pk_columns) != set(supabase_pk):
        try:
            cursor_supabase.execute(
                sql.SQL("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};").format(
                    sql.Identifier(nome_tabela), sql.Identifier(f"pk_{nome_tabela}")
                )
            )
        except Exception as e:
            logger.warning(f"Erro ao remover PK antiga em {nome_tabela}: {e}")
        if duck_pk_columns:
            query = sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} PRIMARY KEY ({});").format(
                sql.Identifier(nome_tabela),
                sql.Identifier(f"pk_{nome_tabela}"),
                sql.SQL(", ").join([sql.Identifier(col) for col in duck_pk_columns]),
            )
            cursor_supabase.execute(query)
            logger.info(f"PK atualizada para {nome_tabela}: {duck_pk_columns}")

    cursor_supabase.execute(
        """
        SELECT kcu.column_name, ccu.table_name, ccu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
        WHERE tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY';
        """,
        (nome_tabela,),
    )
    supabase_fks = cursor_supabase.fetchall()
    supabase_fk_set = set(supabase_fks)
    for fk_col, ref_table, ref_col in duck_fk_info:
        if (fk_col, ref_table, ref_col) not in supabase_fk_set:
            constraint_name = f"fk_{nome_tabela}_{fk_col}"
            try:
                cursor_supabase.execute("SAVEPOINT sp_fk")
                query = sql.SQL(
                    "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({});"
                ).format(
                    sql.Identifier(nome_tabela),
                    sql.Identifier(constraint_name),
                    sql.Identifier(fk_col),
                    sql.Identifier(ref_table),
                    sql.Identifier(ref_col),
                )
                cursor_supabase.execute(query)
                logger.info(
                    f"FK adicionada em {nome_tabela}: {fk_col} -> {ref_table}({ref_col})"
                )
            except Exception as e:
                cursor_supabase.execute("ROLLBACK TO SAVEPOINT sp_fk")
                logger.error(f"Erro ao adicionar FK em {nome_tabela} para coluna {fk_col}: {e}")


def executar_operacao_supabase(
    func: Callable[[], None], contexto: str
) -> None:
    """Executa uma operação contra o Supabase com retentativas automáticas."""
    last_error: Optional[Exception] = None
    for tentativa in range(1, SUPABASE_MAX_RETRIES + 1):
        try:
            func()
            return
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            last_error = e
            if (not is_transient_supabase_error(e)) or tentativa == SUPABASE_MAX_RETRIES:
                logger.error(f"Erro ao {contexto}: {e}")
                raise
            espera = min(
                SUPABASE_RETRY_BASE_SECONDS * tentativa, SUPABASE_RETRY_MAX_SECONDS
            )
            logger.warning(
                f"Falha ao {contexto} (tentativa {tentativa}/{SUPABASE_MAX_RETRIES}). "
                f"Nova tentativa em {espera:.1f}s."
            )
            time.sleep(espera)
        except Exception as e:
            logger.error(f"Erro inesperado ao {contexto}: {e}")
            raise
    if last_error:
        raise last_error


def _processar_tabela_once(nome_tabela: str) -> None:
    """Executa uma única tentativa de processamento (criar/atualizar) da tabela destino."""
    with conectar_supabase() as conn_supabase:
        with conn_supabase.cursor() as cursor_supabase:
            ini = datetime.now()

            cursor_supabase.execute(
                "SELECT ultima_carga, linhas_carregadas FROM controle_cargas WHERE tabela_nome = %s",
                (nome_tabela,),
            )
            row_ctrl = cursor_supabase.fetchone()
            existe = row_ctrl is not None
            ultima_carga_prev: Optional[datetime] = row_ctrl[0] if row_ctrl else None

            duck_schema, duck_pk_columns = get_duckdb_table_schema(nome_tabela)
            duck_fk_info = get_duckdb_fk_info(nome_tabela)

            if not existe:
                logger.info(f"Criando tabela {nome_tabela} no Supabase.")
                colunas_supabase = []
                for col in duck_schema:
                    tipo_postgres = mapear_tipo_duckdb_para_postgres_type(col[1])
                    colunas_supabase.append(f'"{col[0]}" {tipo_postgres}')
                if duck_pk_columns:
                    pk_cols_str = ", ".join([f'"{col}"' for col in duck_pk_columns])
                    colunas_supabase.append(f"PRIMARY KEY ({pk_cols_str})")
                query_criar = (
                    f"CREATE TABLE {nome_tabela} ({', '.join(colunas_supabase)});"
                )
                cursor_supabase.execute(query_criar)
                conn_supabase.commit()
                logger.info(f"Tabela {nome_tabela} criada no Supabase.")
                cursor_supabase.execute(
                    "INSERT INTO controle_cargas (tabela_nome, ultima_carga, linhas_carregadas) VALUES (%s, %s, %s)",
                    (nome_tabela, None, 0),
                )
                conn_supabase.commit()
            else:
                logger.info(
                    f"Tabela {nome_tabela} já existe no Supabase. Verificando constraints..."
                )
                verificar_e_corrigir_constraints(
                    cursor_supabase, nome_tabela, duck_pk_columns, duck_fk_info
                )

            if TRUNCATE_BEFORE_LOAD:
                cursor_supabase.execute(
                    f"TRUNCATE TABLE {nome_tabela} RESTART IDENTITY CASCADE;"
                )
                conn_supabase.commit()
                logger.info(f"Tabela {nome_tabela} truncada com sucesso.")

            ts_col = INCREMENTAL_TIMESTAMP_COLUMN or detectar_coluna_timestamp_incremental(
                nome_tabela
            )
            if TRUNCATE_BEFORE_LOAD:
                ultima_carga_prev = None

            if ts_col and ultima_carga_prev is not None:
                query_duck = f"SELECT * FROM {nome_tabela} WHERE {ts_col} > ?"
                params = [ultima_carga_prev]
            else:
                query_duck = f"SELECT * FROM {nome_tabela}"
                params: List[datetime] = []

            col_names = [f'"{col[0]}"' for col in duck_schema]
            query_insert = (
                f"INSERT INTO {nome_tabela} ({', '.join(col_names)}) VALUES %s ON CONFLICT DO NOTHING"
            )

            total_inseridos = 0
            max_ts: Optional[datetime] = ultima_carga_prev

            conn_duck = get_duckdb_connection()
            duck_cursor = conn_duck.execute(query_duck, params)
            while True:
                lote = duck_cursor.fetchmany(BATCH_SIZE)
                if not lote:
                    break
                try:
                    execute_values(
                        cursor_supabase,
                        query_insert,
                        lote,
                        page_size=BATCH_SIZE,
                    )
                    conn_supabase.commit()
                    total_inseridos += len(lote)
                    if ts_col:
                        idx_ts = next(
                            (i for i, c in enumerate(duck_schema) if c[0] == ts_col),
                            None,
                        )
                        if idx_ts is not None:
                            for row in lote:
                                ts_val = row[idx_ts]
                                if isinstance(ts_val, datetime):
                                    if (max_ts is None) or (ts_val > max_ts):
                                        max_ts = ts_val
                except Exception as e:
                    try:
                        conn_supabase.rollback()
                    except psycopg2.InterfaceError:
                        logger.warning(
                            "Rollback ignorado pois a conexão com o Supabase já estava encerrada."
                        )
                    logger.error(f"Erro ao inserir lote na tabela {nome_tabela}: {e}")
                    raise

            if total_inseridos > 0:
                nova_ultima_carga = max_ts if (ts_col and max_ts) else datetime.now()
                cursor_supabase.execute(
                    "UPDATE controle_cargas SET ultima_carga = %s, linhas_carregadas = COALESCE(linhas_carregadas,0) + %s WHERE tabela_nome = %s",
                    (nova_ultima_carga, total_inseridos, nome_tabela),
                )
                conn_supabase.commit()
                dur = (datetime.now() - ini).total_seconds()
                logger.info(
                    f"{total_inseridos} registros inseridos em {nome_tabela} (tempo: {dur:.2f}s). Última carga: {nova_ultima_carga}."
                )
            else:
                logger.info(f"Nenhum registro novo para {nome_tabela}.")


def processar_tabela(nome_tabela: str) -> None:
    """Processa a tabela com retentativas automáticas em caso de queda de conexão."""
    last_error: Optional[Exception] = None
    for tentativa in range(1, SUPABASE_MAX_RETRIES + 1):
        try:
            _processar_tabela_once(nome_tabela)
            return
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            last_error = e
            if (not is_transient_supabase_error(e)) or tentativa == SUPABASE_MAX_RETRIES:
                logger.error(f"Erro ao processar tabela {nome_tabela}: {e}")
                raise
            espera = min(
                SUPABASE_RETRY_BASE_SECONDS * tentativa, SUPABASE_RETRY_MAX_SECONDS
            )
            logger.warning(
                f"Conexão com Supabase perdida ao processar {nome_tabela} "
                f"(tentativa {tentativa}/{SUPABASE_MAX_RETRIES}). Nova tentativa em {espera:.1f}s."
            )
            time.sleep(espera)
        except Exception as e:
            logger.error(f"Erro ao processar tabela {nome_tabela}: {e}")
            raise
    if last_error:
        raise last_error


def ordenar_tabelas_topologicamente(
    tabelas: List[str], metadados: Dict[str, str]
) -> List[str]:
    """Ordena as tabelas com base nas dependências inferidas em schema_json."""
    grafo: Dict[str, List[str]] = defaultdict(list)
    in_degree: Dict[str, int] = {tabela: 0 for tabela in tabelas}
    for tabela in tabelas:
        schema_json_str = metadados.get(tabela)
        dependencias: List[str] = []
        if schema_json_str:
            try:
                schema_dict = json.loads(schema_json_str)
                for _, definicao in schema_dict.items():
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
    ordenadas: List[str] = []
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


def main() -> None:
    try:
        def inicializar_tabela_controle() -> None:
            with conectar_supabase() as conn_supabase:
                with conn_supabase.cursor() as cursor_supabase:
                    criar_tabela_controle(cursor_supabase)

        executar_operacao_supabase(
            inicializar_tabela_controle, "criar/verificar tabela de controle"
        )

        conn_duck = get_duckdb_connection()
        tabelas_duckdb = conn_duck.execute("SHOW TABLES").fetchall()
        nomes_tabelas = [tabela[0] for tabela in tabelas_duckdb]

        if "table_metadata" in nomes_tabelas:
            logger.info("Processando tabela 'table_metadata' primeiro.")
            processar_tabela("table_metadata")
            nomes_tabelas.remove("table_metadata")

        metadados: Dict[str, str] = {}
        try:
            conn_duck = get_duckdb_connection()
            resultados = conn_duck.execute(
                "SELECT table_name, schema_json FROM table_metadata"
            ).fetchall()
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
        close_duckdb_connection()
        logger.info("Pipeline concluído.")


if __name__ == "__main__":
    main()
