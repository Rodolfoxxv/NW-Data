import duckdb
import psycopg2
import logging
import json
import time
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from collections import defaultdict, deque
from typing import Dict, List, Optional, Sequence, Tuple, Any
from psycopg2.extras import execute_values
from psycopg2 import sql
from abc import ABC, abstractmethod


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

load_dotenv()

# Constantes Globais
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DESTINATION_PATH = os.getenv("DESTINATION_PATH")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
TRUNCATE_BEFORE_LOAD = os.getenv("TRUNCATE_BEFORE_LOAD", "false").lower() == "true"
DESTINATION_TYPE = os.getenv("DESTINATION_TYPE", "SUPABASE").upper() # SUPABASE, DATABRICKS, S3

TIMESTAMP_CANDIDATES = [
    "updated_at", "updated_on", "modified_at", "modified_on",
    "last_update", "last_updated", "created_at", "created_on",
    "ts_update", "dt_update", "ultima_atualizacao", "data_atualizacao",
]

# ==============================================================================
# CLASSES DE ESTRATÉGIA (STRATEGY PATTERN)
# ==============================================================================

class DestinationStrategy(ABC):
    """Classe abstrata que define o contrato para qualquer destino de dados."""

    @abstractmethod
    def connect(self):
        """Estabelece conexão com o destino."""
        pass

    @abstractmethod
    def close(self):
        """Fecha conexão."""
        pass

    @abstractmethod
    def get_last_sync_info(self, table_name: str) -> Optional[datetime]:
        """Retorna a data da última carga para controle incremental."""
        pass

    @abstractmethod
    def prepare_table(self, table_name: str, schema: List[Tuple], pk_columns: List[str], fk_info: List[Tuple]) -> None:
        """Cria tabela ou valida estrutura no destino."""
        pass

    @abstractmethod
    def load_data(self, table_name: str, data_iterator, columns: List[str], ts_column: str) -> int:
        """Carrega os dados efetivamente."""
        pass


class SupabaseStrategy(DestinationStrategy):
    """Implementação para PostgreSQL / Supabase."""

    def __init__(self):
        self.host = os.getenv("DB_HOST")
        self.dbname = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.port = os.getenv("DB_PORT", "5432")
        self.sslmode = os.getenv("DB_SSLMODE")
        self.sslrootcert = os.getenv("DB_SSLROOTCERT")
        self.conn = None
        
        # Validação básica
        if not all([self.host, self.dbname, self.user]):
            raise ValueError("Credenciais do Supabase/Postgres incompletas no .env")

    def connect(self):
        params = {
            "host": self.host, "database": self.dbname, "user": self.user,
            "password": self.password, "port": self.port
        }
        if self.sslmode: params["sslmode"] = self.sslmode
        if self.sslrootcert: params["sslrootcert"] = self.sslrootcert
        
        self.conn = psycopg2.connect(**params)
        self._setup_control_table()

    def close(self):
        if self.conn:
            self.conn.close()

    def _setup_control_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS controle_cargas (
                    tabela_nome TEXT PRIMARY KEY,
                    ultima_carga TIMESTAMP,
                    linhas_carregadas INT
                );
            """)
            self.conn.commit()

    def get_last_sync_info(self, table_name: str) -> Optional[datetime]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT ultima_carga FROM controle_cargas WHERE tabela_nome = %s", (table_name,))
            row = cur.fetchone()
            return row[0] if row else None

    def prepare_table(self, table_name: str, schema: List[Tuple], pk_columns: List[str], fk_info: List[Tuple]) -> None:
        with self.conn.cursor() as cur:
            # 1. Verifica se tabela existe
            cur.execute(sql.SQL("SELECT to_regclass({})").format(sql.Literal(table_name)))
            exists = cur.fetchone()[0] is not None

            if not exists:
                logger.info(f"[Supabase] Criando tabela {table_name}")
                cols = []
                for col in schema:
                    pg_type = self._map_type(col[1])
                    cols.append(f'"{col[0]}" {pg_type}')
                
                if pk_columns:
                    cols.append(f"PRIMARY KEY ({', '.join([f'{c}' for c in pk_columns])})")
                
                create_sql = f"CREATE TABLE {table_name} ({', '.join(cols)});"
                cur.execute(create_sql)
                # Registra no controle
                cur.execute("INSERT INTO controle_cargas (tabela_nome, linhas_carregadas) VALUES (%s, 0)", (table_name,))
            
            # (Opcional) Aqui iria a lógica de verificar constraints/FKs existente no código original
            # Simplificado para manter o foco na estrutura
            self.conn.commit()

            if TRUNCATE_BEFORE_LOAD:
                cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;")
                self.conn.commit()

    def load_data(self, table_name: str, data_iterator, columns: List[str], ts_column: str) -> int:
        total_inserted = 0
        max_ts = None
        
        col_names = [f'"{c}"' for c in columns]
        query = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES %s ON CONFLICT DO NOTHING"

        with self.conn.cursor() as cur:
            while True:
                batch = data_iterator.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                
                execute_values(cur, query, batch, page_size=BATCH_SIZE)
                total_inserted += len(batch)

                # Calcular Max Timestamp do lote
                if ts_column:
                    try:
                        idx = columns.index(ts_column)
                        batch_max = max(row[idx] for row in batch if row[idx])
                        if max_ts is None or batch_max > max_ts:
                            max_ts = batch_max
                    except:
                        pass # Falha segura se timestamp não for comparável
        
            if total_inserted > 0:
                self._update_control(table_name, max_ts, total_inserted)
                self.conn.commit()
            
        return total_inserted

    def _update_control(self, table, last_ts, count):
        with self.conn.cursor() as cur:
            if last_ts:
                cur.execute(
                    "UPDATE controle_cargas SET ultima_carga = %s, linhas_carregadas = COALESCE(linhas_carregadas,0) + %s WHERE tabela_nome = %s",
                    (last_ts, count, table)
                )
            else:
                 cur.execute(
                    "UPDATE controle_cargas SET linhas_carregadas = COALESCE(linhas_carregadas,0) + %s WHERE tabela_nome = %s",
                    (count, table)
                )

    def _map_type(self, duck_type: str) -> str:
        t = duck_type.upper()
        if "TINYINT" in t: return "SMALLINT"
        if "BLOB" in t: return "BYTEA"
        if "DOUBLE" in t: return "DOUBLE PRECISION"
        return t


class DatabricksStrategy(DestinationStrategy):
    """
    Implementação para Databricks (LAKEHOUSE).
    Requer: pip install databricks-sql-connector
    """
    def __init__(self):
        try:
            from databricks import sql
            self.sql_module = sql
        except ImportError:
            raise ImportError("Instale 'databricks-sql-connector' para usar este destino.")
        
        self.hostname = os.getenv("DATABRICKS_HOST")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.conn = None

    def connect(self):
        self.conn = self.sql_module.connect(
            server_hostname=self.hostname,
            http_path=self.http_path,
            access_token=self.token
        )
        # Tabela de controle
        # Delta Table.
        self._setup_control_table()

    def _setup_control_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS controle_cargas (
                    tabela_nome STRING,
                    ultima_carga TIMESTAMP,
                    linhas_carregadas BIGINT
                ) USING DELTA;
            """)

    def close(self):
        if self.conn:
            self.conn.close()

    def get_last_sync_info(self, table_name: str) -> Optional[datetime]:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT max(ultima_carga) FROM controle_cargas WHERE tabela_nome = '{table_name}'")
            row = cur.fetchone()
            return row[0] if row else None

    def prepare_table(self, table_name: str, schema: List[Tuple], pk_columns: List[str], fk_info: List[Tuple]) -> None:
        with self.conn.cursor() as cur:
            cols = []
            for col in schema:
                cols.append(f"{col[0]} {col[1]}")
            
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)}) USING DELTA;"
            cur.execute(sql)

    def load_data(self, table_name: str, data_iterator, columns: List[str], ts_column: str) -> int:
        # Em produção use COPY INTO do S3.
        total = 0
        with self.conn.cursor() as cur:
            while True:
                batch = data_iterator.fetchmany(BATCH_SIZE)
                if not batch: break
                
                values_str = []
                for row in batch:
                    # Tratamento básico
                    row_fmt = [f"'{str(v)}'" if isinstance(v, (str, datetime)) else str(v) for v in row]
                    values_str.append(f"({', '.join(row_fmt)})")
                
                if values_str:
                    sql = f"INSERT INTO {table_name} VALUES {','.join(values_str)}"
                    cur.execute(sql)
                    total += len(batch)
        

        return total


class S3ParquetStrategy(DestinationStrategy):
    """
    Implementação para Data Lake (S3).
    Usa o poder do DuckDB para exportar direto para Parquet/S3 via HTTPFS.
    """
    def __init__(self, local_duckdb_conn):
        self.bucket = os.getenv("S3_BUCKET_NAME")
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.duck = local_duckdb_conn

        if not self.bucket:
            raise ValueError("S3_BUCKET_NAME não definido.")

    def connect(self):
        self.duck.execute("INSTALL httpfs; LOAD httpfs;")
        self.duck.execute(f"SET s3_region='{self.region}';")
        self.duck.execute(f"SET s3_access_key_id='{self.access_key}';")
        self.duck.execute(f"SET s3_secret_access_key='{self.secret_key}';")

    def close(self):
        pass

    def get_last_sync_info(self, table_name: str) -> Optional[datetime]:
        return None 

    def prepare_table(self, table_name: str, schema: List[Tuple], pk_columns: List[str], fk_info: List[Tuple]) -> None:
        pass

    def load_data(self, table_name: str, data_iterator, columns: List[str], ts_column: str) -> int:
        # Iteramos com DuckDB
        
        date_partition = datetime.now().strftime('%Y-%m-%d')
        s3_path = f"s3://{self.bucket}/raw/{table_name}/dt={date_partition}/data.parquet"
        
        logger.info(f"[S3] Exportando {table_name} diretamente para {s3_path}...")
        
        
        query = f"""
            COPY (SELECT * FROM {table_name}) 
            TO '{s3_path}' 
            (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE);
        """
        self.duck.execute(query)
        
        # Conta linhas copiadas (meta-dado do arquivo ou count simples)
        count = self.duck.execute(f"SELECT count(*) FROM read_parquet('{s3_path}')").fetchone()[0]
        logger.info(f"[S3] Exportação concluída: {count} registros.")
        return count



# PIPELINE PRINCIPAL


def get_duckdb_conn():
    full_path = (Path(DESTINATION_PATH) / DUCKDB_PATH).resolve()
    if not full_path.exists():
         raise FileNotFoundError(f"DB não encontrado: {full_path}")
    return duckdb.connect(str(full_path))

def get_strategy(local_conn) -> DestinationStrategy:
    """Factory que retorna a estratégia correta baseada no .env"""
    if DESTINATION_TYPE == "SUPABASE":
        return SupabaseStrategy()
    elif DESTINATION_TYPE == "DATABRICKS":
        return DatabricksStrategy()
    elif DESTINATION_TYPE == "S3":
        return S3ParquetStrategy(local_conn)
    else:
        raise ValueError(f"Tipo de destino desconhecido: {DESTINATION_TYPE}")

def main():
    start_time = datetime.now()
    logger.info(f"Iniciando Pipeline. Destino: {DESTINATION_TYPE}")

    # 1. Conexões
    duck_conn = get_duckdb_conn()
    
    try:
        destination = get_strategy(duck_conn)
        destination.connect()
    except Exception as e:
        logger.error(f"Erro fatal na conexão com destino: {e}")
        return

    try:
        tables = [t[0] for t in duck_conn.execute("SHOW TABLES").fetchall()]
        if "table_metadata" in tables: tables.remove("table_metadata")
        if "fk_metadata" in tables: tables.remove("fk_metadata")
        
        # 3. Loop de Carga
        for table in tables:
            try:
                schema = duck_conn.execute(f"DESCRIBE {table}").fetchall()
                cols = [c[0] for c in schema]
                
                pk_info = duck_conn.execute(f"PRAGMA table_info('{table}')").fetchall()
                pks = [r[1] for r in pk_info if r[5]]
                
                destination.prepare_table(table, schema, pks, [])
                
                last_sync = destination.get_last_sync_info(table)
                ts_col = next((c for c in cols if c in TIMESTAMP_CANDIDATES), None)
                
                query = f"SELECT * FROM {table}"
                params = []
                
                if ts_col and last_sync and not TRUNCATE_BEFORE_LOAD:
                    logger.info(f"[{table}] Modo Incremental. Novos dados desde {last_sync}")
                    query += f" WHERE {ts_col} > ?"
                    params.append(last_sync)
                else:
                    logger.info(f"[{table}] Modo Full Load")
                
                # Executar Carga
                cursor = duck_conn.execute(query, params)
                
                count = destination.load_data(table, cursor, cols, ts_col)
                logger.info(f"[{table}] Sucesso. {count} linhas sincronizadas.")

            except Exception as e:
                logger.error(f"Erro ao processar tabela {table}: {e}")

    finally:
        destination.close()
        duck_conn.close()
        logger.info(f"Pipeline finalizado em {(datetime.now() - start_time).total_seconds():.2f}s")

if __name__ == "__main__":
    main()
