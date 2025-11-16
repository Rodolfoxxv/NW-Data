"""Utilitário para gerar um arquivo DuckDB com dados de exemplo e metadados."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict

import duckdb
from dotenv import load_dotenv

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "database.duckdb")
DESTINATION_PATH = os.getenv("DESTINATION_PATH", "./data")

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SAMPLE_TABLES: Dict[str, str] = {
    "clientes": (
        """
        CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY,
            nome VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    ),
    "pedidos": (
        """
        CREATE TABLE IF NOT EXISTS pedidos (
            id INTEGER PRIMARY KEY,
            cliente_id INTEGER,
            valor_total DECIMAL(10,2),
            status VARCHAR,
            updated_at TIMESTAMP,
            FOREIGN KEY(cliente_id) REFERENCES clientes(id)
        );
        """
    ),
    "itens_pedido": (
        """
        CREATE TABLE IF NOT EXISTS itens_pedido (
            id INTEGER PRIMARY KEY,
            pedido_id INTEGER,
            produto VARCHAR,
            quantidade INTEGER,
            valor_unit DECIMAL(10,2),
            FOREIGN KEY(pedido_id) REFERENCES pedidos(id)
        );
        """
    ),
    "pagamentos": (
        """
        CREATE TABLE IF NOT EXISTS pagamentos (
            id INTEGER PRIMARY KEY,
            pedido_id INTEGER,
            valor_pago DECIMAL(10,2),
            metodo VARCHAR,
            pago_em TIMESTAMP,
            FOREIGN KEY(pedido_id) REFERENCES pedidos(id)
        );
        """
    ),
}

SAMPLE_DATA = {
    "clientes": [
        (1, "Ana", "ana@example.com", "2024-10-01 10:00:00"),
        (2, "Bruno", "bruno@example.com", "2024-10-05 11:30:00"),
    ],
    "pedidos": [
        (1, 1, 150.50, "pago", "2024-10-06 08:00:00"),
        (2, 2, 320.00, "pendente", "2024-10-07 09:15:00"),
    ],
    "itens_pedido": [
        (1, 1, "Notebook", 1, 150.50),
        (2, 2, "Monitor", 2, 160.00),
    ],
    "pagamentos": [
        (1, 1, 150.50, "cartao", "2024-10-06 09:00:00"),
        (2, 2, 160.00, "pix", "2024-10-07 10:20:00"),
    ],
}


def ensure_path() -> Path:
    path = Path(DESTINATION_PATH).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path / DUCKDB_PATH


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for ddl in SAMPLE_TABLES.values():
        conn.execute(ddl)
    logger.info("Tabelas de exemplo criadas.")


def seed_data(conn: duckdb.DuckDBPyConnection) -> None:
    for table in reversed(list(SAMPLE_DATA.keys())):
        conn.execute(f"DELETE FROM {table}")
    for table, rows in SAMPLE_DATA.items():
        if not rows:
            continue
        placeholders = ", ".join(["?"] * len(rows[0]))
        conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
    logger.info("Dados de exemplo inseridos.")


def ensure_metadata_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS table_metadata (
            table_name VARCHAR PRIMARY KEY,
            schema_json JSON
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fk_metadata (
            table_name VARCHAR,
            column_name VARCHAR,
            ref_table VARCHAR,
            ref_column VARCHAR,
            PRIMARY KEY (table_name, column_name)
        );
        """
    )

    tables = conn.execute("SHOW TABLES").fetchall()
    for (table,) in tables:
        if table in ("table_metadata", "fk_metadata"):
            continue
        columns_info = conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        metadata: Dict[str, Dict[str, object]] = {}
        for col in columns_info:
            metadata[col[1]] = {
                "data_type": col[2],
                "primary_key": bool(col[5]),
                "foreign_key": None,
            }
        try:
            fk_info = conn.execute(f"PRAGMA foreign_key_list('{table}')").fetchall()
        except Exception:
            fk_info = []
        for fk in fk_info:
            fk_column = fk[3]
            ref_table = fk[2]
            ref_column = fk[4]
            metadata[fk_column]["foreign_key"] = {"table": ref_table, "column": ref_column}
            conn.execute(
                """
                INSERT INTO fk_metadata (table_name, column_name, ref_table, ref_column)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (table_name, column_name) DO UPDATE SET
                    ref_table = excluded.ref_table,
                    ref_column = excluded.ref_column;
                """,
                (table, fk_column, ref_table, ref_column),
            )
        conn.execute(
            """
            INSERT INTO table_metadata (table_name, schema_json)
            VALUES (?, ?)
            ON CONFLICT (table_name) DO UPDATE SET schema_json = excluded.schema_json;
            """,
            (table, json.dumps(metadata)),
        )
    logger.info("Metadados sincronizados.")


def main() -> None:
    database_path = ensure_path()
    logger.info("Gerando DuckDB em %s", database_path)
    conn = duckdb.connect(str(database_path))
    try:
        create_tables(conn)
        seed_data(conn)
        ensure_metadata_tables(conn)
    finally:
        conn.close()
    logger.info("Arquivo DuckDB pronto para uso.")


if __name__ == "__main__":
    main()
