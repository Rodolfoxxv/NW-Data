import importlib
import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("DUCKDB_PATH", "database.duckdb")
os.environ.setdefault("DESTINATION_PATH", "./data")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_NAME", "postgres")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "postgres")

loader = importlib.import_module("scripts.incremental_loader")


def test_mapear_tipo_duckdb_para_postgres_type():
    cases = {
        "tinyint": "SMALLINT",
        "blob": "BYTEA",
        "double": "DOUBLE PRECISION",
        "decimal(10,2)": "NUMERIC(10,2)",
        "varchar": "VARCHAR",
    }
    for source, expected in cases.items():
        assert loader.mapear_tipo_duckdb_para_postgres_type(source) == expected


def test_ordenar_tabelas_topologicamente_respeita_dependencias():
    tabelas = ["clientes", "pedidos", "itens_pedido", "pagamentos"]
    metadata = {
        "clientes": json.dumps({}),
        "pedidos": json.dumps({
            "cliente_id": {"foreign_key": {"table": "clientes", "column": "id"}}
        }),
        "itens_pedido": json.dumps({
            "pedido_id": {"foreign_key": {"table": "pedidos", "column": "id"}}
        }),
        "pagamentos": json.dumps({
            "pedido_id": {"foreign_key": {"table": "pedidos", "column": "id"}}
        }),
    }
    ordenadas = loader.ordenar_tabelas_topologicamente(tabelas, metadata)
    assert ordenadas.index("clientes") < ordenadas.index("pedidos")
    assert ordenadas.index("pedidos") < ordenadas.index("itens_pedido")
    assert ordenadas.index("pedidos") < ordenadas.index("pagamentos")


def test_ordenar_tabelas_topologicamente_com_ciclo_retorna_lista_original():
    tabelas = ["a", "b"]
    metadata = {
        "a": json.dumps({"b_id": {"foreign_key": {"table": "b", "column": "id"}}}),
        "b": json.dumps({"a_id": {"foreign_key": {"table": "a", "column": "id"}}}),
    }
    ordenadas = loader.ordenar_tabelas_topologicamente(tabelas, metadata)
    assert set(ordenadas) == set(tabelas)
