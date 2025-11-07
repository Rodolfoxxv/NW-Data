import streamlit as st
import pandas as pd
import plotly.express as px
import json
import duckdb
import os
import logging
import re
from pathlib import Path
from dotenv import load_dotenv

# ===== Utilitários de Validação =====
def sanitize_identifier(name: str) -> str:
    """
    Valida e sanitiza identificadores (nomes de tabelas/colunas).
    Aceita apenas caracteres alfanuméricos e underscores.
    """
    if not name or not isinstance(name, str):
        raise ValueError("Identificador inválido: deve ser uma string não vazia.")
    
    # Remove espaços em branco
    name = name.strip()
    
    # Verifica se contém apenas caracteres válidos
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
        raise ValueError(
            f"Identificador inválido '{name}': deve começar com letra ou underscore "
            "e conter apenas letras, números e underscores."
        )
    
    # Previne palavras reservadas do SQL
    reserved_words = {
        'select', 'insert', 'update', 'delete', 'drop', 'create', 
        'alter', 'table', 'from', 'where', 'union', 'exec', 'execute'
    }
    if name.lower() in reserved_words:
        raise ValueError(f"Identificador '{name}' é uma palavra reservada do SQL.")
    
    return name

def validate_data_type(data_type: str) -> str:
    """
    Valida tipos de dados permitidos no DuckDB.
    """
    allowed_types = {
        'VARCHAR', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT',
        'DOUBLE', 'FLOAT', 'DECIMAL', 'BOOLEAN', 'DATE', 
        'TIMESTAMP', 'TIME', 'BLOB', 'TEXT'
    }
    
    data_type = data_type.strip().upper()
    base_type = data_type.split('(')[0]  # Remove parâmetros como VARCHAR(50)
    
    if base_type not in allowed_types:
        raise ValueError(f"Tipo de dado '{data_type}' não permitido.")
    
    return data_type

# ===== Configuração do Ambiente e Logging =====
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DESTINATION_PATH = os.getenv("DESTINATION_PATH")

if not all([DUCKDB_PATH, DESTINATION_PATH]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")

destination_path_obj = Path(DESTINATION_PATH)
full_duckdb_path = (destination_path_obj / DUCKDB_PATH).resolve()
if not full_duckdb_path.is_file():
    raise FileNotFoundError(f"Arquivo DuckDB não encontrado: {full_duckdb_path}")

logging.info(f"DUCKDB_PATH: {full_duckdb_path}")

# ===== Classe da Pipeline DuckDB =====
class DuckDBPipeline:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.create_metadata_tables()
        self.sync_metadata_with_existing_tables()

    def create_metadata_tables(self):
        
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS table_metadata (
                table_name VARCHAR PRIMARY KEY,
                schema_json JSON
            );
            """
        )

        self.conn.execute(
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

    def sync_metadata_with_existing_tables(self):
        tables = self.conn.execute("SHOW TABLES").fetchall()
        for (table,) in tables:
            if table not in ("table_metadata", "fk_metadata"):
                exists = self.conn.execute(
                    "SELECT 1 FROM table_metadata WHERE table_name = ?",
                    (table,)
                ).fetchone()
                if not exists:
                    self.register_table_metadata(table)

    def register_table_metadata(self, table_name: str):
        # Valida o nome da tabela
        table_name = sanitize_identifier(table_name)
        
        # Obter estrutura da tabela
        columns_info = self.conn.execute(
            f"PRAGMA table_info('{table_name}')"
        ).fetchall()
        fk_info = self.conn.execute(
            f"PRAGMA foreign_key_list('{table_name}')"
        ).fetchall()

        metadata = {}
        pk_found = False


        for col in columns_info:
            col_name = col[1]
            data_type = col[2]
            is_pk = bool(col[5])
            if is_pk:
                pk_found = True
            metadata[col_name] = {
                "data_type": data_type,
                "primary_key": is_pk,
                "foreign_key": None  # inicializa sem FK
            }
        if not pk_found:
            logging.warning(f"Tabela '{table_name}' não possui chave primária.")
            # Garante que todas as colunas sejam marcadas como não PK
            for col in metadata:
                metadata[col]["primary_key"] = False

        # Processa informações de FK, se houver
        if fk_info:
            for fk in fk_info:
                # PRAGMA foreign_key_list retorna: id, seq, table, from, to, on_update, on_delete, match
                fk_column = fk[3]
                ref_table = fk[2]
                ref_column = fk[4]
                metadata[fk_column]["foreign_key"] = {"table": ref_table, "column": ref_column}
                self.conn.execute(
                    """
                    INSERT INTO fk_metadata (table_name, column_name, ref_table, ref_column)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING;
                    """,
                    (table_name, fk_column, ref_table, ref_column)
                )
        # Se não houver FK, elas já estão registradas como None

        # Insere ou atualiza os metadados na tabela table_metadata
        self.conn.execute(
            """
            INSERT INTO table_metadata (table_name, schema_json)
            VALUES (?, ?)
            ON CONFLICT (table_name) DO UPDATE SET schema_json = excluded.schema_json;
            """,
            (table_name, json.dumps(metadata))
        )
        logging.info(f"Metadados da tabela '{table_name}' sincronizados.")

    def create_table_dynamic(self, table_name: str, fields: list):
        # Valida o nome da tabela
        try:
            table_name = sanitize_identifier(table_name)
        except ValueError as e:
            raise ValueError(f"Nome de tabela inválido: {e}")
        
        # Verifica se a tabela já está registrada
        exists = self.conn.execute(
            "SELECT 1 FROM table_metadata WHERE table_name = ?",
            (table_name,)
        ).fetchone()
        if exists:
            raise ValueError(f"A tabela '{table_name}' já existe na pipeline.")

        column_defs = []
        metadata = {}
        pk_columns = []

        for field in fields:
            col_name = field.get("name")
            if not col_name:
                raise ValueError("Cada campo deve ter um 'name' definido.")
            
            # Valida o nome da coluna
            try:
                col_name = sanitize_identifier(col_name)
            except ValueError as e:
                raise ValueError(f"Nome de coluna inválido '{col_name}': {e}")
            
            data_type = field.get("data_type", "VARCHAR").strip() or "VARCHAR"
            
            # Valida o tipo de dado
            try:
                data_type = validate_data_type(data_type)
            except ValueError as e:
                raise ValueError(f"Tipo de dado inválido para coluna '{col_name}': {e}")
            col_def = f"{col_name} {data_type}"
            is_pk = field.get("primary_key", False)
            if is_pk:
                pk_columns.append(col_name)
            metadata[col_name] = {
                "data_type": data_type,
                "primary_key": is_pk,
                "foreign_key": None
            }

            # Verifica e registra FK, se houver
            fk = field.get("foreign_key")
            if fk:
                fk_table = fk.get("table")
                fk_column = fk.get("column")
                if fk_table and fk_column:
                    # Valida identificadores de FK
                    try:
                        fk_table = sanitize_identifier(fk_table)
                        fk_column = sanitize_identifier(fk_column)
                    except ValueError as e:
                        raise ValueError(f"Identificador de FK inválido: {e}")
                    
                    # Verifica se a tabela referenciada já existe nos metadados
                    ref_exists = self.conn.execute(
                        "SELECT 1 FROM table_metadata WHERE table_name = ?",
                        (fk_table,)
                    ).fetchone()
                    if not ref_exists:
                        raise ValueError(f"Tabela referenciada '{fk_table}' para FK da coluna '{col_name}' não existe.")
                    col_def += f" REFERENCES {fk_table}({fk_column})"
                    metadata[col_name]["foreign_key"] = {"table": fk_table, "column": fk_column}
                else:
                    raise ValueError(f"Chave estrangeira inválida para a coluna '{col_name}'")
            column_defs.append(col_def)

        # Adiciona restrição de PK se existir
        if pk_columns:
            if len(pk_columns) > 1:
                column_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")
            # Se somente uma PK, ela já pode estar definida na coluna
        else:
            logging.warning(f"Tabela '{table_name}' não possui chave primária.")
            for col in metadata:
                metadata[col]["primary_key"] = False

        sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)});"
        self.conn.execute(sql)
        logging.info(f"Tabela '{table_name}' criada com sucesso.")

        # Registra metadados na tabela table_metadata
        self.conn.execute(
            """
            INSERT INTO table_metadata (table_name, schema_json)
            VALUES (?, ?);
            """,
            (table_name, json.dumps(metadata))
        )
        # Registra metadados de FK, se houver
        for field in fields:
            fk = field.get("foreign_key")
            if fk:
                fk_table = fk.get("table")
                fk_column = fk.get("column")
                self.conn.execute(
                    """
                    INSERT INTO fk_metadata (table_name, column_name, ref_table, ref_column)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT DO NOTHING;
                    """,
                    (table_name, field.get("name"), fk_table, fk_column)
                )

    def get_table_metadata(self, table_name: str):
        try:
            row = self.conn.execute(
                "SELECT schema_json FROM table_metadata WHERE table_name = ?",
                (table_name,)
            ).fetchone()
            return json.loads(row[0]) if row else {}
        except Exception as e:
            st.error(f"Erro ao obter metadados: {str(e)}")
            return {}

    def insert_data(self, table_name: str, data: dict):
        metadata = self.get_table_metadata(table_name)
        # Verifica integridade das FKs
        for col, info in metadata.items():
            fk = info.get("foreign_key")
            if fk and col in data and data[col] != "":
                ref_table = fk.get("table")
                ref_column = fk.get("column")
                value = data[col]
                exists = self.conn.execute(
                    f"SELECT 1 FROM {ref_table} WHERE {ref_column} = ?",
                    (value,)
                ).fetchone()
                if not exists:
                    st.error(f"Valor '{value}' para a coluna '{col}' não existe na tabela referenciada '{ref_table}'.")
                    return False

        # Verifica integridade da chave primária
        primary_keys = [col for col, info in metadata.items() if info.get("primary_key")]
        if primary_keys:
            missing = [pk for pk in primary_keys if not data.get(pk)]
            if missing:
                st.error(f"Valores faltando para chave(s) primária(s): {', '.join(missing)}")
                return False
            conditions = " AND ".join([f"{pk} = ?" for pk in primary_keys])
            values = tuple(data[pk] for pk in primary_keys)
            exists = self.conn.execute(
                f"SELECT 1 FROM {table_name} WHERE {conditions}",
                values
            ).fetchone()
            if exists:
                st.error(f"Registro com chave primária {dict(zip(primary_keys, values))} já existe.")
                return False

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        try:
            self.conn.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                list(data.values())
            )
            self.conn.commit()
            st.success("Dados inseridos com sucesso!")
            return True
        except duckdb.Error as e:
            st.error(f"Erro de banco de dados: {str(e)}")
            self.conn.rollback()
            return False
        except Exception as e:
            st.error(f"Erro inesperado: {str(e)}")
            self.conn.rollback()
            return False

    def update_data(self, table_name: str, set_data: dict, where_clause: str, where_params: tuple):
        try:
            set_str = ", ".join([f"{col} = ?" for col in set_data.keys()])
            sql = f"UPDATE {table_name} SET {set_str} WHERE {where_clause};"
            values = tuple(set_data.values()) + where_params
            self.conn.execute(sql, values)
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Erro na atualização: {str(e)}")
            self.conn.rollback()
            return False

    def delete_data(self, table_name: str, where_clause: str, where_params: tuple):
        try:
            sql = f"DELETE FROM {table_name} WHERE {where_clause};"
            self.conn.execute(sql, where_params)
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Erro na deleção: {str(e)}")
            self.conn.rollback()
            return False

    def delete_table(self, table_name: str):
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.conn.execute("DELETE FROM table_metadata WHERE table_name = ?;", (table_name,))
            self.conn.execute("DELETE FROM fk_metadata WHERE table_name = ?;", (table_name,))
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Erro ao dropar tabela: {str(e)}")
            self.conn.rollback()
            return False

    def list_tables(self):
        try:
            rows = self.conn.execute("SELECT table_name FROM table_metadata;").fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            st.error(f"Erro ao listar tabelas: {str(e)}")
            return []

    def get_table_data(self, table_name: str):
        try:
            return self.conn.execute(f"SELECT * FROM {table_name};").fetchdf()
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame()

    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            st.error(f"Erro ao fechar conexão: {str(e)}")

# ===== Instanciação da Pipeline =====
pipeline = DuckDBPipeline(str(full_duckdb_path))

# ===== Interface Streamlit =====
st.title("📊 Interface DuckDB")

# Menu lateral
operation = st.sidebar.selectbox(
    "Operação",
    ("Criar Tabela", "Inserir Dados", "Atualizar Dados", "Deletar Dados", 
     "Dropar Tabela", "Limpar Tabela", "Visualizar Tabela", "Listar Tabelas"),
    index=0
)

# ----- CRIAR TABELA -----
if operation == "Criar Tabela":
    st.header("🆕 Criar Nova Tabela")
    create_mode = st.radio("Modo de Criação", ("Formulário Manual", "Colar JSON"), horizontal=True)
    table_name = st.text_input("Nome da Tabela", key="create_table_name")
    
    if create_mode == "Formulário Manual":
        st.subheader("Definição de Campos")
        if "fields" not in st.session_state:
            st.session_state.fields = []

        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("➕ Adicionar Campo"):
                st.session_state.fields.append({
                    "name": "",
                    "data_type": "VARCHAR",
                    "primary_key": False,
                    "foreign_key_table": "",
                    "foreign_key_column": ""
                })

        for idx, field in enumerate(st.session_state.fields):
            st.markdown(f"---\n**Campo #{idx+1}**")
            cols = st.columns([3, 2, 1, 3, 3])
            with cols[0]:
                field["name"] = st.text_input("Nome", key=f"name_{idx}", value=field["name"])
            with cols[1]:
                field["data_type"] = st.selectbox(
                    "Tipo",
                    ["VARCHAR", "INTEGER", "BOOLEAN", "DATE", "TIMESTAMP", "FLOAT", "BLOB"],
                    key=f"type_{idx}",
                    index=0
                )
            with cols[2]:
                field["primary_key"] = st.checkbox("PK", key=f"pk_{idx}", value=field["primary_key"])
            with cols[3]:
                field["foreign_key_table"] = st.text_input("Tabela FK", key=f"fk_table_{idx}", value=field["foreign_key_table"])
            with cols[4]:
                field["foreign_key_column"] = st.text_input("Coluna FK", key=f"fk_col_{idx}", value=field["foreign_key_column"])

        if st.button("✅ Criar Tabela", type="primary"):
            try:
                fields = []
                for field in st.session_state.fields:
                    f = {
                        "name": field["name"],
                        "data_type": field["data_type"],
                        "primary_key": field["primary_key"]
                    }
                    if field["foreign_key_table"] and field["foreign_key_column"]:
                        f["foreign_key"] = {
                            "table": field["foreign_key_table"],
                            "column": field["foreign_key_column"]
                        }
                    fields.append(f)
                pipeline.create_table_dynamic(table_name, fields)
                st.success(f"Tabela '{table_name}' criada com sucesso!")
                st.session_state.fields = []
            except Exception as e:
                st.error(f"Erro: {str(e)}")
    
    else:
        st.subheader("Definição por JSON")
        json_schema = st.text_area(
            "Cole o schema JSON (formato lista de objetos)",
            height=200,
            help='Exemplo: [{"name": "id", "data_type": "INTEGER", "primary_key": true}]'
        )
        if st.button("✅ Criar via JSON", type="primary"):
            try:
                fields = json.loads(json_schema)
                pipeline.create_table_dynamic(table_name, fields)
                st.success(f"Tabela '{table_name}' criada com sucesso!")
            except Exception as e:
                st.error(f"Erro: {str(e)}")

# ----- INSERIR DADOS -----
elif operation == "Inserir Dados":
    st.header("📥 Inserir Dados")
    tables = pipeline.list_tables()
    
    if not tables:
        st.warning("Nenhuma tabela encontrada no banco de dados.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables)
        metadata = pipeline.get_table_metadata(table_name)
        
        if metadata:
            with st.form("insert_form"):
                st.subheader("Valores para Inserção")
                data = {}
                for col, info in metadata.items():
                    data[col] = st.text_input(
                        label=f"{col} ({info['data_type']})" + 
                              (" 🔑" if info['primary_key'] else "") +
                              (" 🌐" if info['foreign_key'] else ""),
                        help=f"Tipo: {info['data_type']}" +
                             (" | Chave Primária" if info['primary_key'] else "") +
                             (f" | Chave Estrangeira: {info['foreign_key']['table']}.{info['foreign_key']['column']}" 
                              if info['foreign_key'] else "")
                    )
                if st.form_submit_button("🚀 Inserir Dados"):
                    result = pipeline.insert_data(table_name, data)
                    if result:
                        st.rerun()

# ----- ATUALIZAR DADOS -----
elif operation == "Atualizar Dados":
    st.header("🔄 Atualizar Dados")
    tables = pipeline.list_tables()
    
    if not tables:
        st.warning("Nenhuma tabela encontrada no banco de dados.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables, key="update_table")
        metadata = pipeline.get_table_metadata(table_name)
        
        if metadata:
            with st.form("update_form"):
                st.subheader("Novos Valores")
                set_data = {}
                for col in metadata.keys():
                    set_data[col] = st.text_input(f"Novo valor para {col}")
                
                st.subheader("Condição WHERE")
                where_clause = st.text_input("Exemplo: id = ?", help="Use ? para parâmetros")
                where_values = st.text_input("Valores (separados por vírgula)")
                
                if st.form_submit_button("💾 Salvar Alterações"):
                    try:
                        where_params = tuple([v.strip() for v in where_values.split(",")] if where_values else [])
                        success = pipeline.update_data(
                            table_name,
                            {k: v for k, v in set_data.items() if v},
                            where_clause,
                            where_params
                        )
                        if success:
                            st.rerun()
                    except Exception as e:
                        st.error(f"Erro na atualização: {str(e)}")

# ----- DELETAR DADOS -----
elif operation == "Deletar Dados":
    st.header("Deletar Dados")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables, key="delete_table")
        with st.form("delete_form"):
            where_clause = st.text_input("Cláusula WHERE (ex.: id = ?)", key="del_where")
            where_value = st.text_input("Valor para a cláusula WHERE", key="del_where_value")
            submitted = st.form_submit_button("Deletar")
            if submitted:
                if where_clause == '' or where_value == '':
                    st.error("Preencha a condição de deleção.")
                else:
                    pipeline.delete_data(table_name, where_clause, (where_value,))
                    st.success("Dados deletados com sucesso!")

# ----- DROPAR TABELA -----
elif operation == "Dropar Tabela":
    st.header("Dropar Tabela")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela para dropar", tables, key="drop_table")
        if st.button("Dropar Tabela"):
            try:
                pipeline.delete_table(table_name)
                st.success(f"Tabela '{table_name}' removida com sucesso!")
            except Exception as e:
                st.error(f"Erro: {e}")

# ----- LIMPAR TABELA (TRUNCATE) -----
elif operation == "Limpar Tabela":
    st.header("Limpar Tabela (TRUNCATE)")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela para limpar", tables, key="truncate_table")
        if st.button("Limpar Tabela"):
            try:
                pipeline.conn.execute(f"TRUNCATE TABLE {table_name};")
                pipeline.conn.commit()
                st.success(f"Tabela '{table_name}' limpa com sucesso!")
            except Exception as e:
                st.error(f"Erro ao limpar a tabela: {str(e)}")

# ----- VISUALIZAR TABELA -----
elif operation == "Visualizar Tabela":
    st.header("Visualizar Dados da Tabela")
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
    else:
        table_name = st.selectbox("Selecione a Tabela", tables, key="view_table")
        try:
            df = pipeline.get_table_data(table_name)
            st.dataframe(df)
            num_cols = df.select_dtypes(include='number').columns.tolist()
            cat_cols = df.select_dtypes(include='object').columns.tolist()
            if num_cols and cat_cols:
                st.subheader("Gráfico de Dispersão (Exemplo)")
                fig = px.scatter(df, x=cat_cols[0], y=num_cols[0], title=f"{table_name} - {cat_cols[0]} vs {num_cols[0]}")
                st.plotly_chart(fig)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")

# ----- LISTAR TABELAS -----
elif operation == "Listar Tabelas":
    st.header("Listar Tabelas")
    tables = pipeline.list_tables()
    st.write(tables)

st.sidebar.info("Interface integrada com a pipeline do DuckDB.")
