import streamlit as st
import pandas as pd
import plotly.express as px
import json
import duckdb
import os
from pathlib import Path
from dotenv import load_dotenv

# ========= Configuração do Ambiente =========
load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
if DUCKDB_PATH is None:
    st.error("DUCKDB_PATH não está definido no arquivo .env")
    st.stop()

full_duckdb_path = Path('D:/Projetos/NW-Data/data') / DUCKDB_PATH
full_duckdb_path = full_duckdb_path.resolve()

# ========= Pipeline do DuckDB =========
class DuckDBPipeline:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect(db_path)
        self.create_metadata_table()

    def create_metadata_table(self):
        """Cria a tabela de metadados usando table_name como PRIMARY KEY."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS table_metadata (
                table_name VARCHAR PRIMARY KEY,
                schema_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def create_table_dynamic(self, table_name: str, fields: list):
        """Cria tabelas dinamicamente com base em um schema fornecido."""
        existing = self.conn.execute(
            "SELECT table_name FROM table_metadata WHERE table_name = ?;",
            (table_name,)
        ).fetchone()
        if existing:
            raise ValueError(f"A tabela '{table_name}' já existe na pipeline.")

        column_defs = []
        schema_for_metadata = {}
        pk_columns = []

        for field in fields:
            col_name = field.get("name")
            if not col_name:
                raise ValueError("Cada campo deve ter um 'name' definido.")
           
            data_type = field.get("data_type", "").strip() or "VARCHAR"
            col_def = f"{col_name} {data_type}"
            schema_for_metadata[col_name] = {
                "data_type": data_type,
                "primary_key": False,
                "foreign_key": None
            }

            if field.get("primary_key", False):
                pk_columns.append(col_name)
                schema_for_metadata[col_name]["primary_key"] = True

            fk = field.get("foreign_key")
            if fk:
                fk_table, fk_column = fk.get("table"), fk.get("column")
                if fk_table and fk_column:
                    col_def += f" REFERENCES {fk_table}({fk_column})"
                    schema_for_metadata[col_name]["foreign_key"] = {
                        "table": fk_table,
                        "column": fk_column
                    }
                else:
                    raise ValueError(f"Chave estrangeira inválida para a coluna '{col_name}'")

            column_defs.append(col_def)

        # Adicionar constraints para chaves primárias
        if pk_columns:
            if len(pk_columns) > 1:
                column_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")
            else:
                column_defs.append(f"PRIMARY KEY ({pk_columns[0]})")

        sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)});"
        self.conn.execute(sql)
        self.conn.execute(
            "INSERT INTO table_metadata (table_name, schema_json) VALUES (?, ?);",
            (table_name, json.dumps(schema_for_metadata))
        )

    def insert_data(self, table_name: str, data: dict):
        """Insere dados na tabela garantindo que a chave primária não seja duplicada."""
        metadata = self.get_table_metadata(table_name)
        primary_keys = [col for col, info in metadata.items() if info.get("primary_key")]

        try:
            if primary_keys:
                # Verificar chaves primárias ausentes
                missing_pks = [pk for pk in primary_keys if pk not in data]
                if missing_pks:
                    st.error(f"Valores faltando para chave(s) primária(s): {', '.join(missing_pks)}")
                    return False

                # Gerar ID automático se for chave única 'id'
                if len(primary_keys) == 1 and primary_keys[0] == "id" and not data.get("id"):
                    max_id = self.conn.execute(
                        f"SELECT COALESCE(MAX(id), 0) FROM {table_name};"
                    ).fetchone()[0]
                    data["id"] = max_id + 1

                # Verificar duplicatas
                pk_conditions = " AND ".join([f"{pk} = ?" for pk in primary_keys])
                pk_values = tuple(data[pk] for pk in primary_keys)
                exists = self.conn.execute(
                    f"SELECT 1 FROM {table_name} WHERE {pk_conditions};",
                    pk_values
                ).fetchone()
                
                if exists:
                    st.error(f"Registro já existe: {dict(zip(primary_keys, pk_values))}")
                    return False

            # Inserir dados
            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
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
        """Atualiza dados na tabela com verificação de integridade."""
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
        """Remove dados da tabela com verificação de segurança."""
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
        """Remove tabela e seus metadados com tratamento de erros."""
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.conn.execute("DELETE FROM table_metadata WHERE table_name = ?;", (table_name,))
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Erro ao dropar tabela: {str(e)}")
            self.conn.rollback()
            return False

    def get_table_metadata(self, table_name: str):
        """Obtém metadados da tabela com tratamento para tabelas inexistentes."""
        try:
            row = self.conn.execute(
                "SELECT schema_json FROM table_metadata WHERE table_name = ?;",
                (table_name,)
            ).fetchone()
            return json.loads(row[0]) if row else {}
        except Exception as e:
            st.error(f"Erro ao obter metadados: {str(e)}")
            return {}

    def list_tables(self):
        """Lista todas as tabelas com tratamento de erros."""
        try:
            rows = self.conn.execute("SELECT table_name FROM table_metadata;").fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            st.error(f"Erro ao listar tabelas: {str(e)}")
            return []

    def get_table_data(self, table_name: str):
        """Obtém dados da tabela com tratamento de erros."""
        try:
            return self.conn.execute(f"SELECT * FROM {table_name};").fetchdf()
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame()

    def close(self):
        """Fecha a conexão com segurança."""
        try:
            self.conn.close()
        except Exception as e:
            st.error(f"Erro ao fechar conexão: {str(e)}")


pipeline = DuckDBPipeline(str(full_duckdb_path))

# ========= Interface Streamlit =========
st.title("📊 Interface de Gerenciamento de Dados (DuckDB)")

# Menu lateral
operation = st.sidebar.selectbox(
    "Operação",
    ("Criar Tabela", "Inserir Dados", "Atualizar Dados", "Deletar Dados", 
     "Dropar Tabela", "Visualizar Tabela", "Listar Tabelas"),
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
                    "data_type": "",
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
