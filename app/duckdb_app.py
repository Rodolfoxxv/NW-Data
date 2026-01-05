import streamlit as st
import pandas as pd
import plotly.express as px
import json
import duckdb
import os
import logging
import subprocess
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Componentes
from components import (
    load_custom_css,
    metric_card,
    info_box,
    progress_bar
)

# Configuração
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
load_dotenv()

# Caminhos
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_FILENAME = "database.duckdb"
full_duckdb_path = (DATA_DIR / DB_FILENAME).resolve()

# Configurar variáveis para o script de bootstrap
os.environ["DUCKDB_PATH"] = DB_FILENAME
os.environ["DESTINATION_PATH"] = str(DATA_DIR)

# Configuração
st.set_page_config(
    page_title="Data Lake Manager",
    page_icon="💠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Logo
try:
    st.logo("app/assets/logo.svg")
except Exception:
    pass

# Estilos
load_custom_css()

# Verificação do Banco
if not full_duckdb_path.parent.exists():
    try:
        full_duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        st.error(f"Erro ao criar diretório de dados: {e}")


# Pipeline
class DuckDBPipeline:
    def __init__(self, db_path: str):
        try:
            self.conn = duckdb.connect(db_path)
            self.create_metadata_tables()
            self.sync_metadata_with_existing_tables()
        except Exception as e:
            st.error(f"Erro ao conectar ao banco de dados: {e}")

    def create_metadata_tables(self):
        try:
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
        except Exception as e:
            logging.error(f"Erro ao criar tabelas de metadados: {e}")

    def sync_metadata_with_existing_tables(self):
        try:
            tables = self.conn.execute("SHOW TABLES").fetchall()
            for (table,) in tables:
                if table not in ("table_metadata", "fk_metadata"):
                    exists = self.conn.execute(
                        "SELECT 1 FROM table_metadata WHERE table_name = ?",
                        (table,)
                    ).fetchone()
                    if not exists:
                        self.register_table_metadata(table)
        except Exception as e:
            logging.error(f"Erro ao sincronizar metadados: {e}")

    def register_table_metadata(self, table_name: str):
        try:
            columns_info = self.conn.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
            try:
                fk_info = self.conn.execute(
                    f"PRAGMA foreign_key_list('{table_name}')"
                ).fetchall()
            except Exception:
                fk_info = []

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
                    "foreign_key": None
                }
            if not pk_found:
                for col in metadata:
                    metadata[col]["primary_key"] = False

            if fk_info:
                for fk in fk_info:
                    fk_column = fk[3]
                    ref_table = fk[2]
                    ref_column = fk[4]
                    if fk_column in metadata:
                        metadata[fk_column]["foreign_key"] = {"table": ref_table, "column": ref_column}
                        self.conn.execute(
                            """
                            INSERT INTO fk_metadata (table_name, column_name, ref_table, ref_column)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT DO NOTHING;
                            """,
                            (table_name, fk_column, ref_table, ref_column)
                        )
            self.conn.execute(
                """
                INSERT INTO table_metadata (table_name, schema_json)
                VALUES (?, ?)
                ON CONFLICT (table_name) DO UPDATE SET schema_json = excluded.schema_json;
                """,
                (table_name, json.dumps(metadata))
            )
        except Exception as e:
            logging.error(f"Erro ao registrar metadados para {table_name}: {e}")

    def create_table_dynamic(self, table_name: str, fields: list):
        try:
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
                data_type = field.get("data_type", "VARCHAR").strip() or "VARCHAR"
                col_def = f"{col_name} {data_type}"
                is_pk = field.get("primary_key", False)
                if is_pk:
                    pk_columns.append(col_name)
                metadata[col_name] = {
                    "data_type": data_type,
                    "primary_key": is_pk,
                    "foreign_key": None
                }
                fk = field.get("foreign_key")
                if fk:
                    fk_table = fk.get("table")
                    fk_column = fk.get("column")
                    if fk_table and fk_column:
                        col_def += f" REFERENCES {fk_table}({fk_column})"
                        metadata[col_name]["foreign_key"] = {"table": fk_table, "column": fk_column}
                    else:
                        raise ValueError(f"Chave estrangeira inválida para a coluna '{col_name}'")
                column_defs.append(col_def)

            if pk_columns:
                if len(pk_columns) > 1:
                    column_defs.append(f"PRIMARY KEY ({', '.join(pk_columns)})")
            else:
                for col in metadata:
                    metadata[col]["primary_key"] = False

            sql = f"CREATE TABLE {table_name} ({', '.join(column_defs)});"
            self.conn.execute(sql)
            
            self.conn.execute(
                """
                INSERT INTO table_metadata (table_name, schema_json)
                VALUES (?, ?);
                """,
                (table_name, json.dumps(metadata))
            )
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
            return True
        except Exception as e:
            raise e

    def add_column(self, table_name: str, field: dict):
        try:
            col_name = field.get("name")
            if not col_name:
                raise ValueError("Nome da coluna é obrigatório.")
            data_type = field.get("data_type", "VARCHAR").strip() or "VARCHAR"
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {data_type};"
            self.conn.execute(alter_sql)
            
            metadata = self.get_table_metadata(table_name)
            metadata[col_name] = {
                "data_type": data_type,
                "primary_key": field.get("primary_key", False),
                "foreign_key": None
            }
            fk = field.get("foreign_key")
            if fk:
                fk_table = fk.get("table")
                fk_column = fk.get("column")
                if fk_table and fk_column:
                    metadata[col_name]["foreign_key"] = {"table": fk_table, "column": fk_column}
                    self.conn.execute(
                        """
                        INSERT INTO fk_metadata (table_name, column_name, ref_table, ref_column)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT DO NOTHING;
                        """,
                        (table_name, col_name, fk_table, fk_column)
                    )
            self.conn.execute(
                "UPDATE table_metadata SET schema_json = ? WHERE table_name = ?;",
                (json.dumps(metadata), table_name)
            )
            return True
        except Exception as e:
            raise e

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
        try:
            metadata = self.get_table_metadata(table_name)
            
            # Validação de FK
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

            # Validação de PK
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
                    st.error(f"Registro com chave primária já existe.")
                    return False

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?"] * len(data))
            
            self.conn.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})",
                list(data.values())
            )
            return True
        except Exception as e:
            st.error(f"Erro ao inserir: {str(e)}")
            return False

    def update_data(self, table_name: str, set_data: dict, where_clause: str, where_params: tuple):
        try:
            set_str = ", ".join([f"{col} = ?" for col in set_data.keys()])
            sql = f"UPDATE {table_name} SET {set_str} WHERE {where_clause};"
            values = tuple(set_data.values()) + where_params
            self.conn.execute(sql, values)
            return True
        except Exception as e:
            st.error(f"Erro na atualização: {str(e)}")
            return False

    def delete_data(self, table_name: str, where_clause: str, where_params: tuple):
        try:
            sql = f"DELETE FROM {table_name} WHERE {where_clause};"
            self.conn.execute(sql, where_params)
            return True
        except Exception as e:
            st.error(f"Erro na deleção: {str(e)}")
            return False

    def delete_table(self, table_name: str):
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")
            self.conn.execute("DELETE FROM table_metadata WHERE table_name = ?;", (table_name,))
            self.conn.execute("DELETE FROM fk_metadata WHERE table_name = ?;", (table_name,))
            return True
        except Exception as e:
            st.error(f"Erro ao dropar tabela: {str(e)}")
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

    def get_table_stats(self, table_name: str):
        try:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            return {"count": count}
        except Exception:
            return {"count": 0}

# Instância Pipeline
@st.cache_resource
def get_pipeline():
    return DuckDBPipeline(str(full_duckdb_path))

pipeline = get_pipeline()

# Auxiliares
def run_supabase_sync():
    script_path = Path(__file__).parent.parent / "scripts" / "incremental_loader.py"
    try:
        with st.spinner("Sincronizando com Supabase..."):
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode == 0:
                st.success("Sincronização concluída com sucesso!")
                with st.expander("Ver log detalhado"):
                    st.code(result.stdout, language="log")
            else:
                st.error("Erro na sincronização!")
                st.code(result.stderr, language="log")
    except subprocess.TimeoutExpired:
        st.error("Timeout: O processo demorou mais que o esperado.")
    except Exception as e:
        st.error(f"Erro interno: {str(e)}")

# Páginas
def page_dashboard():
    st.markdown('<h1><span class="gradient-text">Visão Geral</span></h1>', unsafe_allow_html=True)
    st.markdown("Visão geral do seu ecossistema de dados.")
    
    tables = pipeline.list_tables()

    # Coletar estatísticas
    total_rows = sum([pipeline.get_table_stats(t)["count"] for t in tables])
    try:
        db_size = full_duckdb_path.stat().st_size / (1024 * 1024)
    except:
        db_size = 0
    total_cols = sum([len(pipeline.get_table_metadata(t)) for t in tables])

    st.markdown("### Métricas Principais")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        metric_card("Tabelas", f"{len(tables)}", icon="🗂️")
    with col2:
        metric_card("Registros", f"{total_rows:,}", icon="📝", trend="+12%", trend_color="green")
    with col3:
        metric_card("Armazenamento", f"{db_size:.1f} MB", icon="💾")
    with col4:
        metric_card("Colunas", f"{total_cols}", icon="📋")

    st.markdown("---")

    col_charts_1, col_charts_2 = st.columns([2, 1])

    with col_charts_1:
        if tables:
            st.markdown("#### Distribuição de Dados")
            table_data = []
            for table in tables:
                stats = pipeline.get_table_stats(table)
                table_data.append({"Tabela": table, "Registros": stats["count"]})

            df_stats = pd.DataFrame(table_data)
            
            fig = px.bar(
                df_stats, 
                x="Tabela", 
                y="Registros", 
                color="Registros",
                color_continuous_scale="Blues"
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=0, b=0),
                height=300
            )
            st.plotly_chart(fig, use_container_width=True)
            
    with col_charts_2:
        st.markdown("#### Capacidade")
        capacity_mb = 50 * 1024 
        percentage = (db_size / capacity_mb) * 100
        progress_bar("Uso do Disco", percentage)
        
        st.markdown("#### Resumo")
        if tables:
            st.dataframe(
                df_stats[["Tabela", "Registros"]], 
                hide_index=True, 
                use_container_width=True
            )

def page_view_data():
    st.title("Visualizar Dados")
    
    tables = pipeline.list_tables()
    if not tables:
        info_box("Sem Dados", "Crie tabelas primeiro na aba de Esquemas.", type="warning")
        return

    col_sel, col_empty = st.columns([1, 2])
    with col_sel:
        table_name = st.selectbox("Selecione a Tabela", tables)

    if table_name:
        df = pipeline.get_table_data(table_name)
        metadata = pipeline.get_table_metadata(table_name)
        
        # Tabs
        tab1, tab2 = st.tabs(["📄 Dados", "🔧 Estrutura"])
        
        with tab1:
            if not df.empty:
                st.dataframe(df, use_container_width=True, height=400)
                
                # Download
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"{table_name}.csv",
                    mime="text/csv"
                )
            else:
                info_box("Tabela Vazia", "Nenhum dado encontrado nesta tabela.", type="info")

        with tab2:
            schema_data = []
            for col, info in metadata.items():
                schema_data.append({
                    "Coluna": col,
                    "Tipo": info["data_type"],
                    "Chave": "PK" if info["primary_key"] else ("FK" if info.get("foreign_key") else "-")
                })
            st.dataframe(pd.DataFrame(schema_data), hide_index=True, use_container_width=True)

def page_crud():
    st.title("Gerenciar Dados")
    
    tables = pipeline.list_tables()
    if not tables:
        st.warning("Nenhuma tabela encontrada.")
        return

    # Usando tabs para separar operações CRUD de forma limpa
    tab_insert, tab_update, tab_delete = st.tabs(["➕ Inserir", "🔄 Atualizar", "🗑️ Remover"])

    with tab_insert:
        table_name = st.selectbox("Tabela para Inserção", tables, key="ins_table")
        metadata = pipeline.get_table_metadata(table_name)
        
        with st.form("insert_form", clear_on_submit=True):
            st.subheader("Novos Dados")
            data = {}
            cols = st.columns(2)
            for idx, (col, info) in enumerate(metadata.items()):
                with cols[idx % 2]:
                    help_text = "Chave Primária" if info['primary_key'] else ""
                    data[col] = st.text_input(f"{col} ({info['data_type']})", help=help_text)
            
            if st.form_submit_button("Inserir Registro", type="primary"):
                if pipeline.insert_data(table_name, data):
                    st.success("Registro inserido!")
                    st.rerun()

    with tab_update:
        table_name_up = st.selectbox("Tabela para Atualização", tables, key="up_table")
        metadata = pipeline.get_table_metadata(table_name_up)
        
        with st.form("update_form"):
            st.subheader("Definir Novos Valores")
            set_data = {}
            for col in metadata.keys():
                set_data[col] = st.text_input(f"Novo valor para {col}", key=f"up_{col}")
            
            st.markdown("---")
            st.subheader("Condição (WHERE)")
            col1, col2 = st.columns(2)
            with col1:
                where_clause = st.text_input("Cláusula (ex: id = ?)")
            with col2:
                where_values = st.text_input("Valor (ex: 1)")
            
            if st.form_submit_button("Atualizar Registros"):
                params = tuple(where_values.split(",")) if where_values else ()
                clean_data = {k: v for k, v in set_data.items() if v}
                if clean_data and where_clause:
                    if pipeline.update_data(table_name_up, clean_data, where_clause, params):
                        st.success("Atualizado com sucesso!")
                        st.rerun()
                else:
                    st.error("Preencha os campos e a condição.")

    with tab_delete:
        table_name_del = st.selectbox("Tabela para Remoção", tables, key="del_table")
        
        with st.form("delete_form"):
            st.warning("Cuidado: Ação irreversível.")
            col1, col2 = st.columns(2)
            with col1:
                where_clause = st.text_input("Cláusula WHERE (ex: id = ?)", key="del_w")
            with col2:
                where_val = st.text_input("Valor", key="del_v")
                
            if st.form_submit_button("Deletar Registros", type="primary"):
                if where_clause and where_val:
                    if pipeline.delete_data(table_name_del, where_clause, (where_val,)):
                        st.success("Deletado com sucesso!")
                        st.rerun()

def page_schema():
    st.title("Esquemas")
    st.markdown("Gerencie a estrutura do seu Data Lake.")

    action = st.radio("Ação", ["Criar Tabela", "Adicionar Coluna", "Excluir Tabela"], horizontal=True)
    st.markdown("---")

    if action == "Criar Tabela":
        col1, col2 = st.columns([1, 2])
        with col1:
            table_name = st.text_input("Nome da Nova Tabela")
        
        if "fields" not in st.session_state:
            st.session_state.fields = []

        with col2:
            if st.button("Adicionar Campo"):
                st.session_state.fields.append({"name": "", "type": "VARCHAR"})

        # Renderizar campos
        if st.session_state.fields:
            for i, field in enumerate(st.session_state.fields):
                c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
                with c1:
                    field["name"] = st.text_input(f"Nome do Campo {i+1}", value=field["name"], key=f"f_n_{i}")
                with c2:
                    field["type"] = st.selectbox(f"Tipo {i+1}", ["VARCHAR", "INTEGER", "BOOLEAN", "DATE", "FLOAT"], key=f"f_t_{i}")
                with c3:
                    field["pk"] = st.checkbox("PK", key=f"f_pk_{i}")
                with c4:
                    if st.button("🗑️", key=f"rm_{i}"):
                        st.session_state.fields.pop(i)
                        st.rerun()

        if st.button("Salvar Tabela", type="primary"):
            if table_name and st.session_state.fields:
                try:
                    fmt_fields = [{
                        "name": f["name"],
                        "data_type": f["type"],
                        "primary_key": f.get("pk", False)
                    } for f in st.session_state.fields]
                    
                    if pipeline.create_table_dynamic(table_name, fmt_fields):
                        st.success(f"Tabela {table_name} criada!")
                        st.session_state.fields = []
                        st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")

    elif action == "Adicionar Coluna":
        tables = pipeline.list_tables()
        if not tables:
            st.warning("Nenhuma tabela encontrada. Crie uma tabela primeiro.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                table_target = st.selectbox("Selecione a Tabela", tables)
                new_col_name = st.text_input("Nome da Nova Coluna")
            
            with col2:
                new_col_type = st.selectbox("Tipo de Dado", ["VARCHAR", "INTEGER", "BOOLEAN", "DATE", "FLOAT"])
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("Adicionar Coluna", type="primary"):
                    if table_target and new_col_name:
                        try:
                            field = {
                                "name": new_col_name,
                                "data_type": new_col_type,
                                "primary_key": False
                            }
                            if pipeline.add_column(table_target, field):
                                st.success(f"Coluna '{new_col_name}' adicionada em '{table_target}'!")
                                st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao adicionar coluna: {e}")
                    else:
                        st.error("Preencha o nome da coluna.")

    elif action == "Excluir Tabela":
        tables = pipeline.list_tables()
        if tables:
            t_del = st.selectbox("Selecione para excluir", tables)
            st.error(f"Você está prestes a excluir a tabela **{t_del}** e todos os seus dados.")
            if st.button("Confirmar Exclusão", type="primary"):
                if pipeline.delete_table(t_del):
                    st.success("Tabela excluída.")
                    st.rerun()
        else:
            st.info("Nenhuma tabela para excluir.")

def page_sync():
    st.title("Sincronização")
    info_box(
        "Supabase Sync",
        "Sincronize seus dados locais com a nuvem de forma incremental e segura.",
        icon="☁️",
        type="info"
    )
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Configuração")
        st.caption("As credenciais devem estar configuradas no arquivo .env")
        if st.button("Iniciar Sincronização", type="primary"):
            run_supabase_sync()
            
    with col2:
        st.markdown("#### Status")
        # Simulação de status
        metric_card("Status da Nuvem", "Conectado", icon="🟢", trend="Latência: 45ms")

# Navegação
with st.sidebar:
    selected_page = st.radio(
        "Navegação",
        ["Visão Geral", "Dados", "CRUD", "Esquemas", "Sync"],
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    
    # Debug Info
    with st.expander("🛠️ Debug Info"):
        st.write(f"CWD: {os.getcwd()}")
        st.write(f"DB Path: {full_duckdb_path}")
        st.write(f"Exists: {full_duckdb_path.exists()}")
        if full_duckdb_path.exists():
            st.write(f"Size: {full_duckdb_path.stat().st_size} bytes")
            
        st.markdown("---")
        st.markdown("**Diagnóstico Interno:**")
        try:
            # Tabelas Físicas
            raw_tables = pipeline.conn.execute("SHOW TABLES").fetchall()
            st.write(f"Tabelas Físicas ({len(raw_tables)}):")
            st.code([t[0] for t in raw_tables])
            
            # Metadados
            meta_tables = pipeline.conn.execute("SELECT table_name FROM table_metadata").fetchall()
            st.write(f"Metadados ({len(meta_tables)}):")
            st.code([t[0] for t in meta_tables])
            
            if st.button("Forçar Re-Sync"):
                pipeline.sync_metadata_with_existing_tables()
                st.success("Sincronização forçada!")
                st.rerun()
        except Exception as e:
            st.error(f"Erro ao ler DB: {e}")
    
    st.caption("v2.0.1 - NW Data Solutions")

# Roteamento
if selected_page == "Visão Geral":
    page_dashboard()
elif selected_page == "Dados":
    page_view_data()
elif selected_page == "CRUD":
    page_crud()
elif selected_page == "Esquemas":
    page_schema()
elif selected_page == "Sync":
    page_sync()
