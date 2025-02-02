import os
import duckdb
import pandas as pd
import glob
import csv
import json

# Diretório dos arquivos CSV
csv_dir = r'D:\Projetos\Nort\mywind-master\mywind-master\csv_output'

# Caminho para o arquivo que armazena as tabelas já carregadas
tabelas_carregadas_file = 'tabelas_carregadas.json'

# Carregar a lista de tabelas já carregadas, se existir
if os.path.exists(tabelas_carregadas_file):
    with open(tabelas_carregadas_file, 'r') as f:
        tabelas_ja_carregadas = json.load(f)
else:
    tabelas_ja_carregadas = [
        'customers', 'employees', 'employee_privileges',
        'inventory_transaction_types', 'orders_status', 'orders_tax_status',
        'order_details_status', 'privileges', 'products', 'purchase_order_status',
        'suppliers','shippers','orders','purchase_orders','invoices','sales_reports',
        'strings'
    ]

# Lista das tabelas na ordem correta de carregamento
tabelas_em_ordem = [
    'customers',
    'employees',
    'suppliers',
    'shippers',
    'products',
    'inventory_transaction_types',
    'orders_status',
    'orders_tax_status',
    'purchase_order_status',
    'order_details_status',
    'orders',
    'purchase_orders',
    'inventory_transactions',
    'order_details',
    'purchase_order_details',
    'invoices',
    'sales_reports',
    'strings'
]

# Conectar ao banco de dados DuckDB
conn = duckdb.connect(database=r'D:\Projetos\NW-Data\data\database.duckdb')

# Loop para carregar as tabelas na ordem especificada
for table_name in tabelas_em_ordem:
    # Ignorar as tabelas que já foram carregadas
    if table_name in tabelas_ja_carregadas:
        continue

    csv_file = os.path.join(csv_dir, f'{table_name}.csv')

    print(f"\nProcessando a tabela '{table_name}'...")

    if not os.path.exists(csv_file):
        print(f"Arquivo CSV para a tabela '{table_name}' não encontrado. Pulando...")
        continue

    try:
        # Ajustar a leitura do CSV para considerar aspas
        df = pd.read_csv(
            csv_file,
            delimiter=';',
            quotechar='"',
            engine='python',
            on_bad_lines='warn',
            dtype=str  # Ler todos os campos como string inicialmente
        )
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV '{csv_file}': {e}")
        continue

    # Verificar colunas da tabela existente no DuckDB
    columns_db = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    if not columns_db:
        print(f"Tabela '{table_name}' não existe no banco de dados.")
        continue

    columns_db_names = [col[1] for col in columns_db]
    columns_db_types = [col[2] for col in columns_db]

    # Adaptar o DataFrame para corresponder às colunas da tabela DuckDB
    for col in columns_db_names:
        if col not in df.columns:
            df[col] = None  # Adicionar coluna faltante com valor None
    df = df[columns_db_names]  # Reordenar colunas para corresponder à tabela

    # Converter os tipos de dados no DataFrame para corresponder à tabela
    for col_name, col_type in zip(columns_db_names, columns_db_types):
        if col_name in df.columns:
            if 'INT' in col_type.upper():
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif 'DECIMAL' in col_type.upper() or 'DOUBLE' in col_type.upper() or 'FLOAT' in col_type.upper():
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
            elif 'BOOLEAN' in col_type.upper():
                df[col_name] = df[col_name].astype(bool)
            elif 'VARCHAR' in col_type.upper() or 'TEXT' in col_type.upper() or 'STRING' in col_type.upper():
                df[col_name] = df[col_name].astype(str)
            elif 'DATE' in col_type.upper() or 'TIMESTAMP' in col_type.upper():
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')

    # Identificar a(s) coluna(s) de chave primária
    primary_keys = [col[1] for col in columns_db if col[5] == True]
    if primary_keys:
        df.dropna(subset=primary_keys, inplace=True)
    else:
        print(f"Tabela '{table_name}' não possui chave primária definida. Pulando a remoção de NaNs.")

    # Tentar inserir os dados na tabela real
    try:
        # Registrar o DataFrame pandas no DuckDB como uma tabela temporária
        temp_table_name = f'df_temp_{table_name}'
        conn.register(temp_table_name, df)

        # Inserir os dados da tabela temporária na tabela real
        conn.execute(f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_table_name}
        """)

        print(f"Tabela '{table_name}' carregada no DuckDB com sucesso.")

        # Adicionar a tabela à lista de tabelas já carregadas
        tabelas_ja_carregadas.append(table_name)

        # Salvar a lista atualizada em arquivo
        with open(tabelas_carregadas_file, 'w') as f:
            json.dump(tabelas_ja_carregadas, f)

    except Exception as e:
        print(f"Erro ao inserir dados na tabela '{table_name}': {e}")
        # Salvar o DataFrame problemático para análise posterior
        df.to_csv(f'error_{table_name}.csv', index=False, sep=';')
        continue  # Pular para a próxima tabela

# Fechar a conexão
conn.close()
