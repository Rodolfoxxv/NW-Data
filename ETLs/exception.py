import os
import duckdb


csv_file = r'D:\Projetos\Nort\mywind-master\mywind-master\csv_output\products.csv'
table_name = 'products'
temp_table_name = 'temp_' + table_name


if not os.path.exists(csv_file):
    print(f"Arquivo CSV para a tabela '{table_name}' não encontrado.")
    exit()

conn = duckdb.connect(database=r'D:\Projetos\NW-Data\data\database.duckdb')

tabela_existe = conn.execute(
    f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
).fetchone()[0]

if not tabela_existe:
    print(f"A tabela '{table_name}' não existe. Criando automaticamente com base no CSV...")
    conn.execute(f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM read_csv('{csv_file}', delim=',', header=True)
        LIMIT 0
    """)

conn.execute(f"""
    CREATE OR REPLACE TEMPORARY TABLE {temp_table_name} AS 
    SELECT * FROM read_csv('{csv_file}', delim=',', header=True)
""")
print(f"Dados carregados na tabela temporária '{temp_table_name}'.")


conn.execute(f"""
    INSERT INTO {table_name}
    SELECT * FROM {temp_table_name}
""")
print(f"Dados transferidos com sucesso para a tabela definitiva '{table_name}'!")

# Fechar a conexão
conn.close()
