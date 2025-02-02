import os
import csv
import re

# Caminho do arquivo SQL
sql_path = r'D:\Projetos\Nort\mywind-master\mywind-master\northwind-data.sql'
# Diretório de saída para os arquivos CSV
output_dir = r'D:\Projetos\Nort\mywind-master\mywind-master\csv_output'

# Certifica-se de que o diretório de saída existe
os.makedirs(output_dir, exist_ok=True)

# Padrões regex para capturar o nome da tabela e dados
table_name_pattern = re.compile(r"Dumping data for table '(\w+)'")
insert_pattern = re.compile(r"INSERT INTO `(\w+)` \((.+)\) VALUES (.+);")

def parse_sql_file(sql_path):
    data = {}
    current_table = None
    with open(sql_path, 'r', encoding='utf-8') as file:
        for line in file:
            table_match = table_name_pattern.search(line)
            insert_match = insert_pattern.search(line)

            if table_match:
                current_table = table_match.group(1)
                data[current_table] = []
            elif insert_match:
                table, columns, values = insert_match.groups()
                if table == current_table:
                    columns = [col.strip('`') for col in columns.split(', ')]
                    values = re.findall(r"'(.*?)'|(\d+)", values)
                    values = [value[0] if value[0] else value[1] for value in values]
                    data[current_table].append((columns, values))
    return data

def write_csv_files(output_dir, data):
    for table, rows in data.items():
        csv_path = os.path.join(output_dir, f'{table}.csv')
        with open(csv_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=';')
            writer.writerow(rows[0][0])  # Escreve os cabeçalhos
            for columns, values in rows:
                writer.writerow(values)
        print(f'O arquivo CSV foi gerado para a tabela {table} em: {csv_path}')

data = parse_sql_file(sql_path)
write_csv_files(output_dir, data)
