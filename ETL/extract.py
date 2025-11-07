import os
import csv
import re
import logging
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

sql_path = os.getenv('SQL_SOURCE_PATH')
output_dir = os.getenv('CSV_OUTPUT_DIR', './csv_output')

if not sql_path:
    raise ValueError("Variável SQL_SOURCE_PATH não definida no arquivo .env")

os.makedirs(output_dir, exist_ok=True)
logger.info(f"Diretório de saída: {output_dir}")

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
        logger.info(f'Arquivo CSV gerado para a tabela {table} em: {csv_path}')

if __name__ == "__main__":
    try:
        logger.info(f"Iniciando extração do arquivo SQL: {sql_path}")
        data = parse_sql_file(sql_path)
        write_csv_files(output_dir, data)
        logger.info("Extração concluída com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante a extração: {e}")
        raise
