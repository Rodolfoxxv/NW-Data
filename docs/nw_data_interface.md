# Documentação do Projeto: Gerenciamento de Dados com Streamlit e DuckDB

## Descrição Geral

Este projeto visa a criação de uma interface de gerenciamento de dados utilizando **Streamlit** e **DuckDB**. Ele permite que o usuário crie tabelas dinâmicas, insira, atualize, delete dados e visualize tabelas no banco de dados DuckDB. A interface interativa do Streamlit permite que os usuários definam esquemas de tabelas manualmente ou via JSON.

## Dependências

- `streamlit`: Framework para criar a interface web interativa.
- `pandas`: Biblioteca para manipulação de dados em formato de tabelas.
- `plotly`: Biblioteca para criação de gráficos interativos.
- `duckdb`: Banco de dados relacional de alto desempenho em memória.
- `os`: Utilizada para manipulação de arquivos e variáveis de ambiente.
- `pathlib`: Manipulação de caminhos de arquivos de forma independente de sistema operacional.
- `dotenv`: Leitura de variáveis de ambiente a partir de um arquivo `.env`.
- `logging`: Para registro de logs.
- `json`: Manipulação de dados no formato JSON.
- `contextlib` e `typing`: Tipagem estática e contexto para controle de recursos.

## Estrutura do Código

### 1. **Configuração do Ambiente**

- O código carrega variáveis de ambiente usando o `dotenv`, incluindo o caminho do banco de dados DuckDB e o destino dos dados.
- A variável `DUCKDB_PATH` contém o caminho relativo do arquivo do banco de dados DuckDB, enquanto `DESTINATION_PATH` é o diretório onde o arquivo será armazenado.
- A configuração falha caso uma dessas variáveis não esteja presente ou o arquivo do banco de dados não seja encontrado.

```python
load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH")
DESTINATION_PATH = os.getenv("DESTINATION_PATH")

if not all([DUCKDB_PATH, DESTINATION_PATH]):
    raise ValueError("Variáveis de ambiente ausentes. Verifique o arquivo .env.")
```

### 2. **Funcionalidades Principais**

#### Gerenciamento de Tabelas

- Criação dinâmica de tabelas com suporte a diferentes tipos de dados
- Definição de chaves primárias e estrangeiras
- Validação de esquemas de tabelas
- Prevenção de duplicação de tabelas

#### Manipulação de Dados

- Inserção, atualização e remoção de registros
- Consulta de dados com suporte a filtros e ordenação
- Visualização de dados em formato tabular

#### Visualização de Dados

- Gráficos interativos com Plotly
- Exportação de dados em formato CSV

### 3. **Interface do Usuário**

- Página principal com opções para criar tabelas, inserir dados, visualizar dados e exportar dados.
- Área de feedback para mensagens de sucesso/erro.
- Visualização dinâmica das tabelas existentes.

### 4. **Execução do Código**

- Execução do código inicia a interface do usuário do Streamlit.
- A interface permite que o usuário crie tabelas, insira dados, visualize dados e exporte dados.
- O código é executado em um ambiente controlado, garantindo a consistência e a segurança.

## Fluxo de Trabalho

1. **Configuração Inicial**:
   - Carrega variáveis de ambiente.
   - Verifica a existência do arquivo do banco de dados.
2. **Criação de Tabelas**:
   - Usuário define o esquema da tabela.
   - O código valida o esquema e cria a tabela no banco de dados.
3. **Manipulação de Dados**:
   - Usuário insere, atualiza, remove e consulta dados
   - Visualização e exportação de dados
   - Geração de gráficos interativos
   - Exportação de dados em formato CSV

## Operações CRUD

- Create: Inserção de novos registros com validação de dados
- Read: Visualização de dados em formato tabular
- Update: Atualização de registros existentes
- Delete: Remoção de registros selecionados

## Interface do Usuário

### Página Principal

- Seletor de operações (Criar Tabela, Inserir Dados, Visualizar Dados)
- Área de feedback para mensagens de sucesso/erro
- Visualização dinâmica das tabelas existentes

### Criação de Tabelas

- Formulário interativo para definição de campos
- Opções para tipos de dados (VARCHAR, INTEGER, BOOLEAN, etc.)
- Configuração de chaves primárias e estrangeiras
- Upload de schema via arquivo JSON

### Visualização de Dados

- Tabelas interativas com paginação
- Filtros dinâmicos por coluna
- Exportação de dados em diferentes formatos
- Gráficos estatísticos básicos

### Segurança

- Validação de entrada de dados
- Prevenção de injeção SQL
- Tratamento de erros robusto

#### Performance

- Otimização de consultas
- Gerenciamento eficiente de conexões
- Cache de dados quando apropriado

### Exemplo de criação de tabela com JSON

```json
[
    {
        "name": "Id_p",
        "data_type": "INTEGER",
        "primary_key": true
    },
    {
        "name": "Nome",
        "data_type": "VARCHAR"
    },
    {
        "name": "Telefone",
        "data_type": "VARCHAR"
    },
    {
        "name": "Data_Cadastro",
        "data_type": "TIMESTAMP"
    },
    {
        "name": "Cliente_Id",
        "data_type": "INTEGER",
        "foreign_key": {
            "table": "clientes",
            "column": "id"
        }
    }
]

