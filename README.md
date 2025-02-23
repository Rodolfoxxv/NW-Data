# NW-Data

## Descrição do Projeto

**NW-Data** é um pipeline de processamento e análise de dados baseado em **DuckDB** e **Supabase**, projetado para carregar, transformar e gerenciar tabelas de forma eficiente. O projeto inclui uma estrutura ETL completa e uma aplicação em **Streamlit** para visualização e consulta de dados.

![Stack](imagens/stack_png.png)

## Tecnologias Utilizadas

- **Python**
- **DuckDB**
- **Supabase**
- **Streamlit**
- **uv**

## Instalação

1. **Clone o repositório:**

   ```bash
   git clone https://github.com/Rodolfoxxv/NW-Data.git
   cd NW-Data
   ```

2. **Crie um ambiente virtual opcional:**

   ```bash
   pip install uv
   uv venv
   ```

3. **Instale as dependências usando `uv`:**

   ```bash
   uv sync
   ```

### Se prefiri usar o pip

## Crie o ambiente virtual

  ```bash
   python -m venv
   Venv\Scripts\activate
  ```

## Instale as dependencias

   ```bash
   pip install -r requirements.txt
   ```

## Configuração

1. **Crie um arquivo `.env` na pasta `scripts` e defina as variáveis de ambiente:**

   ```ini
   DUCKDB_PATH=database.duckdb
   DB_HOST=seu_host_postgres
   DB_NAME=nome_do_banco_de_dados
   DB_USER=seu_usuario
   DB_PASSWORD=sua_senha
   DB_PORT=5432 
   ```

## Uso

1. **Inicie a aplicação de visualização DuckDB com Streamlit:**

   ```bash
   streamlit run app/duckdb_app.py
   ```

2. **Variaveis secrets do Github**

## Adicone as variaveis de ambiente no Github secrets

```bash
      - name: Set up environment variables
        run: |
          echo "SUPABASE_URL=${{ secrets.SUPABASE_URL }}" >> $GITHUB_ENV
          echo "SUPABASE_KEY=${{ secrets.SUPABASE_KEY }}" >> $GITHUB_ENV
          echo "DUCKDB_PATH=${{ secrets.DUCKDB_PATH }}" >> $GITHUB_ENV
          echo "DB_HOST=${{ secrets.DB_HOST }}" >> $GITHUB_ENV
          echo "DB_NAME=${{ secrets.DB_NAME }}" >> $GITHUB_ENV
          echo "DB_USER=${{ secrets.DB_USER }}" >> $GITHUB_ENV
          echo "DB_PASSWORD=${{ secrets.DB_PASSWORD }}" >> $GITHUB_ENV
          echo "DB_PORT=${{ secrets.DB_PORT }}" >> $GITHUB_ENV
          echo "DESTINATION_PATH=${{ secrets.DESTINATION_PATH }}" >> $GITHUB_ENV
```

## Estrutura do Projeto

```text
NW-Data/
├── app/
│   └── duckdb_app.py 
├── data/
│   └── database.duckdb
├── docs/
│   └── nw_data_interface.md # Documentação da Interface Sreamlit x DuckDB
│   └── nw_pipeline.md # Documentação do Pipeline
├── ETL/
│   └── ...  # Scripts de ETL
├── scripts/
│   └── incremental_loader.py 
├── LICENSE 
├── README.md
└── pipeline.log 

```

## Contribuição

Contribuições são bem-vindas! Para contribuir:

1. Faça um fork do repositório.
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`).
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`).
4. Faça um push para a branch (`git push origin feature/nova-feature`).
5. Abra um Pull Request.

## Licença

Este projeto está licenciado sob a **MIT License**. Consulte o arquivo `LICENSE` para mais detalhes.
