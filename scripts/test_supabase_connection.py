#!/usr/bin/env python
"""
Script para testar a conexão com o Supabase.
Use este script para validar suas credenciais antes de configurar os GitHub Secrets.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Adicionar o diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Carregar variáveis de ambiente
load_dotenv()

def test_connection():
    """Testa a conexão com o Supabase e exibe informações úteis."""

    print("=" * 60)
    print("🔍 TESTANDO CONEXÃO COM SUPABASE")
    print("=" * 60)
    print()

    # Verificar variáveis de ambiente
    required_vars = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
    }

    optional_vars = {
        "DB_PORT": os.getenv("DB_PORT", "5432"),
        "DB_SSLMODE": os.getenv("DB_SSLMODE", "prefer"),
    }

    print("📋 Variáveis de Ambiente:")
    print("-" * 60)

    missing_vars = []
    for var, value in required_vars.items():
        if value:
            # Mascarar senha
            display_value = "***" if var == "DB_PASSWORD" else value
            print(f"✅ {var:15} = {display_value}")
        else:
            print(f"❌ {var:15} = (não configurado)")
            missing_vars.append(var)

    for var, value in optional_vars.items():
        print(f"ℹ️  {var:15} = {value}")

    print()

    if missing_vars:
        print("❌ ERRO: Variáveis obrigatórias não configuradas:")
        for var in missing_vars:
            print(f"   - {var}")
        print()
        print("💡 Configure estas variáveis no arquivo .env")
        print(f"   Localização: {Path(__file__).parent.parent / '.env'}")
        return False

    # Tentar importar psycopg2
    print("📦 Verificando dependências...")
    try:
        import psycopg2
        print("✅ psycopg2 instalado")
    except ImportError:
        print("❌ psycopg2 não instalado")
        print()
        print("💡 Instale o psycopg2-binary:")
        print("   pip install psycopg2-binary")
        return False

    print()

    # Tentar conectar
    print("🔌 Tentando conectar ao Supabase...")
    print("-" * 60)

    connection_params = {
        "host": required_vars["DB_HOST"],
        "database": required_vars["DB_NAME"],
        "user": required_vars["DB_USER"],
        "password": required_vars["DB_PASSWORD"],
        "port": optional_vars["DB_PORT"],
    }

    if optional_vars["DB_SSLMODE"]:
        connection_params["sslmode"] = optional_vars["DB_SSLMODE"]

    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Testar query simples
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]

        # Obter informações do banco
        cursor.execute("SELECT current_database(), current_user;")
        db_name, current_user = cursor.fetchone()

        print(f"✅ Conexão bem-sucedida!")
        print()
        print("📊 Informações do Banco:")
        print(f"   Database: {db_name}")
        print(f"   User: {current_user}")
        print(f"   PostgreSQL: {version.split(',')[0]}")
        print()

        # Verificar se já existem tabelas da pipeline
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('controle_cargas', 'table_metadata', 'fk_metadata')
            ORDER BY table_name;
        """)
        pipeline_tables = cursor.fetchall()

        if pipeline_tables:
            print("📋 Tabelas da Pipeline encontradas:")
            for (table,) in pipeline_tables:
                print(f"   ✓ {table}")
        else:
            print("ℹ️  Nenhuma tabela da pipeline encontrada (será criada na primeira execução)")

        cursor.close()
        conn.close()

        print()
        print("=" * 60)
        print("✅ TESTE CONCLUÍDO COM SUCESSO!")
        print("=" * 60)
        print()
        print("🎯 Próximos passos:")
        print("   1. Use estas mesmas credenciais nos GitHub Secrets")
        print("   2. Execute: make run-loader (para testar sincronização local)")
        print("   3. Execute: make run-app (para abrir a interface)")
        print()

        return True

    except Exception as e:
        print(f"❌ ERRO na conexão: {str(e)}")
        print()
        print("💡 Possíveis soluções:")
        print("   1. Verifique se as credenciais estão corretas no arquivo .env")
        print("   2. Confirme que o host está acessível")
        print("   3. Verifique se a senha está correta (sem aspas extras)")
        print("   4. Tente adicionar DB_SSLMODE=require no .env")
        print()
        print("📖 Consulte: GITHUB_SECRETS_SETUP.md para mais informações")
        print()

        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
