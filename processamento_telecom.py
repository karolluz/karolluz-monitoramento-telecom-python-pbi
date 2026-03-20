import pandas as pd
from sqlalchemy import create_engine

# 1. Configuração da Conexão (O "Caminho" até o seu Banco)
# Lembre-se: No GitHub, é elegante deixar as credenciais claras para quem for testar
DB_USER = 'postgres'
DB_PASS = 'admin123'
DB_HOST = '127.0.0.1'
DB_PORT = '6125'
DB_NAME = 'postgres'

def processar_dados_telecom():
    try:
        # Criando a conexão com o banco de dados
        engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        print("✅ Conexão estabelecida com sucesso!")

        # 2. EXTRAÇÃO: Buscando as transações que estão com erro ou pendentes
        query = "SELECT * FROM public.transacoes"
        df_transacoes = pd.read_sql(query, engine)
        print(f"📊 {len(df_transacoes)} transações carregadas para processamento.")

        # 3. TRANSFORMAÇÃO: A "Mágica" do Python (O que sua chefe faz no Procv)
        # Vamos simular que o script resolve as falhas de integração
        # Se o status for 'Falha', o Python "corrige" para 'Sucesso via Automação'
        df_transacoes.loc[df_transacoes['status_venda'] == 'Falha', 'status_venda'] = 'Sucesso via Python'
        
        # 4. CARGA: Devolvendo os dados corrigidos para o banco
        # O 'replace' substitui a tabela antiga pela nova e atualizada
        df_transacoes.to_sql('transacoes', engine, if_exists='replace', index=False)
        print("🚀 Dados atualizados no PostgreSQL com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante o processamento: {e}")

if __name__ == "__main__":
    processar_dados_telecom()