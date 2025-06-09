# etl_main.py
from datetime import datetime, timedelta
import os
import sys

# Adiciona o diretório raiz do projeto ao PATH para importações relativas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dim_scripts.dim_time_etl import etl_dim_time
from dim_scripts.dim_user_etl import etl_dim_user
from dim_scripts.dim_zone_etl import etl_dim_zone
from dim_scripts.dim_neighborhood_etl import etl_dim_neighborhood
from dim_scripts.dim_hub_etl import etl_dim_hub
from dim_scripts.dim_status_pedido_etl import etl_dim_status_pedido

from fact_scripts.fact_carona_etl import etl_fact_carona
from fact_scripts.fact_interacao_carona_etl import etl_fact_interacao_carona

from utils import connect_to_db, execute_sql
from sql_queries import ALL_DDL_DROP_QUERIES, ALL_DDL_CREATE_QUERIES, ALL_DDL_DROP_QUERIES_MENOS_TEMPO, ALL_DDL_CREATE_QUERIES_MENOS_TEMPO
from config import DB_OLTP, DB_DW, LAST_RUN_FILE

def get_last_etl_run_date():
    """Lê a última data de execução do arquivo de controle."""
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, 'r') as f:
            date_str = f.read().strip()
            if date_str:
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    # Se for a primeira execução ou arquivo vazio, defina uma data bem antiga
    print("Arquivo de last_etl_run.txt não encontrado ou vazio. Usando data de início padrão.")
    return datetime(2000, 1, 1)

def set_last_etl_run_date(dt):
    """Grava a data atual da execução no arquivo de controle."""
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(dt.strftime("%Y-%m-%d %H:%M:%S.%f"))

def create_dw_tables(conn_dw, recria_dim_time):
    """
    Dropa todas as tabelas existentes no DW e as recria.
    Isso garante um ambiente limpo para cada execução completa do ETL.
    Retorna True em caso de sucesso, False em caso de falha.
    """
    print("Verificando e recriando tabelas do Data Warehouse...")

    if recria_dim_time:
        DROP_QUERIES = ALL_DDL_DROP_QUERIES
        CREATE_QUERIES = ALL_DDL_CREATE_QUERIES
    else:
        DROP_QUERIES = ALL_DDL_DROP_QUERIES_MENOS_TEMPO
        CREATE_QUERIES = ALL_DDL_CREATE_QUERIES_MENOS_TEMPO
    
    try:
        cur = conn_dw.cursor()

        # --- PASSO 1: DROPAR TODAS AS TABELAS (para garantir um estado limpo) ---
        print("Dropping existing tables (if any)...")
        # Itera sobre as queries de DROP em ordem inversa de dependência (definida em sql_queries.py)
        for query in DROP_QUERIES:
            try:
                cur.execute(query)
                conn_dw.commit() # Commita cada DROP para que as dependências sejam liberadas
                print(f"  - Query DROP executada com sucesso: {query.splitlines()[0].strip()}...")
            except Exception as e:
                conn_dw.rollback() # Em caso de erro, desfaz a transação atual
                # Avisa, mas continua, pois a tabela pode não existir na primeira execução
                print(f"  - Aviso: Erro ao dropar tabela (pode não existir): {e}. Query: {query.splitlines()[0].strip()}...")
        print("Finished dropping tables.")

        # --- PASSO 2: CRIAR TODAS AS TABELAS ---
        print("Creating new tables...")
        # Itera sobre as queries de CREATE (definida em sql_queries.py)
        for query in CREATE_QUERIES:
            try:
                cur.execute(query)
                conn_dw.commit() # Commita cada CREATE
                print(f"  - Query CREATE executada com sucesso: {query.splitlines()[0].strip()}...")
            except Exception as e:
                conn_dw.rollback() # Em caso de erro, desfaz a transação atual
                print(f"  - ERRO Crítico ao criar tabela: {e}. Query: {query.splitlines()[0].strip()}...")
                raise # Re-levanta o erro para abortar o ETL
        print("Todas as tabelas do DW verificadas/criadas com sucesso.")
        return True # Retorna True se tudo ocorrer bem

    except Exception as e:
        print(f"Erro fatal ao criar/dropar tabelas do DW: {e}")
        return False # Retorna False se houver um erro grave
    finally:
        cur.close()

def main_etl_process(apaga_ultimo_etl_run, recria_dim_time):
    conn_oltp = None
    conn_dw = None
    try:
        print("Iniciando processo ETL para Caronaê DW...")

        # 0. Apaga o last_etl_run.txt se o parâmetro for True
        if apaga_ultimo_etl_run:
            if os.path.exists(LAST_RUN_FILE):
                os.remove(LAST_RUN_FILE)
                print(f"Arquivo '{LAST_RUN_FILE}' apagado (reset de carga incremental).")
            else:
                print(f"Arquivo '{LAST_RUN_FILE}' não encontrado para apagar.")
        else:
            print(f"Arquivo '{LAST_RUN_FILE}' não será apagado (carga incremental mantida).")

        # Conectar aos bancos de dados OLTP e DW
        print("\nEstabelecendo conexões com os bancos de dados...")
        conn_oltp = connect_to_db(DB_OLTP)
        conn_dw = connect_to_db(DB_DW)

        if not conn_oltp or not conn_dw:
            print("Erro: Não foi possível conectar a um ou ambos os bancos de dados. Abortando ETL.")
            return # Sai da função se a conexão falhar

        print("Conexões com os bancos de dados estabelecidas com sucesso.")

        # 1. Criar/Recriar tabelas do DW (Drop e Create)
        if not create_dw_tables(conn_dw, recria_dim_time):
            print("ETL abortado devido a falha na criação/recriação das tabelas do DW.")
            return # Sai da função se as tabelas não puderem ser criadas

        # Obter a última data de execução para carga incremental
        last_run_date = get_last_etl_run_date()
        current_run_date = datetime.now() # Marcar a hora de início desta execução

        # 2. Executar ETL das Dimensões
        print("\n--- Iniciando ETL das Dimensões ---")
        # A dim_time geralmente só precisa ser carregada uma vez ou quando estender o período.
        if recria_dim_time:
            etl_dim_time()
        
        if not etl_dim_user(): print("ETL DimUser falhou.")
        if not etl_dim_zone(): print("ETL DimZone falhou.")
        if not etl_dim_neighborhood(): print("ETL DimNeighborhood falhou.")
        if not etl_dim_hub(): print("ETL DimHub falhou.")
        if not etl_dim_status_pedido(): print("ETL DimStatusPedido falhou.")
        print("--- ETL das Dimensões Concluído ---")

        # 3. Executar ETL dos Fatos (Carga Incremental)
        print("\n--- Iniciando ETL dos Fatos (Incremental) ---")
        # Passar a data de last_run_date como string para a função
        if not etl_fact_carona(last_run_date.strftime("%Y-%m-%d %H:%M:%S.%f")): print("ETL FatoCarona falhou.")
        if not etl_fact_interacao_carona(last_run_date.strftime("%Y-%m-%d %H:%M:%S.%f")): print("ETL FatoInteracaoCarona falhou.")
        print("--- ETL dos Fatos Concluído ---")

        # 4. Atualizar a marca d'água da última execução
        set_last_etl_run_date(current_run_date)
        print(f"\nProcesso ETL concluído com sucesso! Última execução registrada em: {current_run_date}")
    
    except Exception as e:
        # Captura qualquer exceção não tratada e a imprime
        print(f"\nOcorreu um erro crítico inesperado no processo ETL: {e}")
        # Opcional: registrar stack trace para depuração mais detalhada
        # import traceback
        # traceback.print_exc()
    finally:
        # Garante que as conexões sejam fechadas, mesmo em caso de erro
        if conn_oltp:
            print("Fechando conexão com OLTP.")
            conn_oltp.close()
        if conn_dw:
            print("Fechando conexão com DW.")
            conn_dw.close()
        print("Conexões de banco de dados fechadas.")

if __name__ == "__main__":
    # Para forçar uma carga completa (apaga o last_etl_run.txt e recarrega tudo):
    # Use isso quando quiser ter certeza que tudo está limpo e do zero.
    main_etl_process(apaga_ultimo_etl_run=True, recria_dim_time=False)

    # Para uma carga normal (mantém o last_etl_run.txt e faz carga incremental):
    # main_etl_process(apaga_ultimo_etl_run=False)