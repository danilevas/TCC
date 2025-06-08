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
from sql_queries import ALL_DDL_QUERIES
from config import DB_DW

# Arquivo para armazenar a última data de execução do ETL para cargas incrementais
LAST_RUN_FILE = "last_etl_run.txt"

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

def create_dw_tables():
    """Cria todas as tabelas do Data Warehouse."""
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Falha ao conectar ao DW para criar tabelas.")
        return False

    print("Verificando e criando tabelas do Data Warehouse...")
    for query in ALL_DDL_QUERIES:
        if not execute_sql(conn_dw, query):
            print(f"Erro ao criar tabela com a query:\n{query}")
            conn_dw.close()
            return False
    conn_dw.close()
    print("Todas as tabelas do DW verificadas/criadas com sucesso.")
    return True

def main_etl_process():
    print("Iniciando processo ETL para Caronaê DW...")

    # 1. Criar tabelas do DW (se não existirem)
    if not create_dw_tables():
        print("ETL abortado devido a falha na criação das tabelas do DW.")
        return

    # Obter a última data de execução para carga incremental
    last_run_date = get_last_etl_run_date()
    current_run_date = datetime.now() # Marcar a hora de início desta execução

    # 2. Executar ETL das Dimensões
    print("\n--- Iniciando ETL das Dimensões ---")
    # A dim_time geralmente só precisa ser carregada uma vez ou quando estender o período.
    etl_dim_time() # Descomente para carregar a dim_time
    
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

if __name__ == "__main__":
    main_etl_process()