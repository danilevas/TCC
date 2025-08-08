# etl_main.py
from datetime import datetime
import os
import sys

# Adiciona o diretório raiz do projeto ao PATH para importações relativas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dim_scripts.dim_date_etl import etl_dim_date
from dim_scripts.dim_hour_etl import etl_dim_hour
from dim_scripts.dim_user_etl import etl_dim_user
from dim_scripts.dim_neighborhood_etl import etl_dim_neighborhood
from dim_scripts.dim_hub_etl import etl_dim_hub
from dim_scripts.dim_request_status_etl import etl_dim_request_status
from dim_scripts.dim_ride_flags_etl import etl_dim_ride_flags

from fact_scripts.fact_ride_interaction_etl import etl_fact_ride_interaction

from utils import connect_to_db, insert_unknown_dim_member
from sql_queries import get_queries
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

def create_dw_tables(conn_dw):
    """
    Dropa todas as tabelas existentes no DW e as recria.
    Isso garante um ambiente limpo para cada execução completa do ETL.
    Retorna True em caso de sucesso, False em caso de falha.
    """
    print("Verificando e recriando tabelas do Data Warehouse...")

    # Retorna as queries de DDL baseado na configuração requisitada para as duas tabelas pré-populadas
    DROP_QUERIES, CREATE_QUERIES = get_queries()
    
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

# --- FUNÇÃO PARA INSERIR TODOS OS MEMBROS DESCONHECIDOS ---
def insert_all_unknown_dim_members(conn_dw):
    """
    Insere o membro 'Desconhecido' em todas as tabelas de dimensão.
    """
    print("\n--- Inserindo membros 'Desconhecidos' nas Dimensões ---")

    # --- dim_date ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_date em sql_queries.py
    dim_date_unknown_values = {
        'date_sk': -1,
        'full_date': '1900-01-01',
        'day_of_week': 0,
        'day_name': 'Desconhecido',
        'day_of_month': 0,
        'month': 0,
        'month_name': 'Desconhecido',
        'semester': 0,
        'period': 'Desconhecido',
        'year': 0
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_date', ['date_sk'], dim_date_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_date.")
        return False

    # --- dim_hour ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_hour em sql_queries.py
    dim_hour_unknown_values = {
        'hour_sk': -1,
        'hour_of_day': -1,
        'minute_of_hour': -1,
        'time_of_day_bucket': 'Desconhecido'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_hour', ['hour_sk'], dim_hour_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_hour.")
        return False

    # --- dim_user ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_user em sql_queries.py
    dim_user_unknown_values = {
        'user_sk': -1,
        'user_id': -1,
        'profile': 'Desconhecido',
        'course': 'Desconhecido',
        'phone_number': 'Desconhecido',
        'email': 'Desconhecido',
        'has_car': False,
        'car_model': 'Desconhecido',
        'car_color': '#000000',
        'car_plate': 'Desconhecido',

        'user_location': 'Desconhecido',
        'app_platform': 'Desconhecido',
        'app_version': 'Desconhecido',

        'is_banned': False,
        'institution_id': -1,
        'institution_name': 'Desconhecido',
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_user', ['user_sk'], dim_user_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_user.")
        return False

    # --- dim_neighborhood ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_neighborhood em sql_queries.py
    dim_neighborhood_unknown_values = {
        'neighborhood_sk': -1,
        'neighborhood_id': -1,
        'neighborhood_name': 'Desconhecido',
        'distance_to_fundao': 0.0, # Ou outro valor numérico padrão
        'zone_id': -1,
        'zone_name': 'Desconhecido',
        'zone_color': '#000000'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_neighborhood', ['neighborhood_sk'], dim_neighborhood_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_neighborhood.")
        return False

    # --- dim_hub ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_hub em sql_queries.py
    dim_hub_unknown_values = {
        'hub_sk': -1,
        'hub_id': -1,
        'hub_name': 'Desconhecido',
        'center': 'Desconhecido',
        'campus_id': -1,
        'campus_name': 'Desconhecido',
        'campus_color': '#000000',
        'institution_id': -1,
        'institution_name': 'Desconhecido'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_hub', ['hub_sk'], dim_hub_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_hub.")
        return False

    # --- dim_request_status ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_request_status em sql_queries.py
    dim_request_status_unknown_values = {
        'status_sk': -1,
        'status_name': 'Desconhecido'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_request_status', ['status_sk'], dim_request_status_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_request_status.")
        return False

    # --- dim_ride_flags ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_ride_flags em sql_queries.py
    dim_ride_flags_unknown_values = {
        'ride_flags_sk': -1,

        'has_requests': False,
        'is_routine_ride': False,
        'is_going_to_campus': False,
        'done': False,

        'is_routine_monday': False,
        'is_routine_tuesday': False,
        'is_routine_wednesday': False,
        'is_routine_thursday': False,
        'is_routine_friday': False,
        'is_routine_saturday': False,
        'is_routine_sunday': False,

        'ride_creation_request': False,
        'request_accepted': False,
        'request_refused': False,
        'request_pending': False,
        'request_quit': False,

        'flags_description': 'Desconhecido'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_ride_flags', ['ride_flags_sk'], dim_ride_flags_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_ride_flags.")
        return False

    print("--- Todos os membros 'Desconhecidos' inseridos com sucesso ---")
    return True

def main_etl_process(carga_completa):
    conn_oltp = None
    conn_dw = None
    try:
        print("Iniciando processo ETL para Caronaê DW...")

        # 0. Apaga o last_etl_run.txt se o parâmetro for True
        if carga_completa:
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
        if not create_dw_tables(conn_dw):
            print("ETL abortado devido a falha na criação/recriação das tabelas do DW.")
            return # Sai da função se as tabelas não puderem ser criadas

        # 2. Inserir TODOS os membros "Desconhecidos" nas Dimensões
        # Chame a nova função que encapsula todas as inserções
        if not insert_all_unknown_dim_members(conn_dw):
            print("Abortando ETL devido à falha na inserção de membros 'Desconhecidos'.")
            # conn_dw.close()
            return False
        
        # conn_dw.close() # Feche a conexão temporária usada apenas para inserções de membros desconhecidos

        # Obter a última data de execução para carga incremental
        last_run_date = get_last_etl_run_date()
        current_run_date = datetime.now() # Marcar a hora de início desta execução

        # 3. Executar ETL das Dimensões
        print("\n--- Iniciando ETL das Dimensões ---")

        # As dimensões temporais geralmente só precisam ser carregadas uma vez ou quando estender o período
        etl_dim_date()
        etl_dim_hour()
        
        # A dim_ride_flags só precisa ser carregada uma vez
        etl_dim_ride_flags()
        
        if not etl_dim_user(): print("ETL DimUser falhou.")
        if not etl_dim_neighborhood(): print("ETL DimNeighborhood falhou.")
        if not etl_dim_hub(): print("ETL DimHub falhou.")
        if not etl_dim_request_status(): print("ETL DimRequestStatus falhou.")
        print("--- ETL das Dimensões Concluído ---")

        # 4. Executar ETL dos Fatos (Carga Incremental)
        print("\n--- Iniciando ETL dos Fatos (Incremental) ---")
        # Passar a data de last_run_date como string para a função
        if not etl_fact_ride_interaction(last_run_date.strftime("%Y-%m-%d %H:%M:%S.%f")): print("ETL FactRideInteraction falhou.")
        print("--- ETL dos Fatos Concluído ---")

        # 5. Atualizar a marca d'água da última execução
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
    main_etl_process(carga_completa=True)