# etl_main.py
from datetime import datetime
import os
import sys
import warnings

warnings.filterwarnings('ignore')

# Adiciona o diretório raiz do projeto ao PATH para importações relativas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dim_scripts.dim_date_etl import etl_dim_date
from dim_scripts.dim_hour_etl import etl_dim_hour
from dim_scripts.dim_user_etl import etl_dim_user
from dim_scripts.dim_place_etl import etl_dim_place
from dim_scripts.dim_request_status_etl import etl_dim_request_status
from dim_scripts.dim_routine_etl import etl_dim_routine
from dim_scripts.dim_ride_flags_etl import etl_dim_ride_flags

from fact_scripts.fact_ride_etl import etl_fact_ride
from fact_scripts.fact_ride_request_etl import etl_fact_ride_request

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

def create_or_reset_dw_tables(conn_dw, clear_data=True):
    """
    Gerencia as tabelas do Data Warehouse.

    Se clear_data=True: Limpa todos os dados usando TRUNCATE (carga completa).
                        TRUNCATE é muito mais rápido que DROP/CREATE pois preserva
                        a estrutura, índices e constraints.
    Se clear_data=False: Apenas cria tabelas se não existirem (carga incremental).

    Retorna True em caso de sucesso, False em caso de falha.
    """
    if clear_data:
        print("Limpando dados do Data Warehouse (TRUNCATE)...")
    else:
        print("Verificando e criando tabelas do Data Warehouse (se necessário)...")

    # Retorna as queries de DDL
    DROP_QUERIES, TRUNCATE_QUERIES, CREATE_QUERIES = get_queries()

    try:
        cur = conn_dw.cursor()

        # --- PASSO 1: CRIAR TODAS AS TABELAS (sempre executa primeiro, usando IF NOT EXISTS) ---
        print("Verificando se tabelas existem e criando se necessário...")
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

        # --- PASSO 2: LIMPAR DADOS (apenas se clear_data=True) ---
        if clear_data:
            print("\nLimpando dados das tabelas (TRUNCATE)...")
            # Itera sobre as queries de TRUNCATE em ordem inversa de dependência
            for query in TRUNCATE_QUERIES:
                try:
                    cur.execute(query)
                    conn_dw.commit() # Commita cada TRUNCATE
                    print(f"  - Query TRUNCATE executada com sucesso: {query.splitlines()[0].strip()}...")
                except Exception as e:
                    conn_dw.rollback() # Em caso de erro, desfaz a transação atual
                    # Avisa, mas continua, pois a tabela pode estar vazia
                    print(f"  - Aviso: Erro ao truncar tabela: {e}. Query: {query.splitlines()[0].strip()}...")
            print("Dados limpos com sucesso.")

        print("Todas as tabelas do DW verificadas/preparadas com sucesso.")
        return True # Retorna True se tudo ocorrer bem

    except Exception as e:
        print(f"Erro fatal ao preparar tabelas do DW: {e}")
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
        'academic_affiliation': 'Desconhecido',
        'course': 'Desconhecido',
        'has_car': False,
        'car_model': 'Desconhecido',

        'is_banned': False,
        'institution_id': -1,
        'institution_name': 'Desconhecido',
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_user', ['user_sk'], dim_user_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_user.")
        return False

    # --- dim_place ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_place em sql_queries.py
    dim_place_unknown_values = {
        'place_sk': -1,
        'place_name': 'Desconhecido',
        'place_type': 'Desconhecido',

        'hub_id': -1,
        'center': 'Desconhecido',
        'campus_id': -1,
        'campus_name': 'Desconhecido',
        'institution_id': -1,
        'institution_name': 'Desconhecido',

        'neighborhood_id': -1,
        'zone_id': -1,
        'zone_name' : 'Desconhecido'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_place', ['place_sk'], dim_place_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_place.")
        return False

    # --- dim_request_status ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_request_status em sql_queries.py
    dim_request_status_unknown_values = {
        'status_sk': -1,
        'status_name': 'Desconhecido',
        'status_description': 'Status desconhecido ou não identificado'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_request_status', ['status_sk'], dim_request_status_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_request_status.")
        return False

    # --- dim_routine ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_routine em sql_queries.py
    # Membro "Não Aplicável" (caronas não-rotineiras)
    dim_routine_not_applicable_values = {
        'routine_sk': 0,
        'routine_id': 0,
        'repeats_until': None,
        'is_routine_monday': False,
        'is_routine_tuesday': False,
        'is_routine_wednesday': False,
        'is_routine_thursday': False,
        'is_routine_friday': False,
        'is_routine_saturday': False,
        'is_routine_sunday': False,
        'routine_days_description': 'Não Aplicável'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_routine', ['routine_sk'], dim_routine_not_applicable_values):
        print("Falha ao inserir membro 'Não Aplicável' para dim_routine.")
        return False

    # Membro "Desconhecido" (erro/dado faltante)
    dim_routine_unknown_values = {
        'routine_sk': -1,
        'routine_id': -1,
        'repeats_until': None,
        'is_routine_monday': False,
        'is_routine_tuesday': False,
        'is_routine_wednesday': False,
        'is_routine_thursday': False,
        'is_routine_friday': False,
        'is_routine_saturday': False,
        'is_routine_sunday': False,
        'routine_days_description': 'Desconhecido'
    }
    if not insert_unknown_dim_member(conn_dw, 'dim_routine', ['routine_sk'], dim_routine_unknown_values):
        print("Falha ao inserir membro 'Desconhecido' para dim_routine.")
        return False

    # --- dim_ride_flags ---
    # IMPORTANTE: Alinhe estas colunas e valores EXATAMENTE com a sua DDL de dim_ride_flags em sql_queries.py
    dim_ride_flags_unknown_values = {
        'ride_flags_sk': -1,
        'is_going_to_campus': False,
        'done': False,
        'deleted': False,
        'flags_description': 'Desconhecido',
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

        # 1. Criar/Verificar tabelas do DW e limpar dados se necessário
        # Em carga completa: TRUNCATE (limpa dados, preserva estrutura)
        # Em carga incremental: apenas CREATE IF NOT EXISTS (preserva dados)
        if not create_or_reset_dw_tables(conn_dw, clear_data=carga_completa):
            print("ETL abortado devido a falha na preparação das tabelas do DW.")
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
        print("\n---------- Iniciando ETL das Dimensões ----------")

        # As dimensões temporais geralmente só precisam ser carregadas uma vez ou quando estender o período
        etl_dim_date()
        etl_dim_hour()

        if not etl_dim_user(): print("ETL DimUser falhou.")
        if not etl_dim_place(): print("ETL DimPlace falhou.")
        if not etl_dim_request_status(): print("ETL DimRequestStatus falhou.")
        if not etl_dim_routine(): print("ETL DimRoutine falhou.")

        # A dim_ride_flags só precisa ser carregada uma vez
        etl_dim_ride_flags()

        print("\n---------- ETL das Dimensões Concluído ----------")

        # 4. Executar ETL dos Fatos (Carga Incremental)
        print("\n--- Iniciando ETL dos Fatos (Incremental) ---")
        # Passar a data de last_run_date como string para a função
        if not etl_fact_ride(last_run_date.strftime("%Y-%m-%d %H:%M:%S.%f")): print("ETL FactRide falhou.")
        if not etl_fact_ride_request(last_run_date.strftime("%Y-%m-%d %H:%M:%S.%f")): print("ETL FactRideRequest falhou.")
        print("\n--- ETL dos Fatos Concluído ---")

        # 5. Atualizar a marca d'água da última execução
        set_last_etl_run_date(current_run_date)
        print(f"\nProcesso ETL concluído com *SUCESSO*\nÚltima execução registrada em: {current_run_date}\n")
    
    except Exception as e:
        # Captura qualquer exceção não tratada e a imprime
        print(f"\nOcorreu um *ERRO CRÍTICO* inesperado no processo ETL: {e}")
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