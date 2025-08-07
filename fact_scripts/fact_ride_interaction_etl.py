# fact_scripts/fact_ride_interaction_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from dim_scripts.dim_ride_flags_etl import derive_and_lookup_flags
from utils import connect_to_db, get_last_etl_run_date_se_houver
from psycopg2.extras import execute_batch

def etl_fact_ride_interaction(last_etl_run_date_str=None):
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL FactRideInteraction abortado.")
        return False

    try:
        # Obter o último timestamp do DW para carga incremental
        last_etl_run_date = get_last_etl_run_date_se_houver(conn_dw, last_etl_run_date_str, 'fact_ride_interaction')

        print(f"Extraindo dados de ride_user. A partir de: {last_etl_run_date}")

        # 1. Extração (Extract)
        query_extract_ride_users = f"""
        SELECT
            ru.id AS ride_user_id,
            ru.ride_id,
            ru.user_id,
            ru.created_at,
            ru.updated_at,
            ru.status,
            r.neighborhood AS neighborhood_name, -- Para depois pegarmos o neighborhood_sk
            r.going AS is_going_to_campus, -- Renomear para clareza
            r.hub AS hub_name, -- Para depois pegarmos o hub_sk
            r.week_days,
            r.done
        FROM ride_user ru
        JOIN rides r ON ru.ride_id = r.id
        WHERE ru.created_at >= '{last_etl_run_date}' OR ru.updated_at >= '{last_etl_run_date}';
        """
        ride_users_data = pd.read_sql(query_extract_ride_users, conn_oltp)
        print(f"Extraídas {len(ride_users_data)} interações de carona para processamento incremental.")

        if ride_users_data.empty:
            print("Nenhum dado novo ou atualizado para processar na fact_ride_interaction.")
            return True

        # 1.5. Tratamento de tipos

        # Convertendo as colunas booleanas
        ride_users_data['is_going_to_campus'] = ride_users_data['is_going_to_campus'].fillna(False).astype(bool)

        # 2. Transformação (Transform)

        # Gerar chaves de data/hora a partir da data e hora em que o pedido foi criado (da coluna created_at)
        ride_users_data['creation_date_sk'] = pd.to_datetime(ride_users_data['created_at']).dt.strftime('%Y%m%d').astype('Int64')
        ride_users_data['creation_hour_sk'] = pd.to_datetime(ride_users_data['created_at']).dt.strftime('%H%M').astype('Int64')

        # Gerar chaves de data/hora a partir da data e hora em que o pedido foi atualizado (da coluna updated_at)
        ride_users_data['update_date_sk'] = pd.to_datetime(ride_users_data['updated_at']).dt.strftime('%Y%m%d').astype('Int64')
        ride_users_data['update_hour_sk'] = pd.to_datetime(ride_users_data['updated_at']).dt.strftime('%H%M').astype('Int64')

        # Determinar se é carona de rotina
        ride_users_data['is_routine_ride'] = (ride_users_data['week_days'].notna())

        # Converter a coluna is_routine_ride para Python booleano (True/False)
        ride_users_data['is_routine_ride'] = ride_users_data['is_routine_ride'].fillna(False).astype(bool)

        # -------------------- FLAGS --------------------

        print("Derivando flags e buscando ride_flags_sk...")

        # A coluna 'is_routine_ride' do OLTP, 'week_days', 'description' e 'done'
        # são passadas para 'derive_and_lookup_flags' através do row_oltp.
        ride_users_data['ride_flags_sk'] = ride_users_data.apply(derive_and_lookup_flags, axis=1)
        
        # Transformando a coluna em Int64
        ride_users_data['ride_flags_sk'] = pd.to_numeric(ride_users_data['ride_flags_sk'], errors='coerce').astype('Int64')
        print("Flags e ride_flags_sk processados.")

        # -------------------- STATUS --------------------

        # Criar as colunas booleanas de status
        ride_users_data['is_driver_interaction'] = (ride_users_data['status'] == 'driver')
        ride_users_data['is_passenger_request'] = (ride_users_data['status'].isin(['pending', 'accepted', 'refused', 'quit']))
        ride_users_data['request_accepted'] = (ride_users_data['status'] == 'accepted')
        ride_users_data['request_refused'] = (ride_users_data['status'] == 'refused')
        ride_users_data['request_pending'] = (ride_users_data['status'] == 'pending')
        ride_users_data['request_quit'] = (ride_users_data['status'] == 'quit')

        # -------------------- SKs --------------------

        # Obter chaves substitutas das dimensões
        dim_ride_map = pd.read_sql("SELECT ride_id, ride_sk FROM dim_ride;", conn_dw)
        dim_user_map = pd.read_sql("SELECT user_id, user_sk FROM dim_user;", conn_dw)
        dim_request_status_map = pd.read_sql("SELECT status_name, status_sk FROM dim_request_status;", conn_dw)
        dim_neighborhood_map = pd.read_sql("SELECT neighborhood_name, neighborhood_sk FROM dim_neighborhood;", conn_dw)
        dim_hub_map = pd.read_sql("SELECT hub_name, hub_sk FROM dim_hub;", conn_dw)

        # Fazendo os merges
        ride_users_data = ride_users_data.merge(dim_user_map, left_on='user_id', right_on='user_id', how='left')
        ride_users_data = ride_users_data.merge(dim_request_status_map, left_on='status', right_on='status_name', how='left')
        ride_users_data = ride_users_data.merge(dim_ride_map, left_on='ride_id', right_on='ride_id', how='left')
        ride_users_data = ride_users_data.merge(dim_neighborhood_map, left_on='neighborhood_name', right_on='neighborhood_name', how='left')
        ride_users_data = ride_users_data.merge(dim_hub_map, left_on='hub_name', right_on='hub_name', how='left')

        # Convertendo para Int64 essas sks
        ride_users_data['ride_sk'] = pd.to_numeric(ride_users_data['ride_sk'], errors='coerce').astype('Int64')
        ride_users_data['user_sk'] = pd.to_numeric(ride_users_data['user_sk'], errors='coerce').astype('Int64')
        ride_users_data['status_sk'] = pd.to_numeric(ride_users_data['status_sk'], errors='coerce').astype('Int64')
        ride_users_data['neighborhood_sk'] = pd.to_numeric(ride_users_data['neighborhood_sk'], errors='coerce').astype('Int64')
        ride_users_data['hub_sk'] = pd.to_numeric(ride_users_data['hub_sk'], errors='coerce').astype('Int64')

        # Tratamento de SKs nulas após o merge (se houver IDs que não foram mapeados - assumindo -1 para sk desconhecido)
        ride_users_data['ride_sk'].fillna(-1, inplace=True)
        ride_users_data['user_sk'].fillna(-1, inplace=True)
        ride_users_data['status_sk'].fillna(-1, inplace=True)
        ride_users_data['neighborhood_sk'].fillna(-1, inplace=True)
        ride_users_data['hub_sk'].fillna(-1, inplace=True)

        # Limpar colunas temporárias e selecionar as finais
        final_fact_columns = [
            'ride_user_id', 'ride_sk', 'user_sk', 'status_sk', 'neighborhood_sk', 'hub_sk', 'ride_flags_sk',
            'creation_date_sk', 'creation_hour_sk', 'update_date_sk', 'update_hour_sk',
            'is_driver_interaction', 'is_passenger_request', 'request_accepted',
            'request_refused', 'request_pending', 'request_quit',
            'created_at', 'updated_at'
        ]

        # Garantir que as colunas SK não são nulas
        ride_users_data.dropna(subset=['ride_sk', 'user_sk', 'status_sk', 'neighborhood_sk', 'hub_sk', 'ride_flags_sk',
                                       'creation_date_sk', 'creation_hour_sk', 'update_date_sk', 'update_hour_sk'], inplace=True)

        fact_data_to_load = ride_users_data[final_fact_columns]
        fact_data_to_load = fact_data_to_load.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print(f"Carregando {len(fact_data_to_load)} registros na fact_ride_interaction...")

        insert_or_update_query = """
        INSERT INTO fact_ride_interaction (
            ride_user_id, ride_sk, user_sk, status_sk, neighborhood_sk, hub_sk, ride_flags_sk,
            creation_date_sk, creation_hour_sk, update_date_sk, update_hour_sk,
            is_driver_interaction, is_passenger_request, request_accepted,
            request_refused, request_pending, request_quit,
            created_at, updated_at
        ) VALUES (
            %(ride_user_id)s, %(ride_sk)s, %(user_sk)s, %(status_sk)s, %(neighborhood_sk)s, %(hub_sk)s, %(ride_flags_sk)s,
            %(creation_date_sk)s, %(creation_hour_sk)s, %(update_date_sk)s, %(update_hour_sk)s,
            %(is_driver_interaction)s, %(is_passenger_request)s, %(request_accepted)s,
            %(request_refused)s, %(request_pending)s, %(request_quit)s,
            %(created_at)s, %(updated_at)s
        ) ON CONFLICT (ride_user_id) DO UPDATE SET
            ride_sk = EXCLUDED.ride_sk,
            user_sk = EXCLUDED.user_sk,
            status_sk = EXCLUDED.status_sk,
            neighborhood_sk = EXCLUDED.neighborhood_sk,
            hub_sk = EXCLUDED.hub_sk,
            ride_flags_sk = EXCLUDED.ride_flags_sk,
            
            creation_date_sk = EXCLUDED.creation_date_sk,
            creation_hour_sk = EXCLUDED.creation_hour_sk,

            update_date_sk = EXCLUDED.update_date_sk,
            update_hour_sk = EXCLUDED.update_hour_sk,

            is_driver_interaction = EXCLUDED.is_driver_interaction,
            is_passenger_request = EXCLUDED.is_passenger_request,
            request_accepted = EXCLUDED.request_accepted,
            request_refused = EXCLUDED.request_refused,
            request_pending = EXCLUDED.request_pending,
            request_quit = EXCLUDED.request_quit,

            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
        """
        data_to_load_dicts = fact_data_to_load.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load_dicts)
        conn_dw.commit()
        print("Carga da fact_ride_interaction concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da FactRideInteraction: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()