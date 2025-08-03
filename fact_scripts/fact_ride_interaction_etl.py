# fact_scripts/fact_ride_interaction_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, get_last_etl_run_date_se_houver
from psycopg2.extras import execute_batch

def etl_fact_ride_interaction(last_etl_run_date_str=None):
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL FatoRideInteraction abortado.")
        return False

    try:
        # Obter o último timestamp do DW para carga incremental
        last_etl_run_date = get_last_etl_run_date_se_houver(conn_dw, last_etl_run_date_str)

        print(f"Extraindo dados de ride_user. A partir de: {last_etl_run_date}")

        # 1. Extração (Extract)
        query_extract_ride_users = f"""
        SELECT
            id AS ride_user_id,
            ride_id,
            user_id,
            created_at,
            updated_at,
            status
        FROM ride_user
        WHERE created_at >= '{last_etl_run_date}' OR updated_at >= '{last_etl_run_date}';
        """
        ride_users_data = pd.read_sql(query_extract_ride_users, conn_oltp)
        print(f"Extraídas {len(ride_users_data)} interações de carona para processamento incremental.")

        if ride_users_data.empty:
            print("Nenhum dado novo ou atualizado para processar na fato_ride_interaction.")
            return True

        # 2. Transformação (Transform)
        ride_users_data['date_sk'] = pd.to_datetime(ride_users_data['created_at']).dt.strftime('%Y%m%d').astype(int)
        ride_users_data['hour_sk'] = pd.to_datetime(ride_users_data['created_at']).dt.strftime('%H%M').astype(int)

        # Criar as colunas booleanas de status
        ride_users_data['is_driver_interaction'] = (ride_users_data['status'] == 'driver')
        ride_users_data['is_passenger_request'] = (ride_users_data['status'].isin(['pending', 'accepted', 'refused', 'quit']))
        ride_users_data['request_accepted'] = (ride_users_data['status'] == 'accepted')
        ride_users_data['request_refused'] = (ride_users_data['status'] == 'refused')
        ride_users_data['request_pending'] = (ride_users_data['status'] == 'pending')
        ride_users_data['request_quit'] = (ride_users_data['status'] == 'quit')

        # Obter chaves substitutas das dimensões
        dim_ride_map = pd.read_sql("SELECT ride_id, ride_sk FROM fato_carona;", conn_dw)
        dim_user_map = pd.read_sql("SELECT user_id, user_sk FROM dim_user;", conn_dw)
        dim_request_status_map = pd.read_sql("SELECT status_name, status_sk FROM dim_request_status;", conn_dw)

        # Fazendo os merges
        ride_users_data = ride_users_data.merge(dim_user_map, left_on='user_id', right_on='user_id', how='left')
        ride_users_data = ride_users_data.merge(dim_request_status_map, left_on='status', right_on='status_name', how='left')
        ride_users_data = ride_users_data.merge(dim_ride_map, left_on='ride_id', right_on='ride_id', how='left')

        # Convertendo para Int64 essas sks
        ride_users_data['ride_sk'] = pd.to_numeric(ride_users_data['ride_sk'], errors='coerce').astype('Int64')
        ride_users_data['user_sk'] = pd.to_numeric(ride_users_data['user_sk'], errors='coerce').astype('Int64')
        ride_users_data['status_sk'] = pd.to_numeric(ride_users_data['status_sk'], errors='coerce').astype('Int64')

        # Tratamento de SKs nulas após o merge (se houver IDs que não foram mapeados - assumindo -1 para sk desconhecido)
        ride_users_data['ride_sk'].fillna(-1, inplace=True)
        ride_users_data['user_sk'].fillna(-1, inplace=True)
        ride_users_data['status_sk'].fillna(-1, inplace=True)

        # Limpar colunas temporárias e selecionar as finais
        final_fact_columns = [
            'ride_user_id', 'ride_sk', 'user_sk', 'date_sk', 'hour_sk', 'status_sk',
            'is_driver_interaction', 'is_passenger_request', 'request_accepted',
            'request_refused', 'request_pending', 'request_quit',
            'created_at', 'updated_at'
        ]

        # Garantir que as colunas SK não são nulas
        ride_users_data.dropna(subset=['user_sk', 'date_sk', 'hour_sk', 'status_sk'], inplace=True)

        fact_data_to_load = ride_users_data[final_fact_columns]
        fact_data_to_load = fact_data_to_load.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print(f"Carregando {len(fact_data_to_load)} registros na fato_ride_interaction...")

        insert_or_update_query = """
        INSERT INTO fato_ride_interaction (
            ride_user_id, ride_sk, user_sk, date_sk, hour_sk, status_sk,
            is_driver_interaction, is_passenger_request, request_accepted,
            request_refused, request_pending, request_quit,
            created_at, updated_at
        ) VALUES (
            %(ride_user_id)s, %(ride_sk)s, %(user_sk)s, %(date_sk)s, %(hour_sk)s, %(status_sk)s,
            %(is_driver_interaction)s, %(is_passenger_request)s, %(request_accepted)s,
            %(request_refused)s, %(request_pending)s, %(request_quit)s,
            %(created_at)s, %(updated_at)s
        ) ON CONFLICT (ride_user_id) DO UPDATE SET
            ride_sk = EXCLUDED.ride_sk,
            user_sk = EXCLUDED.user_sk,
            date_sk = EXCLUDED.date_sk,
            hour_sk = EXCLUDED.hour_sk,
            status_sk = EXCLUDED.status_sk,
            is_driver_interaction = EXCLUDED.is_driver_interaction,
            is_passenger_request = EXCLUDED.is_passenger_request,
            request_accepted = EXCLUDED.request_accepted,
            request_refused = EXCLUDED.request_refused,
            request_pending = EXCLUDED.request_pending,
            request_quit = EXCLUDED.request_quit,
            updated_at = EXCLUDED.updated_at
        """
        data_to_load_dicts = fact_data_to_load.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load_dicts)
        conn_dw.commit()
        print("Carga da fato_ride_interaction concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da FatoRideInteraction: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()