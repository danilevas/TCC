# fact_scripts/fact_ride_request_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, get_last_etl_run_date_se_houver
from psycopg2.extras import execute_batch

def etl_fact_ride_request(last_etl_run_date_str=None):
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL FactRideRequest abortado.")
        return False

    try:
        # Obter o último timestamp do DW para carga incremental
        last_etl_run_date = get_last_etl_run_date_se_houver(conn_dw, last_etl_run_date_str, 'fact_ride_request')

        print(f"\n\n--- FACT_RIDE_REQUEST --- \n\n")
        print(f"Extraindo dados de ride_user a partir de: {last_etl_run_date}")

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
        WHERE status <> 'driver' -- já tirando os pedidos de criação de carona
        AND (created_at >= '{last_etl_run_date}' OR updated_at >= '{last_etl_run_date}');
        """
        requests_data = pd.read_sql(query_extract_ride_users, conn_oltp)
        print(f"Extraídas {len(requests_data)} interações de carona para processamento incremental.")

        if requests_data.empty:
            print("Nenhum dado novo ou atualizado para processar na fact_ride_request.")
            return True

        # 2. Transformação (Transform)

        # Gerar chaves de data/hora a partir da data e hora em que o pedido foi criado (da coluna created_at)
        requests_data['creation_date_sk'] = pd.to_datetime(requests_data['created_at']).dt.strftime('%Y%m%d').astype('Int64')
        requests_data['creation_hour_sk'] = pd.to_datetime(requests_data['created_at']).dt.strftime('%H%M').astype('Int64')

        # Gerar chaves de data/hora a partir da data e hora em que o pedido foi atualizado (da coluna updated_at)
        requests_data['update_date_sk'] = pd.to_datetime(requests_data['updated_at']).dt.strftime('%Y%m%d').astype('Int64')
        requests_data['update_hour_sk'] = pd.to_datetime(requests_data['updated_at']).dt.strftime('%H%M').astype('Int64')

        # -------------------- STATUS --------------------

        # Criar as colunas booleanas de status
        requests_data['request_accepted'] = (requests_data['status'] == 'accepted')
        requests_data['request_refused'] = (requests_data['status'] == 'refused')
        requests_data['request_pending'] = (requests_data['status'] == 'pending')
        requests_data['request_quit'] = (requests_data['status'] == 'quit')

        # -------------------- SKs --------------------

        print(f"\n\n{len(requests_data)}\n\n")

        # Obter chaves substitutas das dimensões
        fact_ride_map = pd.read_sql("SELECT ride_id, ride_sk FROM fact_ride;", conn_dw)
        dim_user_map = pd.read_sql("SELECT user_id, user_sk FROM dim_user;", conn_dw)
        dim_request_status_map = pd.read_sql("SELECT status_name, status_sk FROM dim_request_status;", conn_dw)

        # Fazendo os merges
        requests_data = requests_data.merge(dim_user_map, left_on='user_id', right_on='user_id', how='left')
        requests_data = requests_data.merge(dim_request_status_map, left_on='status', right_on='status_name', how='left')
        requests_data = requests_data.merge(fact_ride_map, left_on='ride_id', right_on='ride_id', how='left')

        print(f"\n\n{len(requests_data)}\n\n")

        # Convertendo para Int64 essas sks
        requests_data['ride_sk'] = pd.to_numeric(requests_data['ride_sk'], errors='coerce').astype('Int64')
        requests_data['user_sk'] = pd.to_numeric(requests_data['user_sk'], errors='coerce').astype('Int64')
        requests_data['status_sk'] = pd.to_numeric(requests_data['status_sk'], errors='coerce').astype('Int64')

        # Tratamento de SKs nulas após o merge (se houver IDs que não foram mapeados - assumindo -1 para sk desconhecido)
        requests_data['ride_sk'].fillna(-1, inplace=True)
        requests_data['user_sk'].fillna(-1, inplace=True)
        requests_data['status_sk'].fillna(-1, inplace=True)

        # Limpar colunas temporárias e selecionar as finais
        final_fact_columns = [
            'ride_user_id', 'ride_sk', 'user_sk', 'status_sk',
            'creation_date_sk', 'creation_hour_sk', 'update_date_sk', 'update_hour_sk',
            'request_accepted', 'request_refused', 'request_pending', 'request_quit'
        ]

        # Garantir que as colunas SK não são nulas
        requests_data.dropna(
            subset=['ride_sk', 'user_sk', 'status_sk', 'creation_date_sk', 'creation_hour_sk', 'update_date_sk', 'update_hour_sk'],
            inplace=True
        )

        fact_data_to_load = requests_data[final_fact_columns]
        fact_data_to_load = fact_data_to_load.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print(f"Carregando {len(fact_data_to_load)} registros na fact_ride_request...")

        insert_or_update_query = """
        INSERT INTO fact_ride_request (
            ride_user_id, ride_sk, user_sk, status_sk,
            creation_date_sk, creation_hour_sk, update_date_sk, update_hour_sk,
            request_accepted, request_refused, request_pending, request_quit
        ) VALUES (
            %(ride_user_id)s, %(ride_sk)s, %(user_sk)s, %(status_sk)s,
            %(creation_date_sk)s, %(creation_hour_sk)s, %(update_date_sk)s, %(update_hour_sk)s,
            %(request_accepted)s, %(request_refused)s, %(request_pending)s, %(request_quit)s
        ) ON CONFLICT (ride_user_id) DO UPDATE SET
            ride_sk = EXCLUDED.ride_sk,
            user_sk = EXCLUDED.user_sk,
            status_sk = EXCLUDED.status_sk,
            
            creation_date_sk = EXCLUDED.creation_date_sk,
            creation_hour_sk = EXCLUDED.creation_hour_sk,

            update_date_sk = EXCLUDED.update_date_sk,
            update_hour_sk = EXCLUDED.update_hour_sk,

            request_accepted = EXCLUDED.request_accepted,
            request_refused = EXCLUDED.request_refused,
            request_pending = EXCLUDED.request_pending,
            request_quit = EXCLUDED.request_quit
        """
        data_to_load_dicts = fact_data_to_load.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load_dicts)
        conn_dw.commit()
        print("Carga da fact_ride_request concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da FactRideRequest: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()