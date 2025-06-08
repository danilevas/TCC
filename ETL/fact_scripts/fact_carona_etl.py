# fact_scripts/fact_carona_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, execute_sql, get_latest_timestamp
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch

def etl_fact_carona(last_etl_run_date_str=None):
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL FatoCarona abortado.")
        return False

    try:
        # Obter o último timestamp do DW para carga incremental
        # Se last_etl_run_date_str não for fornecido, tenta buscar no DW
        if not last_etl_run_date_str:
            last_etl_run_date = get_latest_timestamp(conn_dw, 'fato_carona', 'updated_at')
            if last_etl_run_date is None:
                last_etl_run_date = datetime(2000, 1, 1) # Data bem antiga para primeira carga
            else:
                # Adicionar um pequeno buffer para pegar alterações que podem ter ocorrido na borda
                last_etl_run_date -= timedelta(minutes=5)
        else:
            last_etl_run_date = datetime.strptime(last_etl_run_date_str, "%Y-%m-%d %H:%M:%S.%f")

        print(f"Extraindo dados de caronas (rides) e ride_user. A partir de: {last_etl_run_date}")

        # 1. Extração (Extract) dos dados incrementais do OLTP
        # JOIN com ride_user para garantir que pegamos o driver_id associado à carona
        # e com messages para contar as mensagens (se houver uma tabela de mensagens)
        query_extract_rides = f"""
        SELECT
            r.id AS ride_id,
            r.created_at,
            r.updated_at,
            r.slots,
            r.description,
            r.going AS is_going_to_campus, -- Renomear para clareza
            r.done,
            r.week_days, -- Para determinar se é rotina
            r.repeats_until, -- Para determinar se é rotina
            r.hub AS hub_id,
            r.myzone AS zone_id, -- Corrigido para 'myzone' conforme seu PDF
            r.neighborhood AS neighborhood_id,
            ru_driver.user_id AS driver_id, -- Obter o ID do motorista
            COALESCE(message_counts.num_messages, 0) AS messages_count
        FROM rides r
        JOIN ride_user ru_driver ON r.id = ru_driver.ride_id AND ru_driver.status = 'driver'
        LEFT JOIN (
            SELECT ride_id, COUNT(*) AS num_messages
            FROM messages
            GROUP BY ride_id
        ) AS message_counts ON r.id = message_counts.ride_id
        WHERE r.created_at >= '{last_etl_run_date}' OR r.updated_at >= '{last_etl_run_date}';
        """
        rides_data = pd.read_sql(query_extract_rides, conn_oltp)

        # Extrair dados de ride_user para agregação de status
        # Filtrar por updated_at ou created_at para pegar apenas os pedidos recentes ou atualizados
        query_extract_ride_users_for_aggregation = f"""
        SELECT
            ride_id,
            status
        FROM ride_user
        WHERE created_at >= '{last_etl_run_date}' OR updated_at >= '{last_etl_run_date}';
        """
        ride_users_agg_data = pd.read_sql(query_extract_ride_users_for_aggregation, conn_oltp)

        # 2. Transformação (Transform)
        print(f"Extraídas {len(rides_data)} caronas para processamento incremental.")

        if rides_data.empty and ride_users_agg_data.empty:
            print("Nenhum dado novo ou atualizado para processar na fato_carona.")
            return True # Não há dados para carregar, mas não é um erro

        # Gerar chaves de data/hora a partir das datas de criação (carona_created_at)
        rides_data['date_sk'] = pd.to_datetime(rides_data['created_at']).dt.strftime('%Y%m%d').astype(int)
        rides_data['time_sk'] = pd.to_datetime(rides_data['created_at']).dt.strftime('%H%M').astype(int)

        # Determinar se é carona de rotina
        rides_data['is_routine_ride'] = (rides_data['week_days'].notna()) | (rides_data['repeats_until'].notna())

        # Agregar métricas de requests
        # Usar pivot_table para garantir que todos os status possíveis são colunas
        if not ride_users_agg_data.empty:
            requests_summary = ride_users_agg_data.pivot_table(
                index='ride_id',
                columns='status',
                aggfunc='size',
                fill_value=0
            )
            # Renomear e garantir que todas as colunas de status existam (mesmo que com 0)
            status_columns = ['pending', 'accepted', 'refused', 'quit', 'driver']
            for col in status_columns:
                if col not in requests_summary.columns:
                    requests_summary[col] = 0
            
            requests_summary.rename(columns={
                'pending': 'pending_requests_count',
                'accepted': 'accepted_requests_count',
                'refused': 'refused_requests_count',
                'quit': 'quit_requests_count',
                'driver': 'driver_creation_events_agg' # É o evento de criação da carona pelo motorista
            }, inplace=True)
            
            rides_data = rides_data.merge(requests_summary, how='left', left_on='ride_id', right_index=True)
        else: # Se não houver dados de ride_user para agregar
            rides_data['pending_requests_count'] = 0
            rides_data['accepted_requests_count'] = 0
            rides_data['refused_requests_count'] = 0
            rides_data['quit_requests_count'] = 0
            rides_data['driver_creation_events_agg'] = 0 # Adicione esta também

        rides_data['requests_count'] = rides_data[['pending_requests_count', 'accepted_requests_count', 'refused_requests_count', 'quit_requests_count']].sum(axis=1)

        # Tratar NAs após o merge e antes da seleção final
        rides_data.fillna({
            'pending_requests_count': 0, 'accepted_requests_count': 0,
            'refused_requests_count': 0, 'quit_requests_count': 0,
            'requests_count': 0, 'messages_count': 0,
            'description': None, 'is_going_to_campus': False, 'slots': 0, 'is_routine_ride': False,
            'driver_creation_events_agg': 0
        }, inplace=True)
        
        # Converter booleanos para Python booleano (True/False)
        rides_data['is_going_to_campus'] = rides_data['is_going_to_campus'].astype(bool)
        rides_data['is_routine_ride'] = rides_data['is_routine_ride'].astype(bool)

        # Obter chaves substitutas das dimensões já carregadas
        # Otimização: Carregar mapas de SKs uma vez
        dim_user_map = pd.read_sql("SELECT user_id, user_sk FROM dim_user;", conn_dw)
        dim_zone_map = pd.read_sql("SELECT zone_id, zone_sk FROM dim_zone;", conn_dw)
        dim_neighborhood_map = pd.read_sql("SELECT neighborhood_id, neighborhood_sk FROM dim_neighborhood;", conn_dw)
        dim_hub_map = pd.read_sql("SELECT hub_id, hub_sk FROM dim_hub;", conn_dw)

        rides_data = rides_data.merge(dim_user_map, left_on='driver_id', right_on='user_id', how='left')
        rides_data.rename(columns={'user_sk': 'driver_user_sk'}, inplace=True)
        
        rides_data = rides_data.merge(dim_zone_map, left_on='zone_id', right_on='zone_id', how='left')
        rides_data.rename(columns={'zone_sk': 'zone_sk_mapped'}, inplace=True) # Renomear para evitar conflito
        rides_data['zone_sk'] = rides_data['zone_sk_mapped'] # Atualizar a coluna final

        rides_data = rides_data.merge(dim_neighborhood_map, left_on='neighborhood_id', right_on='neighborhood_id', how='left')
        rides_data.rename(columns={'neighborhood_sk': 'neighborhood_sk_mapped'}, inplace=True)
        rides_data['neighborhood_sk'] = rides_data['neighborhood_sk_mapped']

        rides_data = rides_data.merge(dim_hub_map, left_on='hub_id', right_on='hub_id', how='left')
        rides_data.rename(columns={'hub_sk': 'hub_sk_mapped'}, inplace=True)
        rides_data['hub_sk'] = rides_data['hub_sk_mapped']

        # Limpar colunas temporárias e selecionar as finais
        final_fact_columns = [
            'ride_id', 'driver_user_sk', 'zone_sk', 'neighborhood_sk', 'hub_sk',
            'date_sk', 'time_sk', 'is_going_to_campus', 'slots', 'is_routine_ride',
            'requests_count', 'accepted_requests_count', 'refused_requests_count',
            'pending_requests_count', 'quit_requests_count', 'messages_count',
            'description', 'created_at', 'updated_at'
        ]
        # Garantir que as colunas SK não são nulas se as FKs não são opcionais
        rides_data.dropna(subset=['driver_user_sk', 'zone_sk', 'neighborhood_sk', 'hub_sk', 'date_sk', 'time_sk'], inplace=True)
        
        fact_data_to_load = rides_data[final_fact_columns]
        fact_data_to_load = fact_data_to_load.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print(f"Carregando {len(fact_data_to_load)} registros na fato_carona...")
        
        insert_or_update_query = """
        INSERT INTO fato_carona (
            ride_id, driver_user_sk, zone_sk, neighborhood_sk, hub_sk,
            date_sk, time_sk, is_going_to_campus, slots, is_routine_ride,
            requests_count, accepted_requests_count, refused_requests_count,
            pending_requests_count, quit_requests_count, messages_count, description,
            created_at, updated_at
        ) VALUES (
            %(ride_id)s, %(driver_user_sk)s, %(zone_sk)s, %(neighborhood_sk)s, %(hub_sk)s,
            %(date_sk)s, %(time_sk)s, %(is_going_to_campus)s, %(slots)s, %(is_routine_ride)s,
            %(requests_count)s, %(accepted_requests_count)s, %(refused_requests_count)s,
            %(pending_requests_count)s, %(quit_requests_count)s, %(messages_count)s, %(description)s,
            %(created_at)s, %(updated_at)s
        ) ON CONFLICT (ride_id) DO UPDATE SET
            driver_user_sk = EXCLUDED.driver_user_sk,
            zone_sk = EXCLUDED.zone_sk,
            neighborhood_sk = EXCLUDED.neighborhood_sk,
            hub_sk = EXCLUDED.hub_sk,
            date_sk = EXCLUDED.date_sk,
            time_sk = EXCLUDED.time_sk,
            is_going_to_campus = EXCLUDED.is_going_to_campus,
            slots = EXCLUDED.slots,
            is_routine_ride = EXCLUDED.is_routine_ride,
            requests_count = EXCLUDED.requests_count,
            accepted_requests_count = EXCLUDED.accepted_requests_count,
            refused_requests_count = EXCLUDED.refused_requests_count,
            pending_requests_count = EXCLUDED.pending_requests_count,
            quit_requests_count = EXCLUDED.quit_requests_count,
            messages_count = EXCLUDED.messages_count,
            description = EXCLUDED.description,
            updated_at = EXCLUDED.updated_at; -- Atualizar o updated_at para a marca d'água
        """
        data_to_load_dicts = fact_data_to_load.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load_dicts)
        conn_dw.commit()
        print("Carga da fato_carona concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da FatoCarona: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()