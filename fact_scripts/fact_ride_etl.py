# fact_scripts/fact_ride_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, get_last_etl_run_date_se_houver
from psycopg2.extras import execute_batch
import numpy as np # Importar numpy para pd.NA

from dim_scripts.dim_ride_flags_etl import load_ride_flags_lookup, derive_and_lookup_flags

def etl_fact_ride(last_etl_run_date_str=None):
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL FactRide abortado.")
        return False

    try:
        # 0. Carregar a dim_ride_flags para lookup em memória
        load_ride_flags_lookup(conn_dw)

        # Agora importamos o dicionário de lookup
        from dim_scripts.dim_ride_flags_etl import _RIDE_FLAGS_LOOKUP_DICT

        if not _RIDE_FLAGS_LOOKUP_DICT:
            print("Não foi possível carregar dim_ride_flags. ETL FactRide abortado.")
            return False
        
        # Obter o último timestamp do DW para carga incremental
        last_etl_run_date = get_last_etl_run_date_se_houver(conn_dw, last_etl_run_date_str, 'fact_ride')

        print(f"\n--- FACT_RIDE ---")
        print(f"Extraindo dados de caronas (rides) e ride_user a partir de: {last_etl_run_date}")

        # 1. Extração (Extract) dos dados incrementais do OLTP
        # LEFT JOIN com ride_user para garantir que pegamos o driver_id associado à carona (e driver_id=null para as deletadas)
        # e com messages para contar as mensagens
        query_extract_rides = f"""
        SELECT
            r.id AS ride_id,
            r.routine_id,
            ru_driver.user_id AS driver_id,
            r.neighborhood AS neighborhood_name, -- Para pegarmos o place_neighborhood_sk
            r.hub AS hub_name, -- Para pegarmos o place_hub_sk
            r.going AS is_going_to_campus,
            r.week_days,
            r.done,
            r.deleted_at,
            r.repeats_until,
            r.created_at, -- Chaves Temporais / Controle do ETL, marca d'água
            r.updated_at, -- Para controle do ETL, marca d'água
            r.date AS occurred_at, -- Chaves Temporais
            r.slots,
	        msg.messages_count
        FROM rides r 
        LEFT JOIN ride_user ru_driver ON r.id = ru_driver.ride_id AND ru_driver.status = 'driver'
        LEFT JOIN (
            SELECT ride_id, COUNT(*) AS messages_count
            FROM messages
            GROUP BY ride_id
        ) AS msg ON r.id = msg.ride_id
        WHERE (r.created_at >= '{last_etl_run_date}' OR r.updated_at >= '{last_etl_run_date}' OR r.deleted_at >= '{last_etl_run_date}');
        """
        rides_data = pd.read_sql(query_extract_rides, conn_oltp)

        # Extrair dados de ride_user para agregação de status
        # Filtrar por updated_at ou created_at para pegar apenas os pedidos recentes ou atualizados
        query_extract_ride_users_for_aggregation = f"""
        SELECT
            ride_id,
            status
        FROM ride_user
        WHERE (created_at >= '{last_etl_run_date}' OR updated_at >= '{last_etl_run_date}');
        """
        ride_users_agg_data = pd.read_sql(query_extract_ride_users_for_aggregation, conn_oltp)

        # 1.5. Tratamento de tipos
        # Convertendo as colunas numéricas
        colunas_numericas = ['ride_id', 'routine_id', 'slots', 'driver_id', 'messages_count']
        for coluna in colunas_numericas:
            rides_data[coluna] = pd.to_numeric(rides_data[coluna], errors='coerce').astype('Int64')
        
        # Convertendo as colunas booleanas
        rides_data['is_going_to_campus'] = rides_data['is_going_to_campus'].fillna(False).astype(bool)
        rides_data['done'] = rides_data['done'].fillna(False).astype(bool)

        # 2. Transformação (Transform)
        print(f"Extraídas {len(rides_data)} caronas para processamento incremental.")

        if rides_data.empty and ride_users_agg_data.empty:
            print("Nenhum dado novo ou atualizado para processar na fact_ride.")
            return True # Não há dados para carregar, mas não é um erro

        # Gerar chaves de data/hora a partir da data e hora em que a carona foi criada (da coluna created_at)
        rides_data['creation_date_sk'] = pd.to_datetime(rides_data['created_at']).dt.strftime('%Y%m%d').astype('Int64')
        rides_data['creation_hour_sk'] = pd.to_datetime(rides_data['created_at']).dt.strftime('%H%M').astype('Int64')

        # Gerar chaves de data/hora a partir da data e hora em que a carona estava marcada para ocorrer (da coluna occurred_at, que veio de date)
        rides_data['occurrence_date_sk'] = pd.to_datetime(rides_data['occurred_at']).dt.strftime('%Y%m%d').astype('Int64')
        rides_data['occurrence_hour_sk'] = pd.to_datetime(rides_data['occurred_at']).dt.strftime('%H%M').astype('Int64')

        # Determinar se é carona de rotina
        rides_data['is_routine_ride'] = (rides_data['repeats_until'].notna())

        # Converter a coluna is_routine_ride para Python booleano (True/False)
        rides_data['is_routine_ride'] = rides_data['is_routine_ride'].fillna(False).astype(bool)

        # Tornar nulos os valores de 'routine_id' onde 'is_routine_ride' é falso
        rides_data.loc[~rides_data['is_routine_ride'], 'routine_id'] = pd.NA

        # -------------------- FLAGS --------------------

        print("Derivando flags e buscando ride_flags_sk...")

        # A coluna 'is_routine_ride' do OLTP, 'week_days', 'description' e 'done'
        # são passadas para 'derive_and_lookup_flags' através do row_oltp.
        rides_data['ride_flags_sk'] = rides_data.apply(derive_and_lookup_flags, axis=1)
        
        # Transformando a coluna em Int64
        rides_data['ride_flags_sk'] = pd.to_numeric(rides_data['ride_flags_sk'], errors='coerce').astype('Int64')
        print("Flags e ride_flags_sk processados.")

        # -------------------- PEDIDOS --------------------

        # Agregar métricas de pedidos
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
            rides_data['driver_creation_events_agg'] = 0

        rides_data['requests_count'] = rides_data[['pending_requests_count', 'accepted_requests_count', 'refused_requests_count', 'quit_requests_count']].sum(axis=1)

        # Transformando todo mundo em Int64
        rides_data['pending_requests_count'] = pd.to_numeric(rides_data['pending_requests_count'], errors='coerce').astype('Int64')
        rides_data['accepted_requests_count'] = pd.to_numeric(rides_data['accepted_requests_count'], errors='coerce').astype('Int64')
        rides_data['refused_requests_count'] = pd.to_numeric(rides_data['refused_requests_count'], errors='coerce').astype('Int64')
        rides_data['quit_requests_count'] = pd.to_numeric(rides_data['quit_requests_count'], errors='coerce').astype('Int64')
        rides_data['driver_creation_events_agg'] = pd.to_numeric(rides_data['driver_creation_events_agg'], errors='coerce').astype('Int64')

        # Tratar NAs após o merge e antes da seleção final
        rides_data.fillna({
            'pending_requests_count': 0, 'accepted_requests_count': 0,
            'refused_requests_count': 0, 'quit_requests_count': 0,
            'requests_count': 0, 'messages_count': 0,
            'is_going_to_campus': False, 'slots': 0, 'is_routine_ride': False,
            'driver_creation_events_agg': 0
        }, inplace=True)

        # -------------------- SKs --------------------

        # Obter chaves substitutas das dimensões já carregadas
        # Otimização: Carregar mapas de SKs uma vez
        dim_user_map = pd.read_sql("SELECT user_id, user_sk FROM dim_user;", conn_dw)
        dim_place_neighborhood_map = pd.read_sql("SELECT neighborhood_name, place_sk AS place_neighborhood_sk FROM dim_place;", conn_dw)
        dim_place_hub_map = pd.read_sql("SELECT hub_name, place_sk AS place_hub_sk FROM dim_place;", conn_dw)

        # Convertendo para numéricos os mapas das dimensões
        dim_user_map['user_id'] = pd.to_numeric(dim_user_map['user_id'], errors='coerce').astype('Int64')

        # Fazendo o merge com dim_user_map
        rides_data = rides_data.merge(dim_user_map, left_on='driver_id', right_on='user_id', how='left')
        rides_data.rename(columns={'user_sk': 'driver_user_sk'}, inplace=True)

        # Fazendo o merge com dim_place_neighborhood_map
        rides_data = rides_data.merge(dim_place_neighborhood_map, left_on='neighborhood_name', right_on='neighborhood_name', how='left')

        # Fazendo o merge com dim_place_hub_map
        rides_data = rides_data.merge(dim_place_hub_map, left_on='hub_name', right_on='hub_name', how='left')

        # Convertendo para Int64 essas sks
        rides_data['driver_user_sk'] = pd.to_numeric(rides_data['driver_user_sk'], errors='coerce').astype('Int64')
        rides_data['place_neighborhood_sk'] = pd.to_numeric(rides_data['place_neighborhood_sk'], errors='coerce').astype('Int64')
        rides_data['place_hub_sk'] = pd.to_numeric(rides_data['place_hub_sk'], errors='coerce').astype('Int64')

        # Tratamento de SKs nulas após o merge (se houver IDs que não foram mapeados - assumindo -1 para sk desconhecido)
        rides_data['driver_user_sk'].fillna(-1, inplace=True)
        rides_data['place_neighborhood_sk'].fillna(-1, inplace=True)
        rides_data['place_hub_sk'].fillna(-1, inplace=True)

        # -------------------- FINAL --------------------

        # Limpar colunas temporárias e selecionar as finais
        final_fact_columns = [
            'ride_id', 'routine_id',
            'driver_user_sk', 'place_neighborhood_sk', 'place_hub_sk', 'ride_flags_sk',
            'creation_date_sk', 'creation_hour_sk', 'occurrence_date_sk', 'occurrence_hour_sk',
            'slots', 'messages_count', 'requests_count', 'accepted_requests_count', 'refused_requests_count',
            'pending_requests_count', 'quit_requests_count', 'repeats_until'
        ]
        
        # Garantir que as colunas SK não são nulas se as FKs não são opcionais (refletir se deixamos assim, mas acho que sim porque já tem o -1 pros desconhecidos)
        rides_data.dropna(subset=['driver_user_sk', 'place_neighborhood_sk', 'place_hub_sk', 'ride_flags_sk',
                                  'creation_date_sk', 'creation_hour_sk', 'occurrence_date_sk', 'occurrence_hour_sk'], inplace=True)
        
        fact_data_to_load = rides_data[final_fact_columns]
        fact_data_to_load = fact_data_to_load.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print(f"Carregando {len(fact_data_to_load)} registros na fact_ride...")
        
        insert_or_update_query = """
        INSERT INTO fact_ride (
            ride_id, routine_id,
            driver_user_sk, place_neighborhood_sk, place_hub_sk, ride_flags_sk,
            creation_date_sk, creation_hour_sk, occurrence_date_sk, occurrence_hour_sk,
            slots, messages_count, requests_count, accepted_requests_count, refused_requests_count,
            pending_requests_count, quit_requests_count, repeats_until
        ) VALUES (
            %(ride_id)s, %(routine_id)s,
            %(driver_user_sk)s, %(place_neighborhood_sk)s, %(place_hub_sk)s, %(ride_flags_sk)s,
            %(creation_date_sk)s, %(creation_hour_sk)s, %(occurrence_date_sk)s, %(occurrence_hour_sk)s,
            %(slots)s, %(messages_count)s, %(requests_count)s, %(accepted_requests_count)s, %(refused_requests_count)s,
            %(pending_requests_count)s, %(quit_requests_count)s, %(repeats_until)s
        ) ON CONFLICT (ride_id) DO UPDATE SET
            ride_id = EXCLUDED.ride_id,
            routine_id = EXCLUDED.routine_id,

            driver_user_sk = EXCLUDED.driver_user_sk,
            place_neighborhood_sk = EXCLUDED.place_neighborhood_sk,
            place_hub_sk = EXCLUDED.place_hub_sk,
            ride_flags_sk = EXCLUDED.ride_flags_sk,

            creation_date_sk = EXCLUDED.creation_date_sk,
            creation_hour_sk = EXCLUDED.creation_hour_sk,
            occurrence_date_sk = EXCLUDED.occurrence_date_sk,
            occurrence_hour_sk = EXCLUDED.occurrence_hour_sk,

            slots = EXCLUDED.slots,
            messages_count = EXCLUDED.messages_count,
            requests_count = EXCLUDED.requests_count,
            accepted_requests_count = EXCLUDED.accepted_requests_count,
            refused_requests_count = EXCLUDED.refused_requests_count,
            pending_requests_count = EXCLUDED.pending_requests_count,
            quit_requests_count = EXCLUDED.quit_requests_count,
            repeats_until = EXCLUDED.repeats_until
        """
        data_to_load_dicts = fact_data_to_load.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load_dicts)
        conn_dw.commit()
        print("Carga da fact_ride concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da FactRide: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()