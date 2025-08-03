# dim_scripts/dim_ride_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, get_last_etl_run_date_se_houver

def etl_dim_ride(last_etl_run_date_str=None):
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimRide abortado.")
        return False

    try:
        # Obter o último timestamp do DW para carga incremental
        last_etl_run_date = get_last_etl_run_date_se_houver(conn_dw, last_etl_run_date_str, 'dim_ride')
        print(f"Extraindo dados de caronas (rides). A partir de: {last_etl_run_date}")

        # 1. Extração (Extract) do OLTP
        print("Extraindo dados de rides...")
        query_extract_rides = f"""
        SELECT
            id AS ride_id,
            routine_id,
            repeats_until,
            created_at,
            updated_at,
            date AS occurred_at,
            deleted_at
        FROM rides
        WHERE (created_at >= '{last_etl_run_date}' OR updated_at >= '{last_etl_run_date}' OR deleted_at >= '{last_etl_run_date}');
        """
        rides_data = pd.read_sql(query_extract_rides, conn_oltp)
        print(f"Extraídas {len(rides_data)} caronas.")

        # 1.5. Tratamento de tipos
        # Convertendo as colunas numéricas
        colunas_numericas = ['ride_id', 'routine_id']
        for coluna in colunas_numericas:
            rides_data[coluna] = pd.to_numeric(rides_data[coluna], errors='coerce').astype('Int64')

        # 2. Transformação (Transform)
   
        # Garantir que strings vazias ou NaN sejam None para NULL no banco
        rides_data = rides_data.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print("Carregando dados na dim_ride...")
        
        # Usar UPSERT (ON CONFLICT) para lidar com novas inserções e atualizações de caronas
        # Isso atua como um SCD Tipo 1 (atualiza o registro existente)
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_ride (
            ride_id, routine_id, repeats_until,
            created_at, updated_at, occurred_at, deleted_at
        ) VALUES (
            %(ride_id)s, %(routine_id)s, %(repeats_until)s,
            %(created_at)s, %(updated_at)s, %(occurred_at)s, %(deleted_at)s
        ) ON CONFLICT (ride_id) DO UPDATE SET
            routine_id = EXCLUDED.routine_id,
            repeats_until = EXCLUDED.repeats_until,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at, -- Atualizar o updated_at para a marca d'água
            occurred_at = EXCLUDED.occurred_at, -- Descritivo
            deleted_at = EXCLUDED.deleted_at -- Atualizar o deleted_at para a marca d'água se a carona foi deletada de lá pra cá
        """
        
        # Converter DataFrame para lista de dicionários para execute_batch
        data_to_load = rides_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_ride concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimRide: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()