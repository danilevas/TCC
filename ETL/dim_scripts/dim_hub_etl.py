# dim_scripts/dim_hub_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, execute_sql

def etl_dim_hub():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimHub abortado.")
        return False

    try:
        print("Extraindo dados de hubs e campi...")
        # Inclui o nome e a cor do campus para desnormalizar
        query_extract_hubs = """
        SELECT
            h.id,
            h.name,
            c.name AS campus_name,
            c.color AS campus_color
        FROM hubs h
        LEFT JOIN campi c ON h.campus_id = c.id;
        """
        hubs_data = pd.read_sql(query_extract_hubs, conn_oltp)
        print(f"Extraídos {len(hubs_data)} pólos.")

        hubs_data.rename(columns={'id': 'hub_id', 'name': 'hub_name'}, inplace=True)
        hubs_data = hubs_data.replace({pd.NA: None, '': None})

        print("Carregando dados na dim_hub...")
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_hub (hub_id, hub_name, campus_name, campus_color)
        VALUES (%(hub_id)s, %(hub_name)s, %(campus_name)s, %(campus_color)s)
        ON CONFLICT (hub_id) DO UPDATE SET
            hub_name = EXCLUDED.hub_name,
            campus_name = EXCLUDED.campus_name,
            campus_color = EXCLUDED.campus_color;
        """
        data_to_load = hubs_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_hub concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimHub: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()