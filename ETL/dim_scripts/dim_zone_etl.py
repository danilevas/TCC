# dim_scripts/dim_zone_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, execute_sql

def etl_dim_zone():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimZone abortado.")
        return False

    try:
        print("Extraindo dados de zones...")
        query_extract_zones = "SELECT id, name, color FROM zones;"
        zones_data = pd.read_sql(query_extract_zones, conn_oltp)
        print(f"Extraídas {len(zones_data)} zonas.")

        zones_data.rename(columns={'id': 'zone_id', 'name': 'zone_name', 'color': 'zone_color'}, inplace=True)
        zones_data = zones_data.replace({pd.NA: None, '': None})

        print("Carregando dados na dim_zone...")
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_zone (zone_id, zone_name, zone_color)
        VALUES (%(zone_id)s, %(zone_name)s, %(zone_color)s)
        ON CONFLICT (zone_id) DO UPDATE SET
            zone_name = EXCLUDED.zone_name,
            zone_color = EXCLUDED.zone_color;
        """
        data_to_load = zones_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_zone concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimZone: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()