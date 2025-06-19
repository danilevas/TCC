# dim_scripts/dim_neighborhood_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db

def etl_dim_neighborhood():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimNeighborhood abortado.")
        return False

    try:
        print("Extraindo dados de neighborhoods e zones...")
        # Inclui o nome da zona para desnormalizar
        query_extract_neighborhoods = """
        SELECT
            n.id AS neighborhood_id,
            n.name AS neighborhood_name,
            n.distance AS distance_to_fundao,
            n.zone_id,
            z.name AS zone_name,
            z.color AS zone_color
        FROM neighborhoods n
        LEFT JOIN zones z ON n.zone_id = z.id;
        """
        neighborhoods_data = pd.read_sql(query_extract_neighborhoods, conn_oltp)
        print(f"Extraídos {len(neighborhoods_data)} bairros.")

        neighborhoods_data.rename(columns={
            'id': 'neighborhood_id',
            'name': 'neighborhood_name'
        }, inplace=True)
        neighborhoods_data = neighborhoods_data.replace({pd.NA: None, '': None})

        print("Carregando dados na dim_neighborhood...")
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_neighborhood (neighborhood_id, neighborhood_name, distance_to_fundao, zone_id, zone_name, zone_color)
        VALUES (%(neighborhood_id)s, %(neighborhood_name)s, %(distance)s, %(zone_id)s %(zone_name)s, %(zone_color)s)
        ON CONFLICT (neighborhood_id) DO UPDATE SET
            neighborhood_name = EXCLUDED.neighborhood_name,
            distance_to_fundao = EXCLUDED.distance_to_fundao,
            zone_id = EXCLUDED.zone_id,
            zone_name = EXCLUDED.zone_name,
            zone_color = EXCLUDED.zone_color;
        """
        data_to_load = neighborhoods_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_neighborhood concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimNeighborhood: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()