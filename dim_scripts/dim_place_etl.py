# dim_scripts/dim_place_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db

def etl_dim_place():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimPlace abortado.")
        return False

    try:
        print("\n--- DIM_PLACE ---")
        print("Extraindo dados de hubs, campi e institutions...")

        query_extract_hubs = """
        SELECT
            h.id AS hub_id,
            h.name AS place_name,
            h.center,
            h.campus_id,
            c.name AS campus_name,
            c.institution_id,
            i.name AS institution_name
        FROM hubs h
        LEFT JOIN campi c ON h.campus_id = c.id
        LEFT JOIN institutions i ON c.institution_id = i.id;
        """
        hubs_data = pd.read_sql(query_extract_hubs, conn_oltp)
        print(f"Extraídos {len(hubs_data)} pólos.")

        print("Extraindo dados de neighborhoods e zones...")
        query_extract_neighborhoods = """
        SELECT
            n.id AS neighborhood_id,
            n.name AS place_name,
            n.zone_id,
            z.name AS zone_name
        FROM neighborhoods n
        LEFT JOIN zones z ON n.zone_id = z.id;
        """
        neighborhoods_data = pd.read_sql(query_extract_neighborhoods, conn_oltp)
        print(f"Extraídos {len(neighborhoods_data)} bairros.")

        # Criando a coluna 'place_type' em cada DataFrame antes de concatenar
        hubs_data['place_type'] = 'hub'
        neighborhoods_data['place_type'] = 'neighborhood'

        # Concatenando os DataFrames
        place_data = pd.concat([hubs_data, neighborhoods_data], ignore_index=True)

        # Convertendo as colunas numéricas
        colunas_numericas = ['hub_id', 'campus_id', 'institution_id', 'neighborhood_id', 'zone_id']
        for coluna in colunas_numericas:
            place_data[coluna] = pd.to_numeric(place_data[coluna], errors='coerce').astype('Int64')

        # Jogando a coluna place_type pro início do df_final
        cols = ['place_type'] + [col for col in place_data.columns if col != 'place_type']
        place_data = place_data[cols]

        place_data = place_data.replace({pd.NA: None, '': None})

        print("Carregando dados na dim_place...")
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_place (
            place_name, place_type,
            hub_id, center, campus_id, campus_name, institution_id, institution_name,
            neighborhood_id, zone_id, zone_name
        ) VALUES (
            %(place_name)s, %(place_type)s,
            %(hub_id)s, %(center)s, %(campus_id)s, %(campus_name)s, %(institution_id)s, %(institution_name)s,
            %(neighborhood_id)s, %(zone_id)s, %(zone_name)s
        )
        ON CONFLICT (place_sk) DO UPDATE SET
            place_name = EXCLUDED.place_name,
            place_type = EXCLUDED.place_type,

            hub_id = EXCLUDED.hub_id,
            center = EXCLUDED.center,
            campus_id = EXCLUDED.campus_id,
            campus_name = EXCLUDED.campus_name,
            institution_id = EXCLUDED.institution_id,
            institution_name = EXCLUDED.institution_name,

            neighborhood_id = EXCLUDED.neighborhood_id,
            zone_id = EXCLUDED.zone_id,
            zone_name = EXCLUDED.zone_name
        """
        data_to_load = place_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_place concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimPlace: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()