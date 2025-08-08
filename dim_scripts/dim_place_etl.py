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
            h.name AS hub_name,
            h.center,
            h.campus_id,
            c.name AS campus_name,
            c.color AS campus_color,
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
            n.name AS neighborhood_name,
            n.zone_id,
            z.name AS zone_name,
            z.color AS zone_color
        FROM neighborhoods n
        LEFT JOIN zones z ON n.zone_id = z.id;
        """
        neighborhoods_data = pd.read_sql(query_extract_neighborhoods, conn_oltp)
        print(f"Extraídos {len(neighborhoods_data)} bairros.")

        # Criando a coluna 'tipo' em cada DataFrame antes de concatenar
        hubs_data['tipo'] = 'hub'
        neighborhoods_data['tipo'] = 'neighborhood'

        # Concatenando os DataFrames
        place_data = pd.concat([hubs_data, neighborhoods_data], ignore_index=True)

        # Convertendo as colunas numéricas
        colunas_numericas = ['hub_id', 'campus_id', 'institution_id', 'neighborhood_id', 'zone_id']
        for coluna in colunas_numericas:
            place_data[coluna] = pd.to_numeric(place_data[coluna], errors='coerce').astype('Int64')

        # Jogando a coluna tipo pro início do df_final
        cols = ['tipo'] + [col for col in place_data.columns if col != 'tipo']
        place_data = place_data[cols]

        place_data = place_data.replace({pd.NA: None, '': None})

        print("Carregando dados na dim_place...")
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_place (
            hub_id, hub_name, center,
            campus_id, campus_name, campus_color, institution_id, institution_name,
            neighborhood_id, neighborhood_name, zone_id, zone_name, zone_color
        ) VALUES (
            %(hub_id)s, %(hub_name)s, %(center)s,
            %(campus_id)s, %(campus_name)s, %(campus_color)s, %(institution_id)s, %(institution_name)s,
            %(neighborhood_id)s, %(neighborhood_name)s, %(zone_id)s, %(zone_name)s, %(zone_color)s
        )
        ON CONFLICT (place_sk) DO UPDATE SET
            hub_id = EXCLUDED.hub_id,
            hub_name = EXCLUDED.hub_name,
            center = EXCLUDED.center,
            campus_id = EXCLUDED.campus_id,
            campus_name = EXCLUDED.campus_name,
            campus_color = EXCLUDED.campus_color,
            institution_id = EXCLUDED.institution_id,
            institution_name = EXCLUDED.institution_name,

            neighborhood_id = EXCLUDED.neighborhood_id,
            neighborhood_name = EXCLUDED.neighborhood_name,
            zone_id = EXCLUDED.zone_id,
            zone_name = EXCLUDED.zone_name,
            zone_color = EXCLUDED.zone_color
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