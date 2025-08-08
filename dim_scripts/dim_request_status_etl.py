# dim_scripts/dim_request_status_etl.py
from config import DB_DW
from utils import connect_to_db
from psycopg2.extras import execute_batch

def etl_dim_request_status():
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Erro de conexão. ETL DimRequestStatus abortado.")
        return False

    try:
        # Status conhecidos
        # Você pode obter estes da tabela ride_user.status em um DISTINCT
        # ou defini-los manualmente se forem fixos.
        status_names = ['pending', 'accepted', 'refused', 'quit']
        
        print("\n--- DIM_REQUEST_STATUS ---")
        print("Carregando dados na dim_request_status...")
        
        # Usar UPSERT para garantir que os status existam, mas não duplicar
        insert_or_update_query = """
        INSERT INTO dim_request_status (status_name)
        VALUES (%s)
        ON CONFLICT (status_name) DO NOTHING; -- Não atualiza se já existir
        """
        # Formatar para execute_batch espera uma lista de tuplas
        data_to_load = [(s,) for s in status_names]

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_request_status concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimRequestStatus: {e}")
        return False
    finally:
        if conn_dw: conn_dw.close()