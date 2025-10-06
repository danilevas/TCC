# dim_scripts/dim_request_status_etl.py
from config import DB_DW, DB_OLTP
from utils import connect_to_db
from psycopg2.extras import execute_batch

def etl_dim_request_status():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimRequestStatus abortado.")
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()
        return False

    try:
        print("\n--- DIM_REQUEST_STATUS ---")
        print("Extraindo status distintos do OLTP (ride_user)...")

        # Extrair status distintos do OLTP, excluindo 'driver'
        extract_query = """
        SELECT DISTINCT status
        FROM ride_user
        WHERE status <> 'driver' AND status IS NOT NULL
        ORDER BY status;
        """

        with conn_oltp.cursor() as cur:
            cur.execute(extract_query)
            status_results = cur.fetchall()

        # Converter resultados para lista de tuplas
        status_names = [(row[0],) for row in status_results]

        if not status_names:
            print("Nenhum status encontrado no OLTP. Abortando carga.")
            return False

        print(f"Status encontrados: {[s[0] for s in status_names]}")
        print("Carregando dados na dim_request_status...")

        # Usar UPSERT para garantir que os status existam, mas não duplicar
        insert_or_update_query = """
        INSERT INTO dim_request_status (status_name)
        VALUES (%s)
        ON CONFLICT (status_name) DO NOTHING;
        """

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, status_names)
        conn_dw.commit()
        print("Carga da dim_request_status concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimRequestStatus: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()