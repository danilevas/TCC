# dim_scripts/dim_hour_etl.py
import pandas as pd
from datetime import time
from config import DB_DW
from utils import connect_to_db

def etl_dim_hour():
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Erro de conexão. ETL DimHour abortado.")
        return False

    try:
        print("\n--- DIM_HOUR ---")

        data_to_load = []

        # Para cada dia, gerar dados para todas as horas/minutos
        for hour in range(24):
            for minute in range(60):
                time_dt = time(hour=hour, minute=minute)
                hour_sk = int(time_dt.strftime('%H%M')) # ex: 1435 para 14:35
                hour_of_day = time_dt.hour
                minute_of_hour = time_dt.minute
                
                if 0 <= hour_of_day < 6:
                    time_bucket = 'Madrugada'
                elif 6 <= hour_of_day < 12:
                    time_bucket = 'Manhã'
                elif 12 <= hour_of_day < 18:
                    time_bucket = 'Tarde'
                else:
                    time_bucket = 'Noite'

                data_to_load.append((hour_sk, hour_of_day, minute_of_hour, time_bucket))
        
        print(f"Gerados {len(data_to_load)} registros para dim_hour.")

        # Inserir usando execute_batch para melhor performance
        from psycopg2.extras import execute_batch
        insert_query = """
        INSERT INTO dim_hour (
            hour_sk, hour_of_day, minute_of_hour, time_of_day_bucket
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (hour_sk) DO NOTHING;
        """
        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_hour concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimHour: {e}")
        return False
    finally:
        if conn_dw: conn_dw.close()