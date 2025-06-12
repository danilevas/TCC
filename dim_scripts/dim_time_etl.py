# dim_scripts/dim_time_etl.py
import pandas as pd
from datetime import datetime, timedelta
from config import DB_DW
from utils import connect_to_db, execute_sql

def etl_dim_time():
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Erro de conexão. ETL DimTime abortado.")
        return False

    try:
        # Gerar datas para um período razoável (ex: 2016 a 2026)
        start_date = datetime(2016, 4, 1)
        end_date = datetime(2025, 12, 31)
        
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        data_to_load = []
        for dt in dates:
            # Gerar dados para a parte de data
            date_sk = int(dt.strftime('%Y%m%d'))
            full_date = dt.date()
            day_of_week = dt.isoweekday() # 1=Monday, 7=Sunday
            day_name = dt.strftime('%A')
            day_of_month = dt.day
            month = dt.month
            month_name = dt.strftime('%B')
            semester = (dt.month - 1) // 6 + 1
            year = dt.year

            # Para cada dia, gerar dados para todas as horas/minutos
            for hour in range(24):
                for minute in range(60):
                    time_dt = dt.replace(hour=hour, minute=minute)
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

                    data_to_load.append((date_sk, full_date, day_of_week, day_name, day_of_month,
                                         month, month_name, semester, year,
                                         hour_sk, hour_of_day, minute_of_hour, time_bucket))
        
        print(f"Gerados {len(data_to_load)} registros para dim_time.")
        print(f"Exemplo: {data_to_load[0]}")
        print(f"Exemplo: {data_to_load[1]}")

        # Inserir usando execute_batch para melhor performance
        from psycopg2.extras import execute_batch
        insert_query = """
        INSERT INTO dim_time (
            date_sk, full_date, day_of_week, day_name, day_of_month,
            month, month_name, semester, year,
            hour_sk, hour_of_day, minute_of_hour, time_of_day_bucket
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_sk, hour_sk) DO NOTHING;
        """
        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_time concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimTime: {e}")
        return False
    finally:
        if conn_dw: conn_dw.close()