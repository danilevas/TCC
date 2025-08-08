# dim_scripts/dim_date_etl.py
import pandas as pd
from datetime import datetime
from config import DB_DW
from utils import connect_to_db

def etl_dim_date():
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Erro de conexão. ETL DimDate abortado.")
        return False

    try:
        print("\n--- DIM_DATE ---")

        # Gerar datas para um período razoável (ex: 2016 a 2030)
        start_date = datetime(2016, 4, 1)
        end_date = datetime(2030, 12, 31)
        
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
            period = f"{dt.year}.{(dt.month - 1) // 7 + 1}"
            year = dt.year

            data_to_load.append((date_sk, full_date, day_of_week, day_name, day_of_month, month, month_name, semester, period, year))
        
        print(f"Gerados {len(data_to_load)} registros para dim_date.")

        # Inserir usando execute_batch para melhor performance
        from psycopg2.extras import execute_batch
        insert_query = """
        INSERT INTO dim_date (
            date_sk, full_date, day_of_week, day_name, day_of_month,
            month, month_name, semester, period, year
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_sk) DO NOTHING;
        """
        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_date concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimDate: {e}")
        return False
    finally:
        if conn_dw: conn_dw.close()