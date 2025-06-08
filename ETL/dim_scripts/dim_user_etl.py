# dim_scripts/dim_user_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db, execute_sql

def etl_dim_user():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimUser abortado.")
        return False

    try:
        # 1. Extração (Extract) do OLTP
        print("Extraindo dados de users e institutions...")
        query_extract_users = """
        SELECT
            u.id,
            u.name,
            u.profile,
            u.course,
            u.car_owner,
            u.car_model,
            u.car_color,
            u.car_plate,
            u.banned,
            i.name as institution_name
        FROM users u
        LEFT JOIN institutions i ON u.institution_id = i.id;
        """
        users_data = pd.read_sql(query_extract_users, conn_oltp)
        print(f"Extraídos {len(users_data)} usuários.")

        # 2. Transformação (Transform)
        users_data.rename(columns={
            'id': 'user_id',
            'name': 'user_name',
            'car_owner': 'has_car',
            'banned': 'is_banned'
        }, inplace=True)

        # Tratar valores nulos ou inconsistências (ex: car_model se has_car é falso)
        users_data['car_model'] = users_data.apply(lambda row: row['car_model'] if row['has_car'] else None, axis=1)
        users_data['car_color'] = users_data.apply(lambda row: row['car_color'] if row['has_car'] else None, axis=1)
        users_data['car_plate'] = users_data.apply(lambda row: row['car_plate'] if row['has_car'] else None, axis=1)
        
        # Garantir que strings vazias ou NaN sejam None para NULL no banco
        users_data = users_data.replace({pd.NA: None, '': None})

        # 3. Carga (Load) no DW
        print("Carregando dados na dim_user...")
        
        # Usar UPSERT (ON CONFLICT) para lidar com novas inserções e atualizações de usuários
        # Isso atua como um SCD Tipo 1 (atualiza o registro existente)
        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_user (
            user_id, user_name, profile, course, has_car,
            car_model, car_color, car_plate, is_banned, institution_name
        ) VALUES (
            %(user_id)s, %(user_name)s, %(profile)s, %(course)s, %(has_car)s,
            %(car_model)s, %(car_color)s, %(car_plate)s, %(is_banned)s, %(institution_name)s
        ) ON CONFLICT (user_id) DO UPDATE SET
            user_name = EXCLUDED.user_name,
            profile = EXCLUDED.profile,
            course = EXCLUDED.course,
            has_car = EXCLUDED.has_car,
            car_model = EXCLUDED.car_model,
            car_color = EXCLUDED.car_color,
            car_plate = EXCLUDED.car_plate,
            is_banned = EXCLUDED.is_banned,
            institution_name = EXCLUDED.institution_name;
        """
        
        # Converter DataFrame para lista de dicionários para execute_batch
        data_to_load = users_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_user concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimUser: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()