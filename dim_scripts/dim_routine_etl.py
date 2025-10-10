# dim_scripts/dim_routine_etl.py
import pandas as pd
from config import DB_OLTP, DB_DW
from utils import connect_to_db

# Mapeamento para os dias da semana (1=Segunda, ..., 7=Domingo)
DAY_NUM_TO_NAME = {
    1: 'Seg',
    2: 'Ter',
    3: 'Qua',
    4: 'Qui',
    5: 'Sex',
    6: 'Sab',
    7: 'Dom'
}

DAY_NUM_TO_FLAG_COL = {
    1: 'is_routine_monday',
    2: 'is_routine_tuesday',
    3: 'is_routine_wednesday',
    4: 'is_routine_thursday',
    5: 'is_routine_friday',
    6: 'is_routine_saturday',
    7: 'is_routine_sunday'
}

def parse_week_days(week_days_str):
    """
    Parseia a string week_days e retorna um dicionário com flags booleanas
    e a descrição dos dias.

    Args:
        week_days_str: String com números separados por vírgula (ex: "1,3,5")

    Returns:
        dict com as flags booleanas e routine_days_description
    """
    # Inicializar todas as flags como False
    flags = {col: False for col in DAY_NUM_TO_FLAG_COL.values()}
    days_list = []

    if pd.notna(week_days_str):
        week_days_str = str(week_days_str)
        try:
            day_numbers = [int(d.strip()) for d in week_days_str.split(',') if d.strip()]
            for day_num in day_numbers:
                col_name = DAY_NUM_TO_FLAG_COL.get(day_num)
                day_name = DAY_NUM_TO_NAME.get(day_num)
                if col_name and day_name:
                    flags[col_name] = True
                    days_list.append(day_name)
        except ValueError:
            print(f"Aviso: 'week_days' mal formatado: '{week_days_str}'. Flags definidas como False.")

    # Gerar descrição
    if days_list:
        routine_days_description = ','.join(days_list)
    else:
        routine_days_description = 'Dias não especificados'

    flags['routine_days_description'] = routine_days_description
    return flags

def etl_dim_routine():
    conn_oltp = connect_to_db(DB_OLTP)
    conn_dw = connect_to_db(DB_DW)

    if not conn_oltp or not conn_dw:
        print("Erro de conexão. ETL DimRoutine abortado.")
        return False

    try:
        print("\n--- DIM_ROUTINE ---")
        print("Extraindo dados de rotinas...")

        # Extração: buscar rotinas distintas do OLTP
        # Filtrar apenas rotinas VÁLIDAS (com repeats_until preenchido)
        query_extract_routines = """
        SELECT DISTINCT
            r.routine_id,
            r.repeats_until,
            r.week_days
        FROM rides r
        WHERE r.routine_id IS NOT NULL
          AND r.repeats_until IS NOT NULL
        ORDER BY r.routine_id;
        """
        routines_data = pd.read_sql(query_extract_routines, conn_oltp)
        print(f"Extraídas {len(routines_data)} rotinas.")

        if routines_data.empty:
            print("Nenhuma rotina encontrada no OLTP.")
            return True

        # Transformação: parsear week_days e gerar flags
        print("Transformando dados de week_days em flags...")

        # Aplicar parse_week_days e expandir o resultado em colunas
        parsed_flags = routines_data['week_days'].apply(parse_week_days)
        flags_df = pd.DataFrame(parsed_flags.tolist())

        # Combinar com os dados originais
        routines_data = pd.concat([routines_data, flags_df], axis=1)

        # Remover coluna week_days (não vai para o DW)
        routines_data = routines_data.drop(columns=['week_days'])

        # Converter routine_id para Int64
        routines_data['routine_id'] = pd.to_numeric(routines_data['routine_id'], errors='coerce').astype('Int64')

        # Garantir que flags booleanas estão como bool
        for col in DAY_NUM_TO_FLAG_COL.values():
            routines_data[col] = routines_data[col].astype(bool)

        # Substituir NAs por None para NULL no banco
        routines_data = routines_data.replace({pd.NA: None, '': None})

        # Carga: inserir/atualizar no DW
        print("Carregando dados na dim_routine...")

        from psycopg2.extras import execute_batch
        insert_or_update_query = """
        INSERT INTO dim_routine (
            routine_id, repeats_until,
            is_routine_monday, is_routine_tuesday, is_routine_wednesday,
            is_routine_thursday, is_routine_friday, is_routine_saturday, is_routine_sunday,
            routine_days_description
        ) VALUES (
            %(routine_id)s, %(repeats_until)s,
            %(is_routine_monday)s, %(is_routine_tuesday)s, %(is_routine_wednesday)s,
            %(is_routine_thursday)s, %(is_routine_friday)s, %(is_routine_saturday)s, %(is_routine_sunday)s,
            %(routine_days_description)s
        ) ON CONFLICT (routine_id) DO UPDATE SET
            repeats_until = EXCLUDED.repeats_until,
            is_routine_monday = EXCLUDED.is_routine_monday,
            is_routine_tuesday = EXCLUDED.is_routine_tuesday,
            is_routine_wednesday = EXCLUDED.is_routine_wednesday,
            is_routine_thursday = EXCLUDED.is_routine_thursday,
            is_routine_friday = EXCLUDED.is_routine_friday,
            is_routine_saturday = EXCLUDED.is_routine_saturday,
            is_routine_sunday = EXCLUDED.is_routine_sunday,
            routine_days_description = EXCLUDED.routine_days_description
        """

        data_to_load = routines_data.to_dict(orient='records')

        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_or_update_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_routine concluída.")
        return True

    except Exception as e:
        print(f"Erro no ETL da DimRoutine: {e}")
        return False
    finally:
        if conn_oltp: conn_oltp.close()
        if conn_dw: conn_dw.close()
