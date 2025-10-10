# dim_scripts/dim_ride_flags_etl.py
import itertools
from config import DB_DW
from utils import connect_to_db
from psycopg2.extras import execute_batch
import pandas as pd

# Ordem das flags para lookup na dim_ride_flags
FLAG_NAMES_ORDER = [
    'is_going_to_campus',
    'done',
    'deleted'
]

# Variável global para armazenar o lookup da dim_ride_flags
# Será populada uma vez por execução do ETL
_RIDE_FLAGS_LOOKUP_DICT = {}

def load_ride_flags_lookup(conn_dw):
    """Carrega a dim_ride_flags para um dicionário de lookup em memória."""
    global _RIDE_FLAGS_LOOKUP_DICT

    if _RIDE_FLAGS_LOOKUP_DICT: # Já carregado
        return

    print("Carregando dim_ride_flags para lookup em memória...")
    try:
        query = f"SELECT {', '.join(FLAG_NAMES_ORDER)}, ride_flags_sk FROM dim_ride_flags;"
        flags_df = pd.read_sql(query, conn_dw)

        # Cria o dicionário de lookup: (True, False, ..., True) -> ride_flags_sk
        _RIDE_FLAGS_LOOKUP_DICT = {
            tuple(row[col] for col in FLAG_NAMES_ORDER): row['ride_flags_sk']
            for index, row in flags_df.iterrows()
        }
        print(f"dim_ride_flags carregada: {len(_RIDE_FLAGS_LOOKUP_DICT)} combinações.")
    except Exception as e:
        print(f"Erro ao carregar dim_ride_flags para lookup: {e}")
        _RIDE_FLAGS_LOOKUP_DICT = {} # Limpa em caso de erro para tentar novamente se necessário

def derive_and_lookup_flags(row_oltp):
    """
    Deriva as flags booleanas para uma carona do OLTP e busca o ride_flags_sk correspondente.
    """
    # 1. Inicializar todas as flags como FALSE (estado padrão/desconhecido)
    flags = {name: False for name in FLAG_NAMES_ORDER}

    # 2. Derivar as flags diretamente do OLTP
    # Usar .get para segurança e pd.notna para tratar NaN/None de forma robusta
    if pd.notna(row_oltp.get('is_going_to_campus')) and row_oltp['is_going_to_campus'] is True:
        flags['is_going_to_campus'] = True

    if pd.notna(row_oltp.get('done')) and row_oltp['done'] is True:
        flags['done'] = True

    # Se houver data de exclusão da carona, deleted = True
    if pd.notna(row_oltp.get('deleted_at')):
        flags['deleted'] = True

    # 3. Converter o dicionário de flags para uma tupla na ordem correta para o lookup
    flags_tuple = tuple(flags[name] for name in FLAG_NAMES_ORDER)

    # 4. Lookup no dicionário em memória
    ride_flags_sk = _RIDE_FLAGS_LOOKUP_DICT.get(flags_tuple)

    if ride_flags_sk is None:
        # Isso NÃO DEVE ACONTECER se a dimensão sucata foi pré-populada corretamente
        # e se a lógica de derivação é consistente com a lógica de pré-população.
        print(f"ERRO CRÍTICO: Combinação de flags não encontrada para ride_id {row_oltp.get('id')}: {flags_tuple}")
        raise ValueError(f"Combinação de flags não encontrada: {flags_tuple}")

    return ride_flags_sk

def etl_dim_ride_flags():
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Erro de conexão. ETL DimRideFlags abortado.")
        return False

    try:
        print("\n--- DIM_RIDE_FLAGS ---")
        with conn_dw.cursor() as cur:
            cur.execute("TRUNCATE TABLE dim_ride_flags RESTART IDENTITY CASCADE;")
        conn_dw.commit()
        print("dim_ride_flags truncada com sucesso.")

        data_to_load = []

        # Gerar todas as combinações possíveis de flags (2^3 = 8 combinações)
        for combination in itertools.product([False, True], repeat=len(FLAG_NAMES_ORDER)):
            flags_dict = dict(zip(FLAG_NAMES_ORDER, combination))

            # Construir descrição textual
            description_parts = []

            if flags_dict['is_going_to_campus']:
                description_parts.append("Indo Campus")
            else:
                description_parts.append("Não Indo Campus")

            if flags_dict['done']:
                description_parts.append("Carona Finalizada")
            else:
                description_parts.append("Carona Não Finalizada")

            if flags_dict['deleted']:
                description_parts.append("Carona Deletada")
            else:
                description_parts.append("Carona Não Deletada")

            flags_description = ", ".join(description_parts)

            # Adicionar à lista para carregamento
            data_to_load.append(
                (flags_dict['is_going_to_campus'], flags_dict['done'],
                 flags_dict['deleted'], flags_description)
            )

        print(f"Gerados {len(data_to_load)} registros para dim_ride_flags.")

        # Inserir no DW
        insert_query = """
        INSERT INTO dim_ride_flags (
            is_going_to_campus, done, deleted, flags_description
        ) VALUES (%s, %s, %s, %s);
        """
        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_ride_flags concluída com sucesso.")
        return True

    except Exception as e:
        conn_dw.rollback()
        print(f"Erro durante o ETL da DimRideFlags: {e}")
        return False
    finally:
        if conn_dw: conn_dw.close()

# Exemplo de como chamar (para teste individual)
if __name__ == "__main__":
    etl_dim_ride_flags()
