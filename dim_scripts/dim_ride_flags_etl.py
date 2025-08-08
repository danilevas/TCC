# dim_scripts/dim_ride_flags_etl.py
import itertools
from config import DB_DW
from utils import connect_to_db
from psycopg2.extras import execute_batch
import pandas as pd

# Mapeamento para os dias da semana (1=Segunda, ..., 7=Domingo) JÁ CHEQUEI E É ISSO MESMO
# Isso deve ser consistente com o que foi usado na pré-população da dim_ride_flags
DAY_NUM_TO_FLAG_COL = {
    1: 'is_routine_monday',
    2: 'is_routine_tuesday',
    3: 'is_routine_wednesday',
    4: 'is_routine_thursday',
    5: 'is_routine_friday',
    6: 'is_routine_saturday',
    7: 'is_routine_sunday'
}

# Ordem das flags para lookup na dim_ride_flags (DEVE SER A MESMA DA CRIAÇÃO DA DIMENSÃO SUCATA)
FLAG_NAMES_ORDER = [
    'has_requests',
    'is_routine_ride',
    'is_going_to_campus',
    'done',
    'is_routine_monday',
    'is_routine_tuesday',
    'is_routine_wednesday',
    'is_routine_thursday',
    'is_routine_friday',
    'is_routine_saturday',
    'is_routine_sunday',
    'ride_creation_request',
    'request_accepted',
    'request_refused',
    'request_pending',
    'request_quit'
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

    # 2. Derivar is_routine_ride, is_going_to_campus e done diretamente do OLTP
    # Usar .get para segurança e pd.notna para tratar NaN/None de forma robusta

    if pd.notna(row_oltp.get('has_requests')) and row_oltp['has_requests'] is True:
        flags['has_requests'] = True

    if pd.notna(row_oltp.get('is_routine_ride')) and row_oltp['is_routine_ride'] is True:
        flags['is_routine_ride'] = True
    
    if pd.notna(row_oltp.get('is_going_to_campus')) and row_oltp['is_going_to_campus'] is True:
        flags['is_going_to_campus'] = True

    if pd.notna(row_oltp.get('done')) and row_oltp['done'] is True:
        flags['done'] = True
    
    # 4. Derivar flags dos dias da semana (is_routine_monday, etc.)
    # Apenas se a carona for de rotina E tiver dados válidos em week_days
    if flags['is_routine_ride'] and pd.notna(row_oltp.get('week_days')):
        week_days_str = str(row_oltp['week_days'])
        try:
            day_numbers = [int(d.strip()) for d in week_days_str.split(',') if d.strip()]
            for day_num in day_numbers:
                col_name = DAY_NUM_TO_FLAG_COL.get(day_num)
                if col_name: # Se o número do dia for válido (1-7)
                    flags[col_name] = True
                # else: Ignorar números de dia inválidos (fora do range 1-7)
        except ValueError:
            # week_days_str estava malformada (ex: "abc"). Flags de dia permanecem FALSE.
            print(f"Aviso: 'week_days' mal formatado para ride_id {row_oltp.get('id')}: '{week_days_str}'. Flags de dia definidas como FALSE.")

    # 5. Derivar flags de status de pedido
    # Apenas se a carona tiver pedidos
    if flags['has_requests']:
        # Temos essa cadeia de elifs porque cada pedido pode ter apenas um status
        if pd.notna(row_oltp.get('ride_creation_request')) and row_oltp['ride_creation_request'] is True:
            flags['ride_creation_request'] = True
        
        elif pd.notna(row_oltp.get('request_accepted')) and row_oltp['request_accepted'] is True:
            flags['request_accepted'] = True

        elif pd.notna(row_oltp.get('request_refused')) and row_oltp['request_refused'] is True:
            flags['request_refused'] = True

        elif pd.notna(row_oltp.get('request_pending')) and row_oltp['request_pending'] is True:
            flags['request_pending'] = True

        elif pd.notna(row_oltp.get('request_quit')) and row_oltp['request_quit'] is True:
            flags['request_quit'] = True

    # 6. Converter o dicionário de flags para uma tupla na ordem correta para o lookup
    flags_tuple = tuple(flags[name] for name in FLAG_NAMES_ORDER)

    # 7. Lookup no dicionário em memória
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
        print("Truncando dim_ride_flags antes da pré-população...")
        with conn_dw.cursor() as cur:
            cur.execute("TRUNCATE TABLE dim_ride_flags RESTART IDENTITY CASCADE;")
        conn_dw.commit()
        print("dim_ride_flags truncada com sucesso.")

        data_to_load = []

        # Flags independentes (is_routine_ride, is_going_to_campus, done)
        independent_flags_names = [
            'has_requests',
            'is_routine_ride',
            'is_going_to_campus',
            'done'
        ]

        # Flags dos dias da semana
        day_flags_names = [
            'is_routine_monday', 'is_routine_tuesday', 'is_routine_wednesday',
            'is_routine_thursday', 'is_routine_friday', 'is_routine_saturday', 'is_routine_sunday'
        ]

        # Flags dos status de pedido
        status_flags_names = [
            'ride_creation_request', 'request_accepted', 'request_refused', 'request_pending', 'request_quit'
        ]


        # Gerar combinações para as flags independentes
        for independent_combination in itertools.product([False, True], repeat=len(independent_flags_names)):
            base_flags_dict = dict(zip(independent_flags_names, independent_combination))

            # Se a carona não for de rotina, as flags de dia da semana são sempre FALSE
            if not base_flags_dict['is_routine_ride']:
                middle_flags_dict = base_flags_dict.copy()
                for day_flag in day_flags_names:
                    middle_flags_dict[day_flag] = False # Dias da semana são FALSE

                # Construir descrição para caronas NÃO ROTINEIRAS
                description_parts = ["Não Rotina"]

            # Se a carona for de rotina, gerar todas as combinações de dias da semana
            else: # is_routine_ride is True
                for day_combination in itertools.product([False, True], repeat=len(day_flags_names)):
                    middle_flags_dict = base_flags_dict.copy()
                    middle_flags_dict.update(dict(zip(day_flags_names, day_combination)))

                    # Construir descrição para caronas ROTINEIRAS
                    description_parts = ["Rotina"]
                    days = []
                    if middle_flags_dict['is_routine_monday']: days.append("Seg")
                    if middle_flags_dict['is_routine_tuesday']: days.append("Ter")
                    if middle_flags_dict['is_routine_wednesday']: days.append("Qua")
                    if middle_flags_dict['is_routine_thursday']: days.append("Qui")
                    if middle_flags_dict['is_routine_friday']: days.append("Sex")
                    if middle_flags_dict['is_routine_saturday']: days.append("Sab")
                    if middle_flags_dict['is_routine_sunday']: days.append("Dom")
                    
                    if days:
                        description_parts.append(f"({','.join(days)})")
                    else:
                        # Este caso cobre 'is_routine_ride'=True mas nenhum dia selecionado,
                        # o que ocorreria se o week_days no OLTP estivesse vazio ou malformado.
                        description_parts.append("(Dias não especificados)")

            if middle_flags_dict['is_going_to_campus']:
                description_parts.append("Indo Campus")
            else:
                description_parts.append("Não Indo Campus")
            if middle_flags_dict['done']:
                description_parts.append("Carona Finalizada")
            else:
                description_parts.append("Carona Não Finalizada")
            flags_description = ", ".join(description_parts)

            # Se a carona não tiver pedidos
            if not middle_flags_dict['has_requests']:
                final_flags_dict = middle_flags_dict.copy()
                for status_flag in status_flags_names:
                    final_flags_dict[status_flag] = False # Status são FALSE

                # Construir descrição para caronas SEM PEDIDOS
                description_parts.append(["Carona sem Pedidos"])
            
            # Se a carona tiver pedidos
            else:
                for status_combination in itertools.product([False, True], repeat=len(status_flags_names)):
                    final_flags_dict = middle_flags_dict.copy()
                    final_flags_dict.update(dict(zip(status_flags_names, status_combination)))

                    # Construir descrição para caronas COM PEDIDOS
                    description_parts.append(["Com Pedidos"])
                    days = []
                    if final_flags_dict['ride_creation_request']:
                        description_parts.append("Pedido de Criação de Carona")
                    elif final_flags_dict['request_accepted']:
                        description_parts.append("Pedido Aceito")
                    elif final_flags_dict['request_refused']:
                        description_parts.append("Pedido Recusado")
                    elif final_flags_dict['request_pending']:
                        description_parts.append("Pedido Pendente")
                    elif final_flags_dict['request_quit']:
                        description_parts.append("Desistência do Pedido")
            
            # Adicionar à lista para carregamento
            data_to_load.append(
                (final_flags_dict['is_routine_ride'], final_flags_dict['is_going_to_campus'],
                    final_flags_dict['done'],
                    final_flags_dict['is_routine_monday'], final_flags_dict['is_routine_tuesday'],
                    final_flags_dict['is_routine_wednesday'], final_flags_dict['is_routine_thursday'],
                    final_flags_dict['is_routine_friday'], final_flags_dict['is_routine_saturday'],
                    final_flags_dict['is_routine_sunday'], flags_description)
            )
        
        print(f"Gerados {len(data_to_load)} registros logicamente válidos para dim_ride_flags.")

        # A ordem dos %s deve corresponder à ordem das colunas no INSERT
        insert_query = """
        INSERT INTO dim_ride_flags (
            has_requests, is_routine_ride, is_going_to_campus, done,
            is_routine_monday, is_routine_tuesday, is_routine_wednesday,
            is_routine_thursday, is_routine_friday, is_routine_saturday, is_routine_sunday,
            ride_creation_request, request_accepted, request_refused, request_pending, request_quit,
            flags_description
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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