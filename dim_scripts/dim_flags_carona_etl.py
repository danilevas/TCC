# dim_scripts/dim_flags_carona_etl.py
import itertools
from config import DB_DW
from utils import connect_to_db
from psycopg2.extras import execute_batch

# Mapeamento para os dias da semana (1=Segunda, ..., 7=Domingo) JÁ CHEQUEI E É ISSO MESMO
# Isso deve ser consistente com o que foi usado na pré-população da dim_carona_flags
DAY_NUM_TO_FLAG_COL = {
    1: 'is_routine_monday',
    2: 'is_routine_tuesday',
    3: 'is_routine_wednesday',
    4: 'is_routine_thursday',
    5: 'is_routine_friday',
    6: 'is_routine_saturday',
    7: 'is_routine_sunday'
}

# Ordem das flags para lookup na dim_carona_flags (DEVE SER A MESMA DA CRIAÇÃO DA DIMENSÃO SUCATA)
FLAG_NAMES_ORDER = [
    'is_routine_ride',
    'is_going_to_campus',
    'has_description', # Nova flag derivada da descrição
    'is_routine_monday',
    'is_routine_tuesday',
    'is_routine_wednesday',
    'is_routine_thursday',
    'is_routine_friday',
    'is_routine_saturday',
    'is_routine_sunday'
]

# Variável global para armazenar o lookup da dim_carona_flags
# Será populada uma vez por execução do ETL
_CARONA_FLAGS_LOOKUP_DICT = {}

def load_carona_flags_lookup(conn_dw):
    """Carrega a dim_carona_flags para um dicionário de lookup em memória."""
    global _CARONA_FLAGS_LOOKUP_DICT

    if _CARONA_FLAGS_LOOKUP_DICT: # Já carregado
        return

    print("Carregando dim_carona_flags para lookup em memória...")
    try:
        query = f"SELECT {', '.join(FLAG_NAMES_ORDER)}, carona_flags_sk FROM dim_carona_flags;"
        flags_df = pd.read_sql(query, conn_dw)

        # Cria o dicionário de lookup: (True, False, ..., True) -> carona_flags_sk
        _CARONA_FLAGS_LOOKUP_DICT = {
            tuple(row[col] for col in FLAG_NAMES_ORDER): row['carona_flags_sk']
            for index, row in flags_df.iterrows()
        }
        print(f"dim_carona_flags carregada: {len(_CARONA_FLAGS_LOOKUP_DICT)} combinações.")
    except Exception as e:
        print(f"Erro ao carregar dim_carona_flags para lookup: {e}")
        _CARONA_FLAGS_LOOKUP_DICT = {} # Limpa em caso de erro para tentar novamente se necessário


def derive_and_lookup_flags(row_oltp):
    """
    Deriva as flags booleanas para uma carona do OLTP e busca o carona_flags_sk correspondente.
    """
    # 1. Inicializar todas as flags como FALSE (estado padrão/desconhecido)
    flags = {name: False for name in FLAG_NAMES_ORDER}

    # 2. Derivar is_routine_ride e is_going_to_campus diretamente do OLTP
    # Usar .get para segurança e pd.notna para tratar NaN/None de forma robusta
    if pd.notna(row_oltp.get('is_routine_ride')) and row_oltp['is_routine_ride'] is True:
        flags['is_routine_ride'] = True
    
    if pd.notna(row_oltp.get('is_going_to_campus')) and row_oltp['is_going_to_campus'] is True:
        flags['is_going_to_campus'] = True
    
    # 3. Derivar has_description
    if pd.notna(row_oltp.get('description')) and str(row_oltp['description']).strip() != '':
        flags['has_description'] = True
    
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
    
    # 5. Converter o dicionário de flags para uma tupla na ordem correta para o lookup
    flags_tuple = tuple(flags[name] for name in FLAG_NAMES_ORDER)

    # 6. Lookup no dicionário em memória
    carona_flags_sk = _CARONA_FLAGS_LOOKUP_DICT.get(flags_tuple)
    
    if carona_flags_sk is None:
        # Isso NÃO DEVE ACONTECER se a dimensão sucata foi pré-populada corretamente
        # e se a lógica de derivação é consistente com a lógica de pré-população.
        # Se acontecer, indica um erro grave ou nova combinação não esperada.
        # Para robustez, você pode querer um SK para 'combinação desconhecida' aqui.
        print(f"ERRO CRÍTICO: Combinação de flags não encontrada para ride_id {row_oltp.get('id')}: {flags_tuple}")
        # Retornar um SK padrão para "combinação não mapeada" se você tiver um na dimensão
        # Por exemplo, o SK para a combinação 'all false' ou um SK específico para erros.
        # Para este exemplo, assumiremos que todas as combinações existem.
        raise ValueError(f"Combinação de flags não encontrada: {flags_tuple}")

    return carona_flags_sk

def etl_dim_flags_carona():
    conn_dw = connect_to_db(DB_DW)
    if not conn_dw:
        print("Erro de conexão. ETL DimFlagsCarona abortado.")
        return False

    try:
        # Definir as flags na ordem que queremos que apareçam
        # Lembre-se: 'is_routine_ride', 'is_going_to_campus' e 'done' já vêm do OLTP como booleanos
        # Os dias da semana são derivados da coluna week_days
        flag_names = [
            'is_routine_ride',
            'is_going_to_campus',
            'done',
            'is_routine_monday',
            'is_routine_tuesday',
            'is_routine_wednesday',
            'is_routine_thursday',
            'is_routine_friday',
            'is_routine_saturday',
            'is_routine_sunday'
        ]

        data_to_load = []

        # Gerar todas as combinações de True/False para as 10 flags
        for combination in itertools.product([False, True], repeat=len(flag_names)):
            flags_dict = dict(zip(flag_names, combination))

            # Criar uma descrição textual para a combinação
            description_parts = []
            if flags_dict['is_routine_ride']:
                description_parts.append("Rotina")
                # Adicionar dias da semana se for rotina
                days = []
                if flags_dict['is_routine_monday']: days.append("Seg")
                if flags_dict['is_routine_tuesday']: days.append("Ter")
                if flags_dict['is_routine_wednesday']: days.append("Qua")
                if flags_dict['is_routine_thursday']: days.append("Qui")
                if flags_dict['is_routine_friday']: days.append("Sex")
                if flags_dict['is_routine_saturday']: days.append("Sab")
                if flags_dict['is_routine_sunday']: days.append("Dom")
                if days:
                    description_parts.append(f"({','.join(days)})")
                else: # Se for rotina mas nenhum dia especificado (caso improvável, mas para robustez)
                    description_parts.append("(Dias não especificados)")
            else:
                description_parts.append("Não Rotina")

            if flags_dict['is_going_to_campus']:
                description_parts.append("Indo Campus")
            else:
                description_parts.append("Não Indo Campus")
            
            if flags_dict['done']:
                description_parts.append("Carona Finalizada")
            else:
                description_parts.append("Carona Não Finalizada")

            flags_description = ", ".join(description_parts)

            # Adicionar ao lista para carregamento
            data_to_load.append((
                flags_dict['is_routine_ride'],
                flags_dict['is_going_to_campus'],
                flags_dict['done'],
                flags_dict['is_routine_monday'],
                flags_dict['is_routine_tuesday'],
                flags_dict['is_routine_wednesday'],
                flags_dict['is_routine_thursday'],
                flags_dict['is_routine_friday'],
                flags_dict['is_routine_saturday'],
                flags_dict['is_routine_sunday'],
                flags_description
            ))
        
        print(f"Gerados {len(data_to_load)} registros para dim_flags_carona ({2**len(flag_names)} combinações).")

        # Inserir usando execute_batch para melhor performance
        insert_query = """
        INSERT INTO dim_flags_carona (
            is_routine_ride, is_going_to_campus, done,
            is_routine_monday, is_routine_tuesday, is_routine_wednesday,
            is_routine_thursday, is_routine_friday, is_routine_saturday, is_routine_sunday,
            flags_description
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        with conn_dw.cursor() as cur:
            execute_batch(cur, insert_query, data_to_load)
        conn_dw.commit()
        print("Carga da dim_flags_carona concluída com sucesso.")
        return True

    except Exception as e:
        conn_dw.rollback()
        print(f"Erro durante o ETL da DimFlagsCarona: {e}")
        return False
    finally:
        if conn_dw: conn_dw.close()

# Exemplo de como chamar (para teste individual)
if __name__ == "__main__":
    etl_dim_flags_carona()