# utils.py
import psycopg2
from config import DB_OLTP, DB_DW

def connect_to_db(db_config):
    """
    Estabelece uma conexão com o banco de dados.
    """
    try:
        conn = psycopg2.connect(**db_config)
        print(f"Conexão bem-sucedida ao banco de dados: {db_config['database']}")
        return conn
    except psycopg2.Error as e:
        print(f"Erro ao conectar ao banco de dados {db_config['database']}: {e}")
        return None

def execute_sql(conn, sql_query, fetch_results=False):
    """
    Executa uma query SQL no banco de dados.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            if fetch_results:
                return cur.fetchall()
            conn.commit() # Commita as alterações se for um DDL ou DML
            return True
    except psycopg2.Error as e:
        print(f"Erro ao executar SQL: {e}")
        conn.rollback() # Faz rollback em caso de erro
        return False
    except Exception as e:
        print(f"Erro inesperado ao executar SQL: {e}")
        conn.rollback()
        return False

def get_latest_timestamp(conn_dw, table_name, timestamp_column):
    """
    Obtém o timestamp mais recente de uma coluna de uma tabela no DW.
    Usado para carga incremental.
    """
    query = f"SELECT MAX({timestamp_column}) FROM {table_name};"
    try:
        with conn_dw.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()[0]
            if result:
                return result
            return None
    except psycopg2.Error as e:
        print(f"Erro ao obter o último timestamp para {table_name}: {e}")
        return None