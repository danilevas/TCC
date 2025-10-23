# config.py

# Configurações para o banco de dados OLTP (origem)
DB_OLTP = {
    'host': 'localhost',
    'database': 'caronae_oltp_2020', # Estou pegando os dados mais atualizados!
    'user': 'postgres',
    'password': 'mcpostgresnosanos80',
    'port': '5432'
}

# Configurações para o banco de dados DW (destino)
DB_DW = {
    'host': 'localhost',
    'database': 'caronae_dw', # Nome do Data Warehouse
    'user': 'postgres',
    'password': 'mcpostgresnosanos80',
    'port': '5432'
}

# Arquivo para armazenar a última data de execução do ETL para cargas incrementais
LAST_RUN_FILE = "last_etl_run.txt"