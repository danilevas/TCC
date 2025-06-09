# config.py

# Configurações para o banco de dados OLTP (origem)
DB_OLTP = {
    'host': 'localhost',
    'database': 'caronae_oltp', # Nome do seu banco de dados transacional Caronaê
    'user': 'postgres',
    'password': 'mcpostgresnosanos80',
    'port': '5432'
}

# Configurações para o banco de dados DW (destino)
DB_DW = {
    'host': 'localhost',
    'database': 'caronae_dw',    # Nome do seu novo banco de dados Data Warehouse
    'user': 'postgres',
    'password': 'mcpostgresnosanos80',
    'port': '5432'
}

# Arquivo para armazenar a última data de execução do ETL para cargas incrementais
LAST_RUN_FILE = "last_etl_run.txt"