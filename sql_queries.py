# sql_queries.py

# DDLs para as tabelas de dimensão
CREATE_DIM_TIME_TABLE = """
CREATE TABLE IF NOT EXISTS dim_time (
    date_sk INT NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    semester INT NOT NULL,
    year INT NOT NULL,
    hour_sk INT NOT NULL,
    hour_of_day INT NOT NULL,
    minute_of_hour INT NOT NULL,
    time_of_day_bucket VARCHAR(50) NOT NULL,
    -- Adicione uma restrição de unicidade para a combinação (date_sk, hour_sk)
    -- Isso torna a combinação única, permitindo que seja referenciada por FK
    PRIMARY KEY (date_sk, hour_sk)
    -- UNIQUE (date_sk, hour_sk)
);
"""

CREATE_DIM_USER_TABLE = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL, -- Chave de negócio original
    user_name VARCHAR(255),
    profile VARCHAR(50),
    course VARCHAR(100),
    phone_number VARCHAR(100),
    email VARCHAR(255),
    has_car BOOLEAN NOT NULL,
    car_model VARCHAR(100),
    car_color VARCHAR(50),
    car_plate VARCHAR(20),
    user_location VARCHAR(255),
    cpf VARCHAR(20),
    app_platform VARCHAR(255),
    app_version VARCHAR(255),
    is_banned BOOLEAN NOT NULL,
    institution_id INT UNIQUE NOT NULL,    
    institution_name VARCHAR(255),
    institution_color VARCHAR(10),
    u.created_at TIMESTAMP,
    u.updated_at TIMESTAMP,
    u.deleted_at TIMESTAMP
);
"""

CREATE_DIM_NEIGHBORHOOD_TABLE = """
CREATE TABLE IF NOT EXISTS dim_neighborhood (
    neighborhood_sk SERIAL PRIMARY KEY,
    neighborhood_id INT UNIQUE NOT NULL,
    neighborhood_name VARCHAR(100) NOT NULL,
    distance_to_fundao NUMERIC(10, 2),
    zone_id INT UNIQUE NOT NULL,
    zone_name VARCHAR(100) -- Desnormalizado de DimZone
    zone_color VARCHAR(10) -- Desnormalizado de DimZone
);
"""

CREATE_DIM_HUB_TABLE = """
CREATE TABLE IF NOT EXISTS dim_hub (
    hub_sk SERIAL PRIMARY KEY,
    hub_id INT UNIQUE NOT NULL,
    hub_name VARCHAR(100) NOT NULL,
    center VARCHAR(100),
    campus_id INT UNIQUE NOT NULL,
    campus_name VARCHAR(100) NOT NULL, -- Desnormalizado de DimCampi
    campus_color VARCHAR(10),
    campus_created_at TIMESTAMP,
    campus_updated_at TIMESTAMP,
    institution_id INT UNIQUE NOT NULL,
    institution_name VARCHAR(255),
    institution_created_at TIMESTAMP,
    institution_updated_at TIMESTAMP
);
"""

CREATE_DIM_STATUS_PEDIDO_TABLE = """
CREATE TABLE IF NOT EXISTS dim_status_pedido (
    status_sk SERIAL PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE NOT NULL
);
"""

# Dimensão sucata
CREATE_DIM_FLAGS_CARONA_TABLE = """
CREATE TABLE IF NOT EXISTS dim_flags_carona (
    flags_carona_sk SERIAL PRIMARY KEY,
    is_routine_ride BOOLEAN NOT NULL,
    is_going_to_campus BOOLEAN NOT NULL,
    done BOOLEAN NOT NULL,
    is_routine_monday BOOLEAN NOT NULL,
    is_routine_tuesday BOOLEAN NOT NULL,
    is_routine_wednesday BOOLEAN NOT NULL,
    is_routine_thursday BOOLEAN NOT NULL,
    is_routine_friday BOOLEAN NOT NULL,
    is_routine_saturday BOOLEAN NOT NULL,
    is_routine_sunday BOOLEAN NOT NULL,
    flags_description VARCHAR(255) UNIQUE -- Para facilitar a visualização e garantir unicidade da combinação textual
);
"""

# DDLs para as tabelas de fatos
CREATE_FACT_CARONA_TABLE = """
CREATE TABLE IF NOT EXISTS fato_carona (
    ride_pk SERIAL PRIMARY KEY, -- Chave primária para o fato
    ride_id INT UNIQUE NOT NULL, -- Chave de negócio original da carona
    driver_user_sk INT NOT NULL,
    neighborhood_sk INT NOT NULL,
    hub_sk INT NOT NULL,
    date_sk INT NOT NULL,
    hour_sk INT NOT NULL,
    flags_carona_sk INT NOT NULL,
    routine_id INT NOT NULL,
    slots INT,
    repeats_until TIMESTAMP,
    requests_count INT DEFAULT 0,
    accepted_requests_count INT DEFAULT 0,
    refused_requests_count INT DEFAULT 0,
    pending_requests_count INT DEFAULT 0,
    quit_requests_count INT DEFAULT 0,
    messages_count INT DEFAULT 0,
    created_at TIMESTAMP, -- Para controle do ETL, marca d'água
    updated_at TIMESTAMP, -- Para controle do ETL, marca d'água
    deleted_at TIMESTAMP, -- Para controle do ETL, marca d'água

    FOREIGN KEY (driver_user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (zone_sk) REFERENCES dim_zone(zone_sk),
    FOREIGN KEY (neighborhood_sk) REFERENCES dim_neighborhood(neighborhood_sk),
    FOREIGN KEY (hub_sk) REFERENCES dim_hub(hub_sk),
    -- A FK deve referenciar a combinação única (date_sk, hour_sk)
    FOREIGN KEY (date_sk, hour_sk) REFERENCES dim_time(date_sk, hour_sk)
);
"""

CREATE_FACT_INTERACAO_CARONA_TABLE = """
CREATE TABLE IF NOT EXISTS fato_interacao_carona (
    interaction_pk SERIAL PRIMARY KEY, -- Chave primária para o fato
    ride_user_id INT UNIQUE NOT NULL, -- Chave de negócio original da ride_user
    ride_id INT NOT NULL, -- ID da carona a que se refere (pode ser FK para fato_carona.ride_id)
    user_sk INT NOT NULL, -- Usuário que fez a interação (motorista ou caronista)
    date_sk INT NOT NULL,
    hour_sk INT NOT NULL, -- Para hora e minuto da interação
    status_sk INT NOT NULL, -- Status final da interação
    is_driver_interaction BOOLEAN NOT NULL,
    is_passenger_request BOOLEAN NOT NULL,
    request_accepted BOOLEAN NOT NULL,
    request_refused BOOLEAN NOT NULL,
    request_pending BOOLEAN NOT NULL,
    request_quit BOOLEAN NOT NULL,
    created_at TIMESTAMP, -- Para controle do ETL, marca d'água
    updated_at TIMESTAMP, -- Para controle do ETL, marca d'água

    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    -- A FK deve referenciar a combinação única (date_sk, hour_sk)
    FOREIGN KEY (date_sk, hour_sk) REFERENCES dim_time(date_sk, hour_sk),
    FOREIGN KEY (status_sk) REFERENCES dim_status_pedido(status_sk)
);
"""

# DDL - DROP TABLES (em ordem para evitar problemas de dependência)
DROP_FACT_CARONA_TABLE = "DROP TABLE IF EXISTS fato_carona CASCADE;"
DROP_FACT_INTERACAO_CARONA_TABLE = "DROP TABLE IF EXISTS fato_interacao_carona CASCADE;"
DROP_DIM_TIME_TABLE = "DROP TABLE IF EXISTS dim_time CASCADE;"
DROP_DIM_USER_TABLE = "DROP TABLE IF EXISTS dim_user CASCADE;"
DROP_DIM_NEIGHBORHOOD_TABLE = "DROP TABLE IF EXISTS dim_neighborhood CASCADE;"
DROP_DIM_HUB_TABLE = "DROP TABLE IF EXISTS dim_hub CASCADE;"
DROP_DIM_STATUS_PEDIDO_TABLE = "DROP TABLE IF EXISTS dim_status_pedido CASCADE;"
DROP_DIM_FLAGS_CARONA_TABLE = "DROP TABLE IF EXISTS dim_flags_carona CASCADE;"

ALL_DDL_DROP_QUERIES = [
    DROP_FACT_CARONA_TABLE,
    DROP_FACT_INTERACAO_CARONA_TABLE,
    DROP_DIM_TIME_TABLE,
    DROP_DIM_USER_TABLE,
    DROP_DIM_NEIGHBORHOOD_TABLE,
    DROP_DIM_HUB_TABLE,
    DROP_DIM_STATUS_PEDIDO_TABLE,
    DROP_DIM_FLAGS_CARONA_TABLE
]

ALL_DDL_CREATE_QUERIES = [
    CREATE_DIM_TIME_TABLE,
    CREATE_DIM_USER_TABLE,
    CREATE_DIM_NEIGHBORHOOD_TABLE,
    CREATE_DIM_HUB_TABLE,
    CREATE_DIM_STATUS_PEDIDO_TABLE,
    CREATE_DIM_FLAGS_CARONA_TABLE,
    CREATE_FACT_CARONA_TABLE,
    CREATE_FACT_INTERACAO_CARONA_TABLE
]

# Retorna as queries de DDL corretamente
def get_queries(recria_dim_time, recria_dim_flags_carona):
    # Se não quisermos recriar a tabela dim_time, não a dropamos nem a criamos
    if not recria_dim_time:
        ALL_DDL_DROP_QUERIES.remove(DROP_DIM_TIME_TABLE)
        ALL_DDL_CREATE_QUERIES.remove(CREATE_DIM_TIME_TABLE)
    
    # Se não quisermos recriar a tabela dim_flags_carona, não a dropamos nem a criamos
    if not recria_dim_flags_carona:
        ALL_DDL_DROP_QUERIES.remove(DROP_DIM_FLAGS_CARONA_TABLE)
        ALL_DDL_CREATE_QUERIES.remove(CREATE_DIM_FLAGS_CARONA_TABLE)
    
    return ALL_DDL_DROP_QUERIES, ALL_DDL_CREATE_QUERIES