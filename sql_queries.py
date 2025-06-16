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
    has_car BOOLEAN,
    car_model VARCHAR(100),
    car_color VARCHAR(50),
    car_plate VARCHAR(20),
    user_location VARCHAR(255),
    cpf VARCHAR(20),
    is_banned BOOLEAN,
    institution_name VARCHAR(255)
);
"""

CREATE_DIM_NEIGHBORHOOD_TABLE = """
CREATE TABLE IF NOT EXISTS dim_neighborhood (
    neighborhood_sk SERIAL PRIMARY KEY,
    neighborhood_id INT UNIQUE NOT NULL,
    neighborhood_name VARCHAR(100) NOT NULL,
    distance_to_fundao NUMERIC(10, 2),
    zone_name VARCHAR(100) -- Desnormalizado de DimZone
);
"""

CREATE_DIM_HUB_TABLE = """
CREATE TABLE IF NOT EXISTS dim_hub (
    hub_sk SERIAL PRIMARY KEY,
    hub_id INT UNIQUE NOT NULL,
    hub_name VARCHAR(100) NOT NULL,
    campus_name VARCHAR(100) NOT NULL, -- Desnormalizado de DimCampi
    campus_color VARCHAR(50)
);
"""

CREATE_DIM_STATUS_PEDIDO_TABLE = """
CREATE TABLE IF NOT EXISTS dim_status_pedido (
    status_sk SERIAL PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE NOT NULL
);
"""

# DDLs para as tabelas de fatos
CREATE_FACT_CARONA_TABLE = """
CREATE TABLE IF NOT EXISTS fato_carona (
    ride_pk SERIAL PRIMARY KEY, -- Chave primária para o fato
    ride_id INT UNIQUE NOT NULL, -- Chave de negócio original da carona
    driver_user_sk INT NOT NULL,
    zone_sk INT NOT NULL,
    neighborhood_sk INT NOT NULL,
    hub_sk INT NOT NULL,
    date_sk INT NOT NULL,
    hour_sk INT NOT NULL, -- Para hora e minuto da criação da carona
    is_going_to_campus BOOLEAN,
    slots INT,
    is_routine_ride BOOLEAN,
    requests_count INT DEFAULT 0,
    accepted_requests_count INT DEFAULT 0,
    refused_requests_count INT DEFAULT 0,
    pending_requests_count INT DEFAULT 0,
    quit_requests_count INT DEFAULT 0,
    messages_count INT DEFAULT 0,
    description TEXT,
    done BOOLEAN, -- Incluir com cautela se for usar, dada a sua baixa confiabilidade
    created_at TIMESTAMP, -- Para controle do ETL, marca d'água
    updated_at TIMESTAMP, -- Para controle do ETL, marca d'água

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
    is_driver_interaction BOOLEAN,
    is_passenger_request BOOLEAN,
    request_accepted BOOLEAN,
    request_refused BOOLEAN,
    request_pending BOOLEAN,
    request_quit BOOLEAN,
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

ALL_DDL_DROP_QUERIES = [
    DROP_FACT_CARONA_TABLE,
    DROP_FACT_INTERACAO_CARONA_TABLE,
    DROP_DIM_TIME_TABLE,
    DROP_DIM_USER_TABLE,
    DROP_DIM_NEIGHBORHOOD_TABLE,
    DROP_DIM_HUB_TABLE,
    DROP_DIM_STATUS_PEDIDO_TABLE
]

ALL_DDL_CREATE_QUERIES = [
    CREATE_DIM_TIME_TABLE,
    CREATE_DIM_USER_TABLE,
    CREATE_DIM_NEIGHBORHOOD_TABLE,
    CREATE_DIM_HUB_TABLE,
    CREATE_DIM_STATUS_PEDIDO_TABLE,
    CREATE_FACT_CARONA_TABLE,
    CREATE_FACT_INTERACAO_CARONA_TABLE
]

ALL_DDL_DROP_QUERIES_MENOS_TEMPO = [
    DROP_FACT_CARONA_TABLE,
    DROP_FACT_INTERACAO_CARONA_TABLE,
    DROP_DIM_USER_TABLE,
    DROP_DIM_NEIGHBORHOOD_TABLE,
    DROP_DIM_HUB_TABLE,
    DROP_DIM_STATUS_PEDIDO_TABLE
]

ALL_DDL_CREATE_QUERIES_MENOS_TEMPO = [
    CREATE_DIM_USER_TABLE,
    CREATE_DIM_NEIGHBORHOOD_TABLE,
    CREATE_DIM_HUB_TABLE,
    CREATE_DIM_STATUS_PEDIDO_TABLE,
    CREATE_FACT_CARONA_TABLE,
    CREATE_FACT_INTERACAO_CARONA_TABLE
]