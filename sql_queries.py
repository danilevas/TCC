# sql_queries.py

# DDLs para as tabelas de dimensão
CREATE_DIM_DATE_TABLE = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    semester INT NOT NULL,
    period VARCHAR(20) NOT NULL,
    year INT NOT NULL,

    PRIMARY KEY (date_sk)
);
"""

CREATE_DIM_HOUR_TABLE = """
CREATE TABLE IF NOT EXISTS dim_hour (
    hour_sk INT NOT NULL,
    hour_of_day INT NOT NULL,
    minute_of_hour INT NOT NULL,
    time_of_day_bucket VARCHAR(50) NOT NULL,

    PRIMARY KEY (hour_sk)
);
"""

CREATE_DIM_USER_TABLE = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL, -- Chave de negócio original
    profile VARCHAR(50),
    course VARCHAR(100),
    phone_number VARCHAR(100),
    email VARCHAR(255),
    has_car BOOLEAN NOT NULL,
    car_model VARCHAR(100),
    car_color VARCHAR(50),
    car_plate VARCHAR(20),
    user_location VARCHAR(255),
    app_platform VARCHAR(255),
    app_version VARCHAR(255),
    is_banned BOOLEAN NOT NULL,
    institution_id INT NOT NULL,    
    institution_name VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);
"""

CREATE_DIM_NEIGHBORHOOD_TABLE = """
CREATE TABLE IF NOT EXISTS dim_neighborhood (
    neighborhood_sk SERIAL PRIMARY KEY,
    neighborhood_id INT UNIQUE NOT NULL,
    neighborhood_name VARCHAR(100) NOT NULL,
    distance_to_fundao NUMERIC(8, 2),
    zone_id INT NOT NULL,
    zone_name VARCHAR(100), -- Desnormalizado de DimZone
    zone_color VARCHAR(10) -- Desnormalizado de DimZone
);
"""

CREATE_DIM_HUB_TABLE = """
CREATE TABLE IF NOT EXISTS dim_hub (
    hub_sk SERIAL PRIMARY KEY,
    hub_id INT UNIQUE NOT NULL,
    hub_name VARCHAR(100) NOT NULL,
    center VARCHAR(100),
    campus_id INT NOT NULL,
    campus_name VARCHAR(100) NOT NULL, -- Desnormalizado de DimCampi
    campus_color VARCHAR(10),
    campus_created_at TIMESTAMP,
    campus_updated_at TIMESTAMP,
    institution_id INT NOT NULL,
    institution_name VARCHAR(255),
    institution_created_at TIMESTAMP,
    institution_updated_at TIMESTAMP
);
"""

CREATE_DIM_REQUEST_STATUS_TABLE = """
CREATE TABLE IF NOT EXISTS dim_request_status (
    status_sk SERIAL PRIMARY KEY,
    status_name VARCHAR(50) UNIQUE NOT NULL
);
"""

# Dimensão sucata
CREATE_DIM_RIDE_FLAGS_TABLE = """
CREATE TABLE IF NOT EXISTS dim_ride_flags (
    ride_flags_sk SERIAL PRIMARY KEY,
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
CREATE_FACT_RIDE_TABLE = """
CREATE TABLE IF NOT EXISTS fact_ride (
    ride_sk SERIAL PRIMARY KEY, -- Chave primária para o fato
    ride_id INT UNIQUE NOT NULL,
    routine_id INT, -- Pensar se deixa mesmo aqui
    
    -- SKs
    driver_user_sk INT NOT NULL,
    neighborhood_sk INT NOT NULL,
    hub_sk INT NOT NULL,
    ride_flags_sk INT NOT NULL,

    -- SKs para o momento de CRIAÇÃO da carona
    creation_date_sk INT NOT NULL,
    creation_hour_sk INT NOT NULL,

    -- SKs para o momento de OCORRÊNCIA da carona
    occurrence_date_sk INT NOT NULL,
    occurrence_hour_sk INT NOT NULL,

    slots INT,
    requests_count INT DEFAULT 0,
    accepted_requests_count INT DEFAULT 0,
    refused_requests_count INT DEFAULT 0,
    pending_requests_count INT DEFAULT 0,
    quit_requests_count INT DEFAULT 0,
    messages_count INT DEFAULT 0,

    repeats_until TIMESTAMP, -- Rotina
    created_at TIMESTAMP, -- Para controle do ETL, marca d'água
    updated_at TIMESTAMP, -- Para controle do ETL, marca d'água
    deleted_at TIMESTAMP -- Para controle do ETL, marca d'água

    -- FKs 
    FOREIGN KEY (driver_user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (neighborhood_sk) REFERENCES dim_neighborhood(neighborhood_sk),
    FOREIGN KEY (hub_sk) REFERENCES dim_hub(hub_sk),
    FOREIGN KEY (ride_flags_sk) REFERENCES dim_ride_flags(ride_flags_sk),

    -- FKs para as dimensões de tempo
    FOREIGN KEY (creation_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (creation_hour_sk) REFERENCES dim_hour(hour_sk),
    FOREIGN KEY (occurrence_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (occurrence_hour_sk) REFERENCES dim_hour(hour_sk)
);
"""

CREATE_FACT_RIDE_INTERACTION_TABLE = """
CREATE TABLE IF NOT EXISTS fact_ride_interaction (
    interaction_pk SERIAL PRIMARY KEY, -- Chave primária para o fato
    ride_user_id INT UNIQUE NOT NULL, -- Chave de negócio original da ride_user

    ride_sk INT NOT NULL, -- ID da carona a que se refere (FK para fact_ride.ride_sk)
    user_sk INT NOT NULL, -- Usuário que fez a interação (motorista ou caronista)
    status_sk INT NOT NULL, -- Status final da interação
    neighborhood_sk INT NOT NULL, -- é isso aí!
    hub_sk INT NOT NULL, -- é isso aí!
    ride_flags_sk INT NOT NULL, -- é isso aí!

    -- SKs para o momento de CRIAÇÃO da interação
    creation_date_sk INT NOT NULL,
    creation_hour_sk INT NOT NULL,

    -- SKs para o momento de ATUALIZAÇÃO da interação
    update_date_sk INT NOT NULL,
    update_hour_sk INT NOT NULL,

    is_driver_interaction BOOLEAN NOT NULL,
    is_passenger_request BOOLEAN NOT NULL,
    request_accepted BOOLEAN NOT NULL,
    request_refused BOOLEAN NOT NULL,
    request_pending BOOLEAN NOT NULL,
    request_quit BOOLEAN NOT NULL,

    created_at TIMESTAMP, -- Para controle do ETL, marca d'água
    updated_at TIMESTAMP, -- Para controle do ETL, marca d'água

    -- FKs
    FOREIGN KEY (ride_sk) REFERENCES fact_ride(ride_sk),
    FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
    FOREIGN KEY (status_sk) REFERENCES dim_request_status(status_sk),
    FOREIGN KEY (neighborhood_sk) REFERENCES dim_neighborhood(neighborhood_sk),
    FOREIGN KEY (hub_sk) REFERENCES dim_hub(hub_sk),
    FOREIGN KEY (ride_flags_sk) REFERENCES dim_ride_flags(ride_flags_sk),
    
    -- FKs para as dimensões de tempo
    FOREIGN KEY (creation_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (creation_hour_sk) REFERENCES dim_hour(hour_sk),
    FOREIGN KEY (update_date_sk) REFERENCES dim_date(date_sk),
    FOREIGN KEY (update_hour_sk) REFERENCES dim_hour(hour_sk)
);
"""

# DDL - DROP TABLES (em ordem para evitar problemas de dependência)
DROP_FACT_RIDE_TABLE = "DROP TABLE IF EXISTS fact_ride CASCADE;"
DROP_FACT_RIDE_INTERACTION_TABLE = "DROP TABLE IF EXISTS fact_ride_interaction CASCADE;"
DROP_DIM_DATE_TABLE = "DROP TABLE IF EXISTS dim_date CASCADE;"
DROP_DIM_HOUR_TABLE = "DROP TABLE IF EXISTS dim_hour CASCADE;"
DROP_DIM_USER_TABLE = "DROP TABLE IF EXISTS dim_user CASCADE;"
DROP_DIM_NEIGHBORHOOD_TABLE = "DROP TABLE IF EXISTS dim_neighborhood CASCADE;"
DROP_DIM_HUB_TABLE = "DROP TABLE IF EXISTS dim_hub CASCADE;"
DROP_DIM_REQUEST_STATUS_TABLE = "DROP TABLE IF EXISTS dim_request_status CASCADE;"
DROP_DIM_RIDE_FLAGS_TABLE = "DROP TABLE IF EXISTS dim_ride_flags CASCADE;"

ALL_DDL_DROP_QUERIES = [
    DROP_FACT_RIDE_TABLE,
    DROP_FACT_RIDE_INTERACTION_TABLE,
    DROP_DIM_DATE_TABLE,
    DROP_DIM_HOUR_TABLE,
    DROP_DIM_USER_TABLE,
    DROP_DIM_NEIGHBORHOOD_TABLE,
    DROP_DIM_HUB_TABLE,
    DROP_DIM_REQUEST_STATUS_TABLE,
    DROP_DIM_RIDE_FLAGS_TABLE
]

ALL_DDL_CREATE_QUERIES = [
    CREATE_DIM_DATE_TABLE,
    CREATE_DIM_HOUR_TABLE,
    CREATE_DIM_USER_TABLE,
    CREATE_DIM_NEIGHBORHOOD_TABLE,
    CREATE_DIM_HUB_TABLE,
    CREATE_DIM_REQUEST_STATUS_TABLE,
    CREATE_DIM_RIDE_FLAGS_TABLE,
    CREATE_FACT_RIDE_TABLE,
    CREATE_FACT_RIDE_INTERACTION_TABLE
]

# Retorna as queries de DDL corretamente
def get_queries():
    return ALL_DDL_DROP_QUERIES, ALL_DDL_CREATE_QUERIES