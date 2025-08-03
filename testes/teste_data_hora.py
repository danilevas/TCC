import pandas as pd
from datetime import time

data_to_load = []

# Para cada dia, gerar dados para todas as horas/minutos
for hour in range(24):
    for minute in range(60):
        time_dt = time(hour=hour, minute=minute)
        hour_sk = int(time_dt.strftime('%H%M')) # ex: 1435 para 14:35
        hour_of_day = time_dt.hour
        minute_of_hour = time_dt.minute
        
        if 0 <= hour_of_day < 6:
            time_bucket = 'Madrugada'
        elif 6 <= hour_of_day < 12:
            time_bucket = 'Manhã'
        elif 12 <= hour_of_day < 18:
            time_bucket = 'Tarde'
        else:
            time_bucket = 'Noite'

        data_to_load.append((hour_sk, hour_of_day, minute_of_hour, time_bucket))

print(data_to_load)