from datetime import datetime
import pandas as pd

start_date = datetime(2016, 4, 1)
end_date = datetime(2030, 12, 31)

dates = pd.date_range(start=start_date, end=end_date, freq='D')

data_to_load = []
for dt in dates:
    period = f"{dt.year}.{(dt.month - 1) // 7 + 1}"
    if dt.day == 25:
        print(f"{dt.date()} = {period}")