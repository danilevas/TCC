import pandas as pd

# Caminho onde estão seus arquivos CSV
pasta_csvs = 'D:/Daniel/UFRJ/TCC/Tabelas Banco/'

try:
    df = pd.read_csv(
        pasta_csvs + "campi.csv",
        sep='|',
        encoding="utf-8",
        na_values=["", " ", "NULL", "null", "NaN", "nan"],
        keep_default_na=True
    )
    print(df.head(10))
except Exception as e:
    print("Erro ao ler CSV:", e)