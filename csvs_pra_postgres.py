import pandas as pd
from sqlalchemy import create_engine
import os
import chardet

# Configurações de acesso ao seu banco PostgreSQL
usuario = 'dlevacov@gmail.com'
senha = 'mcpostgresnosanos80'
host = 'localhost'
porta = '5432'
banco = 'meu-caronae'

# Caminho onde estão seus arquivos CSV
pasta_csvs = 'D:/Daniel/UFRJ/TCC/Tabelas Banco/'

# Conexão com o PostgreSQL
engine = create_engine(f'postgresql://{usuario}:{senha}@{host}:{porta}/{banco}')

# Função para detectar encoding de um arquivo
def detectar_encoding(caminho_arquivo, n_bytes=10000):
    with open(caminho_arquivo, 'rb') as f:
        resultado = chardet.detect(f.read(n_bytes))
        return resultado['encoding']

# Loop pelos arquivos CSV
for nome_arquivo in os.listdir(pasta_csvs):
    if nome_arquivo.endswith('.csv'):
        caminho_arquivo = os.path.join(pasta_csvs, nome_arquivo)
        nome_tabela = os.path.splitext(nome_arquivo)[0].lower()

        print(f'\n📄 Importando {nome_arquivo} → tabela {nome_tabela}')

        # Detecta a codificação do arquivo
        encoding = detectar_encoding(caminho_arquivo)
        print(f'📌 Codificação detectada: {encoding}')

        # Lê o arquivo CSV com tratamento de valores nulos
        df = pd.read_csv(
            caminho_arquivo,
            sep='|',
            encoding=encoding,
            na_values=["", " ", "NULL", "null", "NaN", "nan"],
            keep_default_na=True
        )

        # Substitui NaNs por None (compatível com SQL)
        df = df.where(pd.notnull(df), None)

        for i, row in df.iterrows():
            try:
                row.to_frame().T.to_sql(nome_tabela, engine, index=False, if_exists='append')
            except Exception as e:
                print(f'Erro ao inserir linha {i}:', e)
                print('Dados da linha:', row)
                break

print('\n✅ Todas as tabelas foram importadas com sucesso!')
