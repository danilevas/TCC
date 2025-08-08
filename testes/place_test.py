import pandas as pd

# Criando os DataFrames de exemplo
df_hubs = pd.DataFrame({
    'institution': ['UERJ', 'UERJ', 'UNIGRANRIO'],
    'campus': ['Maracanã', 'Maracanã', 'Duque de Caxias'],
    'hub': ['Casa do Caseiro', 'Escritório do Reitor', 'Prédio 1']
})

df_neighborhoods = pd.DataFrame({
    'neighborhood': ['Tijuca', 'Grajaú', 'Leblon'],
    'zone': ['Norte', 'Norte', 'Sul']
})

# Criando a coluna 'tipo' em cada DataFrame antes de concatenar
df_hubs['tipo'] = 'hub'
df_neighborhoods['tipo'] = 'neighborhood'

# Concatenando os DataFrames
df_final = pd.concat([df_hubs, df_neighborhoods], ignore_index=True)

# Jogando a coluna tipo pro início do df_final
cols = ['tipo'] + [col for col in df_final.columns if col != 'tipo']
df_final = df_final[cols]

print(df_final)