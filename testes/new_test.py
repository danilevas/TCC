import pandas as pd

# Criando os DataFrames de exemplo
rides_data = pd.DataFrame({
    'is_routine_ride': [True, True, True, False, False, False],
    'routine_id': [1, 1, 1, 3, 6, 7]
})

# Tornar nulos os valores de 'routine_id' onde 'is_routine_ride' é falso
rides_data.loc[~rides_data['is_routine_ride'], 'routine_id'] = pd.NA

print(rides_data)