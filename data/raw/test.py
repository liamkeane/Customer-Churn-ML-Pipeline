import pandas as pd

df = pd.read_csv("demographics.csv")
df = df.rename(columns={'Number of Dependents': 'NOD'})
print(df.head())