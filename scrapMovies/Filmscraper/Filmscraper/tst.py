import pandas as pd
import sys
import json

# Charger les données d'entrée depuis stdin
data = json.load(sys.stdin)

# Transformer les données
df = pd.DataFrame(data)
df['duree_minutes'] = df['duree'].apply(lambda x: convert_duration_to_minutes(x))
df['press_rate'] = df['press_rate'].str.replace(',', '.').astype(float)
df['spectateur_rate'] = df['spectateur_rate'].str.replace(',', '.').astype(float)

# Supprimer la colonne 'duree'
df.drop(columns=['duree'], inplace=True)

# Exporter le résultat en JSON
print(df.to_json(orient='records'))

def convert_duration_to_minutes(duration):
    hours, minutes = map(int, duration.replace('h', ' ').replace('min', '').split())
    return hours * 60 + minutes
