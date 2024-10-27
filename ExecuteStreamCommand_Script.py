import sys
import pandas as pd
import numpy as np
import json

# Fonction pour convertir la durée en minutes
def convert_duration_to_minutes(duration):
    if isinstance(duration, str):
        try:
            hours, minutes = duration.split('h')
            minutes = minutes.replace('min', '').strip()
            total_minutes = int(hours) * 60 + int(minutes)
            return total_minutes
        except Exception as e:
            print(f"Error converting duration: {e}")
            return np.nan
    return np.nan

# Lecture des données JSON depuis l'entrée standard
input_data = sys.stdin.read()
data = json.loads(input_data)

# Créer un DataFrame à partir des données
df = pd.DataFrame(data)

# Remplacer '--' par NaN
df['press_rate'] = df['press_rate'].replace('--', np.nan)
df['spectateur_rate'] = df['spectateur_rate'].replace('--', np.nan)

# Nettoyer les colonnes de texte
df['title'] = df['title'].str.encode('utf-8', errors='ignore').str.decode('utf-8')
df['histoire'] = df['histoire'].str.encode('utf-8', errors='ignore').str.decode('utf-8')

# Conversion des notes en float
df['press_rate'] = df['press_rate'].str.replace(',', '.').astype(float)
df['spectateur_rate'] = df['spectateur_rate'].str.replace(',', '.').astype(float)

# Appliquer la fonction de conversion de la durée
df['duree_minutes'] = df['duree'].apply(convert_duration_to_minutes)

# Supprimer les colonnes 'url' et 'duree'
df.drop(columns=['url', 'duree'], inplace=True)

# Supprimer les lignes contenant des NaN
df.dropna(inplace=True)

# Supprimer les lignes dupliquées
df.drop_duplicates(inplace=True)

# Écrire le DataFrame nettoyé en sortie au format CSV
output_csv = df.to_csv(index=False, encoding='utf-8')  # Supprimé line_terminator
print(output_csv)  # Imprime le CSV pour qu'il puisse être capturé par ExecuteStreamCommand
