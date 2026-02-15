import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import numpy as np


import boto3
import io


def load_data_from_minio(bucket_name, folder_name):
    """
    Charge tous les fichiers d'un dossier Parquet depuis Minio.

    Parameters
    ----------
    bucket_name : str
        Nom du bucket.
    folder_name : str
        Nom du dossier (ex: 'refined_taxi_data.parquet').

    Returns
    -------
    pd.DataFrame
        Données fusionnées.
    """
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123'
    )

    # 1. Lister les fichiers dans le dossier
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    
    parts = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        # On ne lit que les fichiers qui contiennent des données (part-...)
        if key.endswith(".parquet") and "part-" in key:
            data = s3.get_object(Bucket=bucket_name, Key=key)
            parts.append(pd.read_parquet(io.BytesIO(data['Body'].read())))

    # 2. Fusionner tous les morceaux
    return pd.concat(parts, ignore_index=True)


def train():
    """
    Exécute le pipeline d'entraînement du modèle de prédiction de prix.
    """
    # 1. Chargement des données nettoyées (Silver Layer)
    df = load_data_from_minio('nyc-processed', 'refined_taxi_data.parquet')

    # 2. Sélection des variables explicatives (Features)
    features = [
        'passenger_count', 'trip_distance',
        'PULocationID', 'DOLocationID'
    ]
    X = df[features]
    y = df['total_amount']

    # 3. Séparation Entraînement / Test (80/20)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # 4. Entraînement du modèle (Random Forest)
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # 5. Évaluation des performances
    predictions = model.predict(X_test)
    rmse = np.sqrt(mean_squared_error(y_test, predictions))

    print("-" * 30)
    print("✅ Modèle entraîné avec succès.")
    print(f"📊 RMSE obtenu : {rmse:.2f}")

    # Vérification de l'objectif de l'exercice 5
    if rmse < 10:
        print("🎯 Objectif de précision (< 10 RMSE) : ATTEINT")
    else:
        print("⚠️ Objectif de précision : NON ATTEINT")
    print("-" * 30)


if __name__ == "__main__":
    train()
