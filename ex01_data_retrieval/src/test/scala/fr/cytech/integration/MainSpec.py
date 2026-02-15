import os
import boto3
from botocore.client import Config

def test_data_retrieval():
    # 1. Vérification du fichier local
    local_file = "data/raw/yellow_tripdata_2024-01.parquet"
    assert os.path.exists(local_file), f"Le fichier local {local_file} est manquant !"
    print("✅ Test Local : Fichier trouvé dans data/raw.")

    # 2. Vérification sur Minio
    s3 = boto3.resource('s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    try:
        bucket = s3.Bucket('nyc-raw')
        objs = list(bucket.objects.filter(Prefix='yellow_tripdata_2024-01.parquet'))
        assert len(objs) > 0, "Le fichier est absent du bucket Minio 'nyc-raw'"
        print("✅ Test Minio : Fichier trouvé dans le bucket 'nyc-raw'.")
    except Exception as e:
        print(f"❌ Erreur lors du test Minio : {e}")
        exit(1)

if __name__ == "__main__":
    test_data_retrieval()