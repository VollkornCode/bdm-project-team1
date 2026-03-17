import boto3
from botocore.exceptions import ClientError

# Configuración de conexión a MinIO (ajustar según tu despliegue Docker)
minio_config = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
}

def setup_landing_zone():
    # Inicializar el cliente S3 compatible con MinIO
    s3 = boto3.client("s3", **minio_config)
    
    bucket_name = "landing-zone"
    
    try:
        # 1. Crear el bucket principal si no existe
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' creado exitosamente.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print(f"El bucket '{bucket_name}' ya existe.")
        else:
            raise e

    # 2. Crear estructura de sub-buckets (carpetas lógicas)
    # En S3/MinIO las carpetas se crean añadiendo un objeto vacío con '/'
    sub_zones = ["temporal_landing/", "persistent_landing/"]
    
    for zone in sub_zones:
        s3.put_object(Bucket=bucket_name, Key=zone)
        print(f"Sub-zona '{zone}' inicializada en {bucket_name}.")

if __name__ == "__main__":
    setup_landing_zone()