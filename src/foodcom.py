import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# 1. Cargar el .env al inicio de todo
load_dotenv()

def ingest_kaggle_to_minio():
    # Seteamos las variables de entorno del sistema explícitamente
    os.environ['KAGGLE_USERNAME'] = os.getenv("KAGGLE_USERNAME", "")
    os.environ['KAGGLE_KEY'] = os.getenv("KAGGLE_KEY", "")

    # Validar que no estén vacías
    if not os.environ['KAGGLE_USERNAME'] or not os.environ['KAGGLE_KEY']:
        print("Error: No se encontraron las credenciales de Kaggle en el .env")
        return

    # IMPORTACIÓN LOCAL: Esto obliga a Kaggle a leer las variables que acabamos de setear
    from kaggle.api.kaggle_api_extended import KaggleApi
    
    try:
        api = KaggleApi()
        api.authenticate()
        print("Autenticación en Kaggle: OK")
    except Exception as e:
        print(f"Fallo crítico de autenticación: {e}")
        return
    
    # Configuración MinIO
    minio_config = {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
        "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
    }
    
    s3 = boto3.client("s3", **minio_config)
    
    dataset = "shuyangli94/food-com-recipes-and-user-interactions"
    bucket_name = "landing-zone"
    target_folder = "temporal_landing"
    download_path = "./temp_kaggle_files"

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Solo los CSVs que nos interesan
    files_to_ingest = ["dish_ingredients.csv", "dish_nutrition_values.csv", "ingredients_metadata.csv"]
    
    print(f"--- Iniciando proceso de ingesta selectiva ---")

    for file_name in files_to_ingest:
        try:
            # Descarga selectiva
            print(f"Descargando {file_name}...")
            api.dataset_download_file(dataset, file_name, path=download_path)
            
            # La API de Kaggle a veces descarga el archivo directamente o dentro de un zip
            # Si se descarga como zip, habría que descomprimirlo, pero usualmente 
            # dataset_download_file maneja el archivo individual.
            local_file_path = os.path.join(download_path, file_name)
            
            # Verificación de que el archivo existe antes de subir
            if os.path.exists(local_file_path):
                object_key = f"{target_folder}{file_name}"
                print(f"Subiendo a MinIO: {object_key}")
                s3.upload_file(local_file_path, bucket_name, object_key)
                
                # Limpieza de archivos locales
                os.remove(local_file_path)
                print(f"Éxito: {file_name} procesado y eliminado localmente.")
            
        except Exception as e:
            print(f"Error procesando {file_name}: {e}")

if __name__ == "__main__":
    ingest_kaggle_to_minio()