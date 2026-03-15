import os
import json
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def ingest_spoonacular_to_minio():
    # 1. Configuración de API y Clientes
    api_key = os.getenv("SPOON_KEY")
    url = "https://api.spoonacular.com/recipes/random"
    
    s3 = boto3.client("s3", 
                      endpoint_url=os.getenv("MINIO_ENDPOINT"),
                      aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                      aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"))
    
    bucket_name = "landing-zone/spoon/"
    # Carpeta organizada por fuente y fecha
    target_folder = "temporal_landing"
    
    # 2. Parámetros de la petición
    params = {
        "apiKey": api_key,
        "number": 10,
        "includeNutrition": "true"
    }

    print(f"Solicitando {params['number']} recetas aleatorias a Spoonacular...")

    try:
        # 3. Realizar la petición GET
        response = requests.get(url, params=params)
        response.raise_for_status() # Lanza error si la petición falla (4xx o 5xx)
        
        data = response.json()

        # 4. Preparar el archivo para MinIO
        # Usamos timestamp para que cada ejecución genere un archivo nuevo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"random_recipes_{timestamp}.json"
        object_key = f"{target_folder}{file_name}"

        # Convertir el diccionario a string JSON
        json_data = json.dumps(data, indent=4)

        # 5. Subir a MinIO (usando put_object para no guardar archivo localmente)
        print(f"Subiendo resultados a MinIO: {object_key}")
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_data,
            ContentType='application/json'
        )

        print("--- Ingesta de Spoonacular completada con éxito ---")

    except requests.exceptions.HTTPError as err:
        print(f"Error en la petición a Spoonacular: {err}")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    ingest_spoonacular_to_minio()