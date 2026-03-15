import os
import json
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def ingest_usda_to_minio(query="apple"):
    # 1. Configuración de API y Clientes
    api_key = os.getenv("USDA_API_KEY")
    # Endpoint de búsqueda
    url = "https://api.nal.usda.gov/fdc/v1/foods/search"
    
    s3 = boto3.client("s3", 
                      endpoint_url=os.getenv("MINIO_ENDPOINT"),
                      aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                      aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"))
    
    bucket_name = "landing-zone"
    target_folder = "temporal_landing/usda/"
    
    # 2. Parámetros de la búsqueda
    # Usamos POST para poder configurar filtros más complejos fácilmente
    payload = {
        "query": query,
        "pageSize": 50, # Cantidad de resultados
        "dataType": ["Foundation", "Survey (FNDDS)"], # Tipos de datos más fiables nutricionalmente
        "api_key": api_key
    }

    print(f"Buscando alimentos en USDA para el término: '{query}'...")

    try:
        # 3. Petición a USDA
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        data = response.json()

        # 4. Preparar nombre de archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Limpiamos el query para el nombre del archivo
        clean_query = query.replace(" ", "_").lower()
        file_name = f"search_{clean_query}_{timestamp}.json"
        object_key = f"{target_folder}{file_name}"

        # 5. Subir a MinIO
        print(f"Subiendo {len(data.get('foods', []))} resultados a MinIO: {object_key}")
        
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )

        print("--- Ingesta de USDA completada ---")

    except Exception as e:
        print(f"Error en la ingesta de USDA: {e}")

if __name__ == "__main__":
    # Puedes cambiar "apple" por cualquier ingrediente que necesites
    ingest_usda_to_minio("apple")