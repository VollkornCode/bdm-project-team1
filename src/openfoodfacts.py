import os
import json
import requests
import boto3
import time  # Para el control de tiempo
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def ingest_off_automatic(pages=3, page_size=100):
    # 1. Configuración
    url = "https://world.openfoodfacts.org/api/v2/search"
    headers = {
        "User-Agent": "BigDataManagementProjectUniversitatPolitecnicaCatalunya/1.0 (contact: sergi.gonzalez.martos@estudiantat.upc.edu)"
    }
    
    s3 = boto3.client("s3", 
                      endpoint_url=os.getenv("MINIO_ENDPOINT"),
                      aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                      aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"))
    
    bucket_name = "landing-zone"
    target_folder = "temporal_landing/openfoodfacts/automatic/"

    print(f"--- Iniciando Ingesta Automática (Respetando límites: 10 req/min) ---")

    # 2. Bucle de páginas
    for current_page in range(1, pages + 1):
        params = {
            "sort_by": "unique_scans_n", # Ordenar por popularidad/escaneos
            "page": current_page,
            "page_size": page_size,
            "json": "true",
            "fields": "code,product_name,brands,nutriments,countries,categories"
        }

        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Pidiendo página {current_page}...")
            response = requests.get(url, params=params, headers=headers)
            
            # Si recibimos un error de Rate Limit (429)
            if response.status_code == 429:
                print("¡Límite alcanzado! Esperando 60 segundos...")
                time.sleep(60)
                continue

            response.raise_for_status()
            data = response.json()

            # 3. Subir a MinIO
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"off_page_{current_page}_{timestamp}.json"
            object_key = f"{target_folder}{file_name}"

            s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=json.dumps(data, indent=4),
                ContentType='application/json'
            )
            print(f"Éxito: Página {current_page} sincronizada.")

            # 4. CONTROL DE VELOCIDAD (Crucial para no ser baneado)
            if current_page < pages:
                print("Esperando 10 segundos para la siguiente petición...")
                time.sleep(10) 

        except Exception as e:
            print(f"Fallo en página {current_page}: {e}")
            break

    print("--- Proceso completado ---")

if __name__ == "__main__":
    # Descargamos 3 páginas de prueba (300 productos)
    ingest_off_automatic(pages=3, page_size=100)