import os
import json
import requests
import boto3
import time  # Para el control de tiempo
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def ingest_off_automatic(pages=3, page_size=20):
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
    target_folder = "temporal_landing/openfoodfacts/"

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

        max_retries = 2  # Intentar cada página hasta 2 veces si falla
        for attempt in range(max_retries):
            try:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Pidiendo página {current_page} (Intento {attempt+1})...")
                
                # Añadimos timeout=30 para dar margen al servidor
                response = requests.get(url, params=params, headers=headers, timeout=30)
                
                if response.status_code == 429:
                    print("¡Límite 429 alcanzado! Esperando 60 segundos...")
                    time.sleep(60)
                    continue # Reintenta esta misma página

                response.raise_for_status()
                data = response.json()

                # --- Lógica de subida a MinIO (igual que la tienes) ---
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
                break # Éxito: salimos del bucle de reintentos y vamos a la siguiente página

            except (requests.exceptions.RequestException, Exception) as e:
                print(f"Error en intento {attempt+1} para página {current_page}: {e}")
                if attempt < max_retries - 1:
                    print("Esperando 15 segundos para reintentar...")
                    time.sleep(15)
                else:
                    print(f"Página {current_page} falló tras {max_retries} intentos. Saltando...")

    print("--- Proceso completado ---")

if __name__ == "__main__":
    # Descargamos 3 páginas de prueba (300 productos)
    ingest_off_automatic(pages=3, page_size=20)