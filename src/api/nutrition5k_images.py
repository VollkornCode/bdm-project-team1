import os
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def ingest_all_images():
    # Credenciales
    os.environ['KAGGLE_USERNAME'] = os.getenv("KAGGLE_USERNAME", "")
    os.environ['KAGGLE_KEY'] = os.getenv("KAGGLE_KEY", "")

    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    s3 = boto3.client("s3", endpoint_url=os.getenv("MINIO_ENDPOINT"),
                      aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                      aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"))
    
    dataset = "gillesokhin/nutrition5k-dataset"
    bucket_name = "landing-zone"
    download_path = "./temp_images"
    
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # 1. OBTENER LOS IDs DE LOS PLATOS
    # Usamos el CSV de nutrición para saber qué carpetas existen
    print("Obteniendo lista de IDs desde el CSV...")
    # Asumimos que ya tienes el archivo localmente o lo descargamos rápido
    csv_file = "dish_nutrition_values.csv"
    if not os.path.exists(csv_file):
        api.dataset_download_file(dataset, csv_file, path=".")
    
    df = pd.read_csv(csv_file, header=None)
    dish_ids = df[0].unique() # La primera columna suele ser el ID del plato
    
    print(f"Se identificaron {len(dish_ids)} platos. Iniciando descarga masiva...")

    # 2. BUCLE DE DESCARGA TOTAL
    for dish_id in dish_ids:
        remote_path = f"imagery/realsense_overhead/{dish_id}/rgb.png"
        unique_name = f"imagery_realsense_overhead_{dish_id}_rgb.png"
        
        try:
            # Descargar archivo individual
            api.dataset_download_file(dataset, remote_path, path=download_path)
            
            # Localizar archivo (Kaggle puede crear subcarpetas o dejarlo en el root)
            local_file = os.path.join(download_path, "rgb.png")
            if not os.path.exists(local_file):
                local_file = os.path.join(download_path, remote_path)

            if os.path.exists(local_file):
                object_key = f"temporal_landing/{unique_name}"
                s3.upload_file(local_file, bucket_name, object_key)
                
                # Limpiar inmediatamente para no llenar el disco duro
                os.remove(local_file)
                print(f"Sincronizado: {unique_name}")
            else:
                print(f"Advertencia: No se encontró {remote_path} en el dataset.")

        except Exception as e:
            # Muchos platos podrían no tener esa imagen específica, así que saltamos errores
            if "404" in str(e):
                continue
            print(f"Error en plato {dish_id}: {e}")

    print("--- Proceso de descarga masiva finalizado ---")

if __name__ == "__main__":
    ingest_all_images()