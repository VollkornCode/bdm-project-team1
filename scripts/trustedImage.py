from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, substring_index
from pyspark.sql.types import BinaryType
from PIL import Image
import io

# Importamos el cliente de MinIO desde tu archivo ingest.py
from scripts.ingest import MinioClient, DeltaLakeClient


class TrustedImageClient:

    def __init__(self, spark: SparkSession, landing_path: str, trusted_path: str, image_size=(224, 224)):
        self.spark = spark
        self.landing_path = landing_path
        self.trusted_path = trusted_path
        self.image_size = image_size

        self.df = None

    def load_data_image(self):
        """Carga las imágenes crudas desde la Landing Zone en formato binario."""
        self.df = (
            self.spark.read
            .format("binaryFile")
            .load(self.landing_path)
        )

        print(f"Loaded {self.df.count()} images")
        return self.df

    def clean_data_image(self):
        """Valida las imágenes, las convierte a RGB, las redimensiona y extrae el nombre del archivo."""
        image_size = self.image_size

        def clean_image(binary_content):
            try:
                image = Image.open(io.BytesIO(binary_content))
                image.verify()
                
                image = Image.open(io.BytesIO(binary_content))
                image = image.convert("RGB") 
                image = image.resize(image_size) 
                
                buffer = io.BytesIO()
                image.save(buffer, format="JPEG")
                return buffer.getvalue()
            except Exception:
                return None

        clean_udf = udf(clean_image, BinaryType())

        self.df = (
            self.df
            .withColumn("cleaned_content", clean_udf(col("content")))
            .filter(col("cleaned_content").isNotNull())
            .withColumn("filename", substring_index(col("path"), "/", -1))
            .select("filename", "cleaned_content")
        )

        print(f"Valid images after cleaning: {self.df.count()}")
        return self.df

    def store_data_image(self, minio_client: MinioClient):
        """Descarga los bytes procesados y utiliza MinioClient para subirlos a la Trusted Zone."""
        # Crea los buckets base definidos en ingest.py si no existieran
        minio_client.create_buckets()

        print("Collecting cleaned images to upload to MinIO...")
        rows = self.df.collect()

        if not rows:
            print("No images to upload.")
            return

        uploaded_count = 0
        for row in rows:
            filename = row["filename"]
            binary_data = row["cleaned_content"]
            s3_key = f"{self.trusted_path}{filename}"
        
            success = minio_client.upload_binary_bytes(
                binary_data=binary_data,
                bucket_name="trusted-zone",
                key=s3_key
            )
            if success:
                uploaded_count += 1

        print(f"Successfully processed and stored {uploaded_count} images via MinioClient.")


def init_trusted_image_pipeline(spark: SparkSession, minio_client: MinioClient, landing_path: str, trusted_path: str):
    '''
    Orchestrates the ETL process for images:
    1. Initializes a SparkSession.
    2. Instantiates the TrustedImageClient.
    3. Verifies or creates the target bucket in MinIO.
    4. Executes the Load, Clean, and Store phases.
    '''
    print(f"--- Starting Trusted Image Pipeline ---")

    try:
        # 2. Inicializamos nuestro cliente de procesamiento
        client = TrustedImageClient(
            spark=spark,
            landing_path=landing_path,
            trusted_path=trusted_path,
            image_size=(224, 224)
        )

        # 3. Verificación de seguridad: Crear el bucket "trusted-zone" si no existe aún en MinIO
        try:
            minio_client.create_buckets()
            print("Bucket 'trusted-zone' verified/created.")
        except Exception:
            pass 

        # 4. Ejecución del pipeline ETL
        client.load_data_image()
        client.clean_data_image()
        client.store_data_image(minio_client)

    except Exception as e:
        print(f"Critical error during Image Trusted pipeline execution: {e}")
        
    finally:
        # Cerramos la sesión de Spark de forma segura para liberar recursos
        spark.stop()
        print("--- Trusted Image Pipeline Completed ---")


if __name__ == "__main__":
    # Ejecución por defecto si se lanza el script directamente
    init_trusted_image_pipeline(
        landing_path="s3a://raw-data/spoonacular/images/",
        trusted_path="spoonacular_images/"
    )