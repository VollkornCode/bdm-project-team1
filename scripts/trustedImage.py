from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, substring_index
from pyspark.sql.types import BinaryType
from PIL import Image
import io
import json

from scripts.ingest import MinioClient, DeltaLakeClient


# ── Processed file registry ────────────────────────────────────────────────────

REGISTRY_BUCKET = "trusted-zone"
REGISTRY_PREFIX = "_processed_registry"


def _registry_key(folder: str) -> str:
    return f"{REGISTRY_PREFIX}/images__{folder}.json"


def _load_registry(minio_client: MinioClient, folder: str) -> set[str]:
    """
    Load the set of already-processed image filenames for *folder*.
    Returns an empty set if no registry exists yet.
    """
    key = _registry_key(folder)
    try:
        response = minio_client.s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        processed = set(data.get("processed_files", []))
        print(f"[Registry] Loaded {len(processed)} already-processed images for '{folder}'.")
        return processed
    except minio_client.s3_client.exceptions.NoSuchKey:
        print(f"[Registry] No registry found for '{folder}' — treating all images as new.")
        return set()
    except Exception as exc:
        print(f"[Registry] Could not load registry for '{folder}': {exc} — treating all images as new.")
        return set()


def _save_registry(minio_client: MinioClient, folder: str, processed_files: set[str]) -> None:
    """
    Persist the updated set of processed image filenames to MinIO.
    """
    key = _registry_key(folder)
    data = json.dumps({"processed_files": sorted(processed_files)}, indent=2)
    minio_client.s3_client.put_object(
        Bucket=REGISTRY_BUCKET,
        Key=key,
        Body=data.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[Registry] Saved registry for '{folder}' ({len(processed_files)} images total).")


def _list_image_files(minio_client: MinioClient, bucket: str, prefix: str) -> list[str]:
    """
    List all image filenames currently under s3://{bucket}/{prefix}.
    Returns full object keys, e.g. ['spoonacular/images/img1.jpg'].
    """
    paginator = minio_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff"}
    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if any(key.lower().endswith(ext) for ext in image_extensions):
                files.append(key)
    return files


# ── Main client ────────────────────────────────────────────────────────────────

class TrustedImageClient:

    def __init__(self, spark: SparkSession, landing_path: str, trusted_path: str, image_size=(224, 224)):
        self.spark = spark
        self.landing_path = landing_path
        self.trusted_path = trusted_path
        self.image_size = image_size

        self.df = None

    def load_data_image(self, new_files: list[str] | None = None):
        """
        Load raw images from the Landing Zone in binary format.
        If *new_files* is provided, only those specific files are read.
        """
        if new_files:
            # Build full s3a paths for each new file key
            paths = [f"s3a://raw-data/{key}" for key in new_files]
            print(f"[Load] Reading {len(paths)} new image file(s)...")
            self.df = self.spark.read.format("binaryFile").load(paths)
        else:
            print(f"[Load] Reading all images from: {self.landing_path}")
            self.df = self.spark.read.format("binaryFile").load(self.landing_path)

        print(f"[Load] Loaded {self.df.count()} images.")
        return self.df

    def clean_data_image(self):
        """Validate images, convert to RGB, resize, and extract filename."""
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

        print(f"[Clean] Valid images after cleaning: {self.df.count()}")
        return self.df

    def store_data_image(self, minio_client: MinioClient):
        """Collect processed bytes and upload them to the Trusted Zone via MinioClient."""
        minio_client.create_buckets()

        print("[Store] Collecting cleaned images to upload to MinIO...")
        rows = self.df.collect()

        if not rows:
            print("[Store] No images to upload.")
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

        print(f"[Store] Successfully processed and stored {uploaded_count} images.")


# ── Orchestration ──────────────────────────────────────────────────────────────

def init_trusted_image_pipeline(
    spark: SparkSession,
    minio_client: MinioClient,
    landing_path: str,
    trusted_path: str,
) -> None:
    """
    Incremental ETL pipeline for images.

    On each run:
      1. Lists all image files in the landing bucket/prefix.
      2. Loads the registry of already-processed files from MinIO.
      3. Computes the diff — only new files are processed.
      4. Runs Load -> Clean -> Store on new files only.
      5. Updates the registry with the newly processed files.
    """
    print("=" * 60)
    print("--- Starting Trusted Image Pipeline ---")
    print(f"    Landing path : {landing_path}")
    print(f"    Trusted path : {trusted_path}")
    print("=" * 60)

    # Derive bucket and prefix from landing_path: s3a://raw-data/spoonacular/images/
    # → bucket = "raw-data", prefix = "spoonacular/images/"
    stripped = landing_path.replace("s3a://", "").replace("s3://", "")
    bucket, _, prefix = stripped.partition("/")
    folder = prefix.strip("/").replace("/", "__")  # used as registry key, e.g. "spoonacular__images"

    try:
        # ── 1. Discover all available image files ──────────────────────────────
        all_files = _list_image_files(minio_client, bucket, prefix)
        print(f"[Incremental] Found {len(all_files)} total image(s) in '{bucket}/{prefix}'.")

        # ── 2. Load registry of already-processed files ────────────────────────
        processed = _load_registry(minio_client, folder)

        # ── 3. Compute diff ────────────────────────────────────────────────────
        new_files = [f for f in all_files if f not in processed]
        print(
            f"[Incremental] {len(new_files)} new image(s) to process "
            f"({len(all_files) - len(new_files)} already processed — skipping)."
        )

        if not new_files:
            print("[Incremental] Nothing to do — all images already processed.")
            return

        # ── 4. ETL on new files only ───────────────────────────────────────────
        client = TrustedImageClient(
            spark=spark,
            landing_path=landing_path,
            trusted_path=trusted_path,
            image_size=(224, 224),
        )

        client.load_data_image(new_files=new_files)
        client.clean_data_image()
        client.store_data_image(minio_client)

        # ── 5. Update registry ─────────────────────────────────────────────────
        updated = processed | set(new_files)
        _save_registry(minio_client, folder, updated)

    except Exception as e:
        print(f"[Pipeline] Critical error: {e}")
        raise

    finally:
        print("=" * 60)
        print("--- Trusted Image Pipeline Completed ---")
        print("=" * 60)


if __name__ == "__main__":
    init_trusted_image_pipeline(
        landing_path="s3a://raw-data/spoonacular/images/",
        trusted_path="spoonacular_images/",
    )