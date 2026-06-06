from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Importamos los clientes de infraestructura desde ingest.py
from scripts.ingest import MinioClient, DeltaLakeClient


def _flatten_struct_columns(df: DataFrame) -> DataFrame:
    """
    Aplana de forma recursiva columnas StructType.
    Añade un límite de seguridad (max_depth) para evitar bucles infinitos
    con tipos de datos complejos como Array de Structs (ej: extendedIngredients).
    """
    max_depth = 7  # Límite de niveles para evitar bucles infinitos
    depth = 0
    
    while depth < max_depth:
        # Buscamos únicamente estructuras de primer nivel puras
        struct_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StructType)
        ]

        if not struct_cols:
            break   # Si ya no quedan estructuras puras, salimos del bucle con éxito

        print(f"    [Flatten] Nivel {depth+1}: Aplanando estructuras -> {struct_cols}")
        
        expand_exprs = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                for subfield in field.dataType.fields:
                    alias = f"{field.name}__{subfield.name}"
                    expand_exprs.append(F.col(f"{field.name}.{subfield.name}").alias(alias))
            else:
                expand_exprs.append(F.col(field.name))
        
        df = df.select(*expand_exprs)
        depth += 1
        
    if depth == max_depth:
        print("    [Flatten] ⚠️ Alerta de Seguridad: Se ha detenido el aplanado en el límite máximo para evitar un bucle infinito.")
        
    return df


def _extract_source_folder(path_col: str, bucket_prefix: str) -> "Column":  # noqa: F821
    """
    Extrae el nombre de la subcarpeta inmediatamente después del prefijo del
    bucket en la ruta S3 del archivo.

    Ejemplo:
        path            : s3a://deltalake/spoonacular/recipes/part-00000.parquet
        bucket_prefix   : s3a://deltalake/
        resultado       : spoonacular/recipes
    """
    # Eliminamos el prefijo del bucket y nos quedamos con la ruta relativa,
    # luego descartamos el nombre del archivo (último segmento).
    relative = F.regexp_replace(F.col(path_col), f"^{bucket_prefix}", "")
    # Eliminamos el último componente (nombre del archivo) para obtener solo
    # la estructura de carpetas.
    folder = F.expr(
        f"regexp_replace(regexp_replace(`{path_col}`, '^{bucket_prefix}', ''), '/[^/]+$', '')"
    )
    return folder


class TrustedParquetClient:

    LANDING_BUCKET_PREFIX = "s3a://deltalake/"

    def __init__(self, spark: SparkSession, landing_path: str, trusted_path: str):
        self.spark        = spark
        self.landing_path = landing_path
        self.trusted_path = trusted_path

        self.df: DataFrame | None = None

        self._partitioned_dfs: dict[str, DataFrame] = {}

    # ------------------------------------------------------------------
    # Phase 1 – Load
    # ------------------------------------------------------------------

    def load_data(self) -> DataFrame:
        """
        Carga todos los archivos .parquet desde la Landing Zone.

        Se activa la opción 'pathGlobFilter' para garantizar que solo se
        lean archivos Parquet, y 'recursiveFileLookup' para recorrer
        subcarpetas de forma automática.

        Adicionalmente se incorpora la columna de metadatos '_source_path'
        con la ruta completa del archivo, necesaria para preservar la
        estructura de carpetas en el destino.
        """
        print(f"[Load] Reading Parquet files from: {self.landing_path}")

        self.df = (
            self.spark.read
            .option("pathGlobFilter", "*.parquet")
            .option("recursiveFileLookup", "true")
            .parquet(self.landing_path)
            # Añadimos la columna de ruta antes de cualquier transformación
            .withColumn("_source_path", F.input_file_name())
        )

        row_count = self.df.count()
        print(f"[Load] Loaded {row_count:,} rows from Parquet files.")
        return self.df

    # ------------------------------------------------------------------
    # Phase 2 – Clean
    # ------------------------------------------------------------------

    def clean_data(self) -> dict[str, DataFrame]:
        if self.df is None:
            raise ValueError("[Clean] No data loaded to clean. Call load_data() first.")

        print("--- Cleaning Data (Flatten & Filter) ---")
        df = self.df

        # ── 1. Extraer de forma segura el nombre de la carpeta origen ───────
        # Eliminamos el prefijo del bucket
        df = df.withColumn("_clean_path", F.regexp_replace(F.col("_source_path"), f"^{self.LANDING_BUCKET_PREFIX}", ""))

        # Usamos regexp_extract para capturar todo lo que esté antes de la primera '/'
        # Si no hay '/', significa que está en la raíz y por defecto le ponemos "recipes"
        df = df.withColumn(
            "_source_folder",
            F.when(
                F.col("_clean_path").contains("/"),
                F.regexp_extract(F.col("_clean_path"), r"^([^/]+)", 1)
            ).otherwise(F.lit("recipes"))
        ).drop("_clean_path")

        # ── 2. Aplanar estructuras anidadas ─────────────────────────────────
        print("[Clean] Step 2/4 – Flattening nested struct columns...")
        df = _flatten_struct_columns(df)

        # ── 3. Eliminar filas con nulos (Usa el how='all' que modificaste) ──
        print("[Clean] Step 3/4 – Dropping rows with all null values...")
        data_cols = [c for c in df.columns if c not in ["_source_path", "_source_folder"]]
        df = df.dropna(how="all", subset=data_cols)

        # ── 4. Eliminar duplicados exactos ──────────────────────────────────
        print("[Clean] Step 4/4 – Dropping exact duplicate rows...")
        df = df.dropDuplicates(subset=data_cols)

        after_dedup = df.count()

        # ── 5. Particionar los DataFrames por carpeta origen ────────────────
        print("[Clean] Partitioning DataFrames by source folder...")
        source_folders = [r[0] for r in df.select("_source_folder").distinct().collect() if r[0] is not None]
        print(f"    Detected source folders to process: {source_folders}")

        self._partitioned_dfs = {}
        for folder in source_folders:
            sub_df = (
                df.filter(F.col("_source_folder") == folder)
                  .drop("_source_path", "_source_folder")
            )
            self._partitioned_dfs[folder] = sub_df
            print(f"    ✓ Partition '{folder}' prepared for storage.")

        print(f"[Clean] Finished. Total clean rows: {after_dedup:,} across {len(source_folders)} partition(s).")
        
        return self._partitioned_dfs

    '''
    def clean_data(self) -> dict[str, DataFrame]:
        """
        Aplica las siguientes transformaciones genéricas de calidad de datos:

        1. Aplanado de StructType  – Promueve subcampos anidados a primer nivel.
        2. Trim en strings         – Elimina espacios al inicio/fin de valores texto.
        3. Eliminación de nulos    – Descarta filas con al menos un valor nulo.
        4. Eliminación de duplicados – Descarta filas 100 % idénticas.

        Adicionalmente, extrae la subcarpeta de origen de cada fila y divide
        el DataFrame en subconjuntos por carpeta, facilitando el particionamiento
        fiel en la escritura.

        Retorna
        -------
        dict { source_folder (str) → DataFrame limpio }
        """
        if self.df is None:
            raise RuntimeError("El DataFrame no está cargado. Ejecuta load_data() primero.")

        df = self.df
        initial_count = df.count()
        print(f"[Clean] Starting with {initial_count:,} rows.")

        # ── 1. Aplanar columnas de tipo StructType ───────────────────────────
        print("[Clean] Step 1/4 – Flattening StructType columns...")
        df = _flatten_struct_columns(df)

        # ── 2. Trim en todas las columnas de tipo string ─────────────────────
        print("[Clean] Step 2/4 – Trimming whitespace from string columns...")
        string_cols = [
            field.name
            for field in df.schema.fields
            if str(field.dataType) in ("StringType()", "StringType")
            and field.name != "_source_path"
        ]
        if string_cols:
            trim_exprs = [
                F.trim(F.col(f"`{c}`")).alias(c) if c in string_cols
                else F.col(f"`{c}`")
                for c in df.columns
            ]
            df = df.select(trim_exprs)
            print(f"    Trimmed {len(string_cols)} string column(s): {string_cols}")
        else:
            print("    No string columns found to trim.")

        # ── 3. Eliminar filas con valores nulos ──────────────────────────────
        print("[Clean] Step 3/4 – Dropping rows with any null value...")
        # Excluimos _source_path del chequeo de nulos (es metadata interna)
        data_cols = [c for c in df.columns if c != "_source_path"]
        df = df.dropna(how="all", subset=data_cols)
        after_nulls = df.count()
        print(f"    Rows after null removal: {after_nulls:,}  "
              f"(dropped {initial_count - after_nulls:,})")

        # ── 4. Eliminar filas duplicadas (en columnas de datos) ──────────────
        print("[Clean] Step 4/4 – Dropping exact duplicate rows...")
        df = df.dropDuplicates(subset=data_cols)
        after_dedup = df.count()
        print(f"    Rows after deduplication: {after_dedup:,}  "
              f"(dropped {after_nulls - after_dedup:,})")

        
        # ── Extraer subcarpeta de origen y particionar ───────────────────────
        print("[Clean] Extracting source folder structure for partitioned output...")
        df = df.withColumn(
            "_source_folder",
            F.regexp_replace(
                F.regexp_replace(F.col("_source_path"), f"^{self.LANDING_BUCKET_PREFIX}", ""),
                "/[^/]+$",   # elimina el nombre del archivo al final
                ""
            )
        )

        source_folders = [
            row["_source_folder"]
            for row in df.select("_source_folder").distinct().collect()
        ]
        print(f"[Clean] Detected source folders: {source_folders}")

        # ── 3. Eliminar filas con valores nulos ──────────────────────────────
        print("==================================================")
        print("[CONTROL-VERIFICACION] ¡ESTOY USANDO EL SCRIPT NUEVO SIN DROPNA ESTRICTO! parte final")
        print("==================================================")

        self._partitioned_dfs = {}
        for folder in source_folders:
            sub_df = (
                df.filter(F.col("_source_folder") == folder)
                  .drop("_source_path", "_source_folder")
            )
            self._partitioned_dfs[folder] = sub_df
            #print(f"    Partition '{folder}': {sub_df.count():,} rows.")

        print(f"[Clean] Finished. Total clean rows: {after_dedup:,} "
              f"across {len(source_folders)} partition(s).")
        
        # ── 3. Eliminar filas con valores nulos ──────────────────────────────
        print("==================================================")
        print("[CONTROL-VERIFICACION] ¡ESTOY USANDO EL SCRIPT NUEVO SIN DROPNA ESTRICTO! saliendo de clean")
        print("==================================================")

        return self._partitioned_dfs
        '''

    # ------------------------------------------------------------------
    # Phase 3 – Store
    # ------------------------------------------------------------------

    def store_data(self, delta_client: DeltaLakeClient) -> None:
        """
        Persiste cada partición en la Trusted Zone mediante DeltaLakeClient,
        respetando la estructura de carpetas del bucket de origen.

        La ruta de destino se construye como:
            <trusted_path>/<source_folder>/

        Parámetros
        ----------
        delta_client : Instancia de DeltaLakeClient lista para escribir.
        """
        # ── 3. Eliminar filas con valores nulos ──────────────────────────────
        print("==================================================")
        print("[CONTROL-VERIFICACION] ¡ESTOY USANDO EL SCRIPT NUEVO SIN DROPNA ESTRICTO! entrando a store")
        print("==================================================")

        if not self._partitioned_dfs:
            raise RuntimeError(
                "No hay datos particionados disponibles. "
                "Ejecuta clean_data() antes de store_data()."
            )

        print(f"[Store] Writing {len(self._partitioned_dfs)} partition(s) to Trusted Zone...")

        stored_count   = 0
        stored_folders = []

        # ── 3. Eliminar filas con valores nulos ──────────────────────────────
        print("==================================================")
        print("[CONTROL-VERIFICACION] ¡ESTOY USANDO EL SCRIPT NUEVO SIN DROPNA ESTRICTO! primer if")
        print("==================================================")

        for source_folder, sub_df in self._partitioned_dfs.items():
            # Construimos la ruta de destino preservando la estructura de carpetas
            destination_path = f"{self.trusted_path.rstrip('/')}/{source_folder}"
            row_count        = sub_df.count()

            print(f"[Store] Writing partition '{source_folder}' "
                  f"({row_count:,} rows) → {destination_path}")

            try:
                # Convertimos a lista de dicts para delegar en DeltaLakeClient
                # (mismo contrato que usa write_table en ingest.py)

                # ── 3. Eliminar filas con valores nulos ──────────────────────────────
                print("==================================================")
                print("[CONTROL-VERIFICACION] ¡ESTOY USANDO EL SCRIPT NUEVO SIN DROPNA ESTRICTO! try")
                print("==================================================")

                rows_as_dicts = [row.asDict(recursive=True) for row in sub_df.collect()]

                delta_client.write_table(
                    data=rows_as_dicts,
                    table_path=destination_path
                )

                stored_count   += row_count
                stored_folders.append(source_folder)
                print(f"    ✓ Partition '{source_folder}' stored successfully.")

            except Exception as e:
                print(f"    ✗ Error storing partition '{source_folder}': {e}")

        print(f"[Store] Done. Stored {stored_count:,} rows across "
              f"{len(stored_folders)} partition(s): {stored_folders}")


# ---------------------------------------------------------------------------
# Orchestration function
# ---------------------------------------------------------------------------

def init_trusted_parquet_pipeline(
    spark: SparkSession,
    delta_client: DeltaLakeClient,
    landing_path: str,
    trusted_path: str,
) -> None:
    """
    Orquesta el proceso ETL completo para archivos Parquet:

    1. Instancia TrustedParquetClient con la configuración recibida.
    2. Fase Load  – Lee los archivos Parquet desde la Landing Zone.
    3. Fase Clean – Aplana structs, hace trim, elimina nulos y duplicados,
                    y divide el resultado por carpeta de origen.
    4. Fase Store – Escribe cada partición en la Trusted Zone vía DeltaLakeClient.

    El bloque finally garantiza que la SparkSession se cierre de forma segura
    incluso en caso de error.

    Parámetros
    ----------
    spark        : SparkSession activa (configurada externamente).
    delta_client : Instancia de DeltaLakeClient lista para escribir.
    landing_path : Ruta glob de origen, ej. "s3a://deltalake/**/*.parquet".
    trusted_path : Ruta base de destino, ej. "s3://trusted-zone/".
    """
    print("=" * 60)
    print("--- Starting Trusted Parquet Pipeline ---")
    print(f"    Landing path : {landing_path}")
    print(f"    Trusted path : {trusted_path}")
    print("=" * 60)

    try:
        # 1. Instanciamos el cliente de procesamiento
        client = TrustedParquetClient(
            spark=spark,
            landing_path=landing_path,
            trusted_path=trusted_path,
        )

        # ── 3. Eliminar filas con valores nulos ──────────────────────────────
        print("==================================================")
        print("[CONTROL-VERIFICACION] ¡ESTOY USANDO EL SCRIPT NUEVO SIN DROPNA ESTRICTO!")
        print("==================================================")

        # 2. Fase Load
        client.load_data()

        # 3. Fase Clean
        client.clean_data()

        # 4. Fase Store
        client.store_data(delta_client)

    except Exception as e:
        print(f"[Pipeline] Critical error during Parquet Trusted pipeline execution: {e}")
        raise

    finally:
        # Cerramos la sesión de Spark de forma segura para liberar recursos
        #spark.stop()
        print("=" * 60)
        print("--- Trusted Parquet Pipeline Completed ---")
        print("=" * 60)