''' Configuration values for the project. '''

import os
import dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#print(f"Project root directory: {PROJECT_ROOT}")

#LOAD ENVIRONMENT VARIABLES
_dotenv_path = os.path.join(PROJECT_ROOT, "config", ".env")
#print(f".env path: {_dotenv_path}")
dotenv.load_dotenv(_dotenv_path)

### MINIO 
# Connection settings
MINIO_LOCAL = True
MINIO_HOST =  "localhost:9000" if MINIO_LOCAL else "minio:9000"
MINIO_ENDPOINT   = f"http://{MINIO_HOST}"
MINIO_ACCESS_KEY = os.getenv("MINIO_UNAME")
MINIO_SECRET_KEY = os.getenv("MINIO_PW")


### DELTA LAKE
DELTALAKE_STORAGE_OPTIONS = {
    "endpoint_url": MINIO_ENDPOINT,
    "access_key_id": MINIO_ACCESS_KEY,
    "secret_access_key": MINIO_SECRET_KEY,
    "region": "eu-west-1",
    "allow_http": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "conditional_put": "etag",
}

# Optional: separate options for Polars read_csv(..., storage_options=...)
POLARS_S3_STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT, "region_name": "eu-west-1"},
    "use_ssl": False,
}

DELTALAKE_TABLES = {
    "FAOSTAT_FOOD_CPI": "s3://deltalake/faostat/food_cpi"
}


### API RELEVANT ENVIRONMNET VARIABLES AND CONSTANTS
## KAGGLE API
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")
KAGGLE_5k_DATASET = "gillesokhin/nutrition5k-dataset"
KAGGLE_FOODCOM_DATASET = "shuyangli94/food-com-recipes-and-user-interactions"

## SPOONOCULAR API
SPOON_KEY = os.getenv("SPOON_KEY")
SPOON_BASE_URL = url = "https://api.spoonacular.com/recipes/random"

## USDA API
USDA_KEY = os.getenv("USDA_KEY")
USDA_BASE_URL = "https://api.nal.usda.gov/fdc/v1/foods/search"

## OFF API
OFF_BASE_URL = "https://world.openfoodfacts.org/api/v2/search"
OFF_BASE_HEADER = {"User-Agent": "BigDataManagementProjectUniversitatPolitecnicaCatalunya/1.0 (contact: sergi.gonzalez.martos@estudiantat.upc.edu)"}

## FAOSTAT API
FAOSTAT_UNAME = os.getenv("FAOSTAT_UNAME")
FAOSTAT_PWD = os.getenv("FAOSTAT_PWD")
FAOSTAT_TOKEN_TIMEOUT = 3600    # Seconds
FAOSTAT_BASE_URL = "https://faostatservices.fao.org/api/v1"
FAOSTAT_EU_COUNTRY_CODES = ','.join([    "11",    # Austria
                                "255",   # Belgium
                                "27",    # Bulgaria
                                "98",    # Croatia
                                "50",    # Cyprus
                                "167",   # Czechia
                                "54",    # Denmark
                                "63",    # Estonia
                                "67",    # Finland
                                "68",    # France
                                "79",    # Germany
                                "84",    # Greece
                                "97",    # Hungary
                                "104",   # Ireland
                                "106",   # Italy
                                "119",   # Latvia
                                "126",   # Lithuania
                                "256",   # Luxembourg
                                "134",   # Malta
                                "150",   # Netherlands (Kingdom of the)
                                "173",   # Poland
                                "174",   # Portugal
                                "183",   # Romania
                                "199",   # Slovakia
                                "198",   # Slovenia
                                "203",   # Spain
                                "210"    # Sweden
])
FAOSTAT_DOMAIN_CODES = {
    "CONSUMER_PRICES": "CP",
    "PRODUCER_PRICES": "PP"
}
FAOSTAT_ITEM_CODES = {
    "FOOD_CPI": "23013",
    "FOOD_PPI": "5539",     
    "PP_LCU": "5530",
}
