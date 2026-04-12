import json
import requests
import os
import sys
import polars as pl
from datetime import datetime

from scripts.ingest import MinioClient, DeltaLakeClient
from scripts.util import util

# Import configuration constants
from scripts.conf import (
    POLARS_S3_STORAGE_OPTIONS, 
    DELTALAKE_TABLES,
    USDA_KEY,
    USDA_PAGE_SIZE,
    USDA_BASE_URL
)

class USDAClient:
    ''' Client for interacting with the USDA FoodData Central API. '''
    
    def __init__(self):
        self.base_url = USDA_BASE_URL
        self.api_key = USDA_KEY

    def fetch_food_recipe(self, ingredient: str, page_size: int = 10) -> dict:
        ''' 
        Searches for food items in the USDA database using a POST request.
        Captures the 100% of the JSON response.
        '''
        payload = {
            "query": ingredient,
            "pageSize": page_size,
            "dataType": ["Foundation", "Survey (FNDDS)"],
            "api_key": self.api_key
        }
        
        # USDA API often requires the API key both in payload and as a parameter
        response = requests.post(
            self.base_url, 
            json=payload, 
            params={"api_key": self.api_key}, 
            timeout=30
        )
        
        if response.status_code == 200:
            items = response.json().get("foods",[])
            if items: return {
                'ingredient': ingredient,
                'recipes': items
            }
        

    def fetch_new_recipes(self):
        FOOD_DATABASE = [
            # Fruits and vegetables
            "apples", "bananas", "oranges", "strawberries", "blueberries", "lemons", "limes", "avocados",
            "onions", "garlic", "potatoes", "carrots", "tomatoes", "bell-peppers", "broccoli", "spinach", 
            "lettuces", "cucumbers", "zucchinis", "mushrooms", "celery", "asparagus", "ginger",
            
            # Meat and proteins
            "chicken-breasts", "chicken-thighs", "ground-beef", "steaks", "pork-chops", "bacon", 
            "hams", "turkeys", "salmons", "shrimps", "tunas", "eggs", "tofus",
            
            # Lactics and eggs
            "milks", "butters", "yogurts", "sour-creams", "heavy-creams", "cheeses", "cheddar-cheeses", 
            "parmesan-cheeses", "mozzarella-cheeses", "cream-cheeses", "almond-milks", "oat-milks",
            
            # Pantry
            "flours", "sugars", "brown-sugars", "honeys", "rice", "pastas", "spaghetti", "oats", 
            "breads", "bread-crumbs", "tortillas", "olive-oils", "vegetable-oils", "canola-oils",
            "apple-cider-vinegars", "balsamic-vinegars", "soy-sauces", "mayonnaises", "mustards", "ketchups",
            
            # Legumes
            "chickpeas", "black-beans", "lentils", "kidney-beans", "tomato-sauces", "broths", "stocks",
            
            # Species and repostery
            "salts", "black-peppers", "cinnamons", "vanilla-extracts", "baking-powders", "baking-soda",
            "paprikas", "cumins", "oregano", "basil", "thyme", "rosemary", "chili-powders"
        ]
        full_batch = []
        for food in FOOD_DATABASE:
            result = self.fetch_food_recipe(food, USDA_PAGE_SIZE)
            if result:
                full_batch.append(result)
                
        return full_batch

'''
Note: The USDA API has a limit of 1.000 requests per hour per IP address.
'''
def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient):
    
    ''' 
    Orchestrates the USDA Ingestion:
    1. Fetches raw food data from the API.
    2. Persists the 100% raw JSON in the 'raw-data' bucket.
    3. Transfers the 100% content to the 'deltalake' bucket using Polars.
    '''
    
    # Initialize API Client
    usda_client = USDAClient()
    
    # Ensure buckets exists
    minio_client.create_buckets()

    print(f"--- Starting USDA Ingestion ---")

    try:
        # 1. Fetch data from API
        data = usda_client.fetch_new_recipes()

        # 2. Define naming convention and paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"usda_{timestamp}.json"
        object_key = f"usda/{filename}"

        # 3. Upload the file to MinIO "raw-data" bucket.
        minio_client.upload_object(data, "raw-data", object_key)
        print(f"Step 1: Raw JSON uploaded to s3://raw-data/{object_key}")

        # 4. Upload the file to MinIO "deltalake" bucket.
        if data:
            delta_client.write_table(data, DELTALAKE_TABLES["USDA"], partition_by=None)        
            print(f"Step 2: 100% of content registered in Delta Lake at {DELTALAKE_TABLES['USDA']}")

    except Exception as e:
        print(f"Critical error during USDA ingestion: {e}")

    print("--- USDA Ingestion Completed ---")