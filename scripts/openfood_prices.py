import requests
import json
import time
import os
from datetime import datetime

import polars as pl

from scripts.ingest import MinioClient, DeltaLakeClient
from scripts.util import util

from scripts.conf import (
    DELTALAKE_TABLES,
    OFFP_BASE_URL,
    OFFP_BASE_HEADER
)

class OpenFoodFactsPricesClient:
    '''Client for interacting with the OpenFoodFacts Prices API'''
    
    def __init__(self):
        # Endpoint de la API de Precios de Open Food Facts
        self.base_url = OFFP_BASE_URL
        self.headers = OFFP_BASE_HEADER
        base_path = os.path.dirname(os.path.abspath(__file__))
        self.state_file = os.path.join(base_path, "openfood_prices_state.json")
        
    def _get_last_processed_page(self) -> int:
        ''' Reads local state file to determine where to resume '''
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    return state.get('last_page', 0)
            except Exception as e:
                print(f"Error reading state file: {e}")
        return 0
    
    def _save_current_page(self, page: int):
        ''' Persists the current page number to maintain ingestion state. '''
        state = {
            'last_page': page,
            'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=4)

    def fetch_new_product(self, category: str, page: int, page_size: int, max_retries: int = 3) -> dict:
        ''' fetches product data for specirfic page '''
        category_tag = category if ":" in category else f"en:{category}"
        european_countries = "es,fr,it,de,be,pt,nl" # spain, france, italy, germany, begium, portugal, netherlands
        params = {
                "category_tag": category_tag,
                "country_code": european_countries,
                "page": page,
                "size": page_size,
                "order_by": "-unique_scans_n"
            }
        
        success = False
        retries = 0
        
        while not success and retries < max_retries:
            try:
                res = requests.get(self.base_url, params=params, headers=self.headers, timeout=30)
                
                if res.status_code == 200:
                    items = res.json().get("items", [])
                    if items:
                        return {
                            'category': category,
                            'data': items
                        }
                    success = True
                    time.sleep(1.5)
                elif res.status_code == 429:
                    retries += 1
                    wait_time = 30*retries
                    time.sleep(wait_time)
                
            except Exception as e:
                retries += 1
                print(f"Error in {category}: {e}")
                time.sleep(5)

    def fetch_new_pages(self, page: int, page_size: int=100):
        ''' Obtains one page for each category/food'''
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
            result = self.fetch_new_product(food, page, page_size)
            if result:
                full_batch.append(result)
            
        return full_batch


def store_data(minio_client: MinioClient, delta_client: DeltaLakeClient, object_key:str, data):
    minio_client.create_buckets()
    
    # upload file to MinIO 'raw-data' bucket
    minio_client.upload_object(data, 'raw-data', object_key)
    print(f"Success: Raw data uploaded to s3://raw-data/{object_key}")
    
    # upload file to MinIO 'deltalake' bucket
    delta_client.write_table(data, DELTALAKE_TABLES["OPENFOODFACTS_PRICES"], partition_by=None)
    print(f"Success: uploaded to s3://deltalake/{object_key}")
    
def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient, pages_to_download: int, page_size: int):
    '''
    Orchestrates ingestion for OpenFoodFacts Prices:
    1. Fetches raw data from the API
    2. Uploads raw JSON classified by category to the 'raw-data' bucket
    3. Transfers the 100% content to the 'deltalake' bucket using Polars.
    '''
    client = OpenFoodFactsPricesClient()
    last_page = client._get_last_processed_page()
    start_page = last_page + 1
    end_page = last_page + pages_to_download

    for current_page in range(start_page, end_page + 1):
        try:
            data = client.fetch_new_pages(current_page, page_size)
            
            # define name convention
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"offp_page_{current_page}_{timestamp}.json"
            object_key = f"openfoodfacts/prices/{filename}"
            
            store_data(minio_client, delta_client, object_key, data)
            client._save_current_page(current_page)
            
        except Exception as e:
            print(f"Critical error on page {current_page}: {e}")
            break
            
            