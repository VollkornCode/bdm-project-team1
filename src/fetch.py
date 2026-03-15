''' Fetching data from the web. Provides classes and methods for all different APIs needed for the project.'''

from datetime import datetime, timedelta
from io import StringIO

import requests
import pandas as pd

from config.conf import FAOSTAT_BASE_URL, FAOSTAT_TOKEN_TIMEOUT, FAOSTAT_EU_COUNTRY_CODES, FAOSTAT_ITEM_CODES, FAOSTAT_DOMAIN_CODES
from util.api_utils import get_faostat_auth_token

### FAOSTAT API Wrapper
# See https://www.fao.org/faostat/en/#developer-portal for more details on the API.
class FaostatClient:
    ''' Client for interacting with the FAOSTAT API. '''
    
    def __init__(self):
            self.auth_token = get_faostat_auth_token()
            self.auth_token_date = datetime.now()

    def _refresh_token_if_needed(self):
        ''' Refreshes the authentication token if it is older than FAOSTAT_TOKEN_TIMEOUT. '''
        if datetime.now() - self.auth_token_date > timedelta(seconds=FAOSTAT_TOKEN_TIMEOUT-5):
            self.auth_token = get_faostat_auth_token()
            self.auth_token_date = datetime.now()

    def get_food_cpi(self, country_codes: str, start_year: int | None = None, end_year: int | None = None) -> pd.DataFrame:
        ''' Fetches the food consumer price index (CPI) data from the FAOSTAT API for the specified country codes and year range. 
            Returns csv as Dataframe'''
        self._refresh_token_if_needed()
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        domain_code = FAOSTAT_DOMAIN_CODES["CONSUMER_PRICES"]
        food_cpi_code = FAOSTAT_ITEM_CODES["FOOD_CPI"]
        params = {
            "item": food_cpi_code,
            "area": country_codes,
            "output_type": "csv"
        }
        if start_year is not None and end_year is not None:
            year_codes = ','.join([str(year) for year in range(start_year, end_year + 1)])
            params["year"] = year_codes

        url = f"{FAOSTAT_BASE_URL}/en/data/{domain_code}"
        print(f"Fetching food consumer price indices for the EU countries...")
        respone_start_time = datetime.now()
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response_end_time = datetime.now()
        print(f"Response time: {response_end_time - respone_start_time}")
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_content = response.content
        # Parse CSV to Dataframe from response bytes
        try:
            df = pd.read_csv(StringIO(response_content.decode('utf-8')))
        except Exception as e:
            raise Exception(f"Failed to parse CSV data from response: {e}")
        return df

