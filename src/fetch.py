''' Fetching data from the web. Provides classes and methods for all different APIs needed for the project.'''

from datetime import datetime, timedelta
from io import StringIO

import requests
import pandas as pd

from api.faostat import init_fetch as faostat_init_fetch
from ingest import DeltaLakeClient, MinioClient

def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient):
    '''This function is only run once on service startup. 
    It fetches all historic data from all the different APIs and saves it to to the raw-bucket as well as the deltalake.'''
    
    # Fetch food CPI data from FAOSTAT API
    faostat_init_fetch(minio_client, delta_client)
    # ADD OTHER INIT API FETCHES HERE @SERGI @MARTA
