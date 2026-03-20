'''main.py: Main entry point for the data ingester.'''

import time
import os
import tempfile

import pandas as pd
import schedule

from config.conf import FAOSTAT_EU_COUNTRY_CODES, PROJECT_ROOT, DELTALAKE_TABLES
import src.util.util as util
from fetch import init_fetch
from ingest import MinioClient, DeltaLakeClient

def some_recurring_task():
    '''Example of a recurring task that could be scheduled to run periodically.'''
    print("This is a recurring task that runs every 5 seconds.")

schedule.every(5).seconds.do(some_recurring_task)

def main():
    minio_client = MinioClient()
    delta_client = DeltaLakeClient()

    ## INIT DATA FETCH ACROSS ALL APIS
    init_fetch(minio_client, delta_client)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
    

if __name__ == "__main__":
    main()
