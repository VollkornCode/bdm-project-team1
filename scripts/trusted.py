''' File providing methods for reading data from landing zone, apply generic transformations and write it in trsuted zone''' 

from deltalake import DeltaTable, write_deltalake
import duckdb
import polars as pl
import boto3
import json
import os
import sys
import requests
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError

class TrustedClient:
    dataset: pd.Dataframe = None
    
    def load_data():

    def store_data()
        
    def clean_CSV():

    def clean_JSON():

    def clean_IMAGES():