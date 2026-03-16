''' Provides cleaning utilities for the project. Contains functions for data cleaning, transformation, and preprocessing.'''
import os

import polars as pl
from pyarrow import parquet

from config.conf import DELTALAKE_STORAGE_OPTIONS, POLARS_S3_STORAGE_OPTIONS

def clean_food_cpi_data(s3_file_path: str) -> pl.DataFrame:
    ''' Cleans the raw food consumer price index (CPI) data from the FAOSTAT API. 
        Reads the raw CSV data from the specified S3 file path, drops unnecessary columns, and renames the "Area" column to "Country". 
        Returns the cleaned data as a Polars DataFrame.'''
    df = pl.read_csv(s3_file_path, storage_options=POLARS_S3_STORAGE_OPTIONS)
    df = df.drop("Domain Code", "Area Code", "Year Code", "Item Code", "Months Code", "Element Code", "Element", "Unit", "Flag", "Flag Description", "Note")
    df = df.rename({"Area": "Country"})
    return df

def rmdir_recursively(path: str):
    ''' Recursively removes a directory and all its contents. '''
    if os.path.isdir(path):
        for entry in os.listdir(path):
            entry_path = os.path.join(path, entry)
            if os.path.isdir(entry_path):
                rmdir_recursively(entry_path)
            else:
                os.remove(entry_path)
        os.rmdir(path)
    else:
        raise ValueError(f"The provided path '{path}' is not a directory.")

def df_to_parquet(df: pl.DataFrame, file_path: str) -> str:
    ''' Writes a Polars DataFrame to a Parquet file at the specified file path. '''
    df.write_parquet(file_path)
    return file_path