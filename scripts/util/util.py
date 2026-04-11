''' Provides cleaning utilities for the project. Contains functions for data cleaning, transformation, and preprocessing.'''
import os

import polars as pl
from pyarrow import parquet

from scripts.conf import DELTALAKE_STORAGE_OPTIONS, POLARS_S3_STORAGE_OPTIONS

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