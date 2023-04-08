"""
File to Store constants
"""
from enum import Enum


class FileTypes(Enum):
    """
    supported file types for process
    """
    CSV = 'csv'
    PARQUET = 'parquet'


class MetaProcessFormat(Enum):
    """
    formation for MetaProcess class
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'


print(FileTypes.CSV)
