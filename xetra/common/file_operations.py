"""
Class to store method related to files read and write operations
S3 bucket methods can also be implemented
FileOperations class is similar to S3BucketConnector class in course

Instance Variables:

Instance Methods:
"""
import pandas
from pathlib import Path
from typing import List


class FileOperations:
    """
    To interact with files on local folders
    """

    def __init__(self, file_path: str):
        """
        :param file_path: Local file path
        """
        self.file_path = file_path

    def list_files_in_location(self):
        pass

    def read_csv_to_df(self):
        pass

    def write_df_to_location(self):
        pass
