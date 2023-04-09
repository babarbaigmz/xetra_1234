"""
Xetra ETL Component
"""
from typing import NamedTuple
from xetra.common.file_operations import FileOperations


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data
    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volume in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    trg_col_isin:
    trg_col_date:
    trg_col_op_price:
    trg_col_clos_price:
    trg_col_min_price:
    trg_col_max_price:
    trg_col_dail_trad_vol:
    trg_col_ch_prev_clos: column name for change to previous day's closing
    trg_key: basic key of target file
    trg_key_date_format:
    trg_format: file format of the target file:
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL:
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(self, files_source: FileOperations, files_target: FileOperations, meta_key: str,
                 src_args: XetraSourceConfig, target_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer
        :param files_source: connection to source files location
        :param files_target: connection to target files location
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param target_args: NamedTouple class with target configuration data
        """
        self.files_source = files_source
        self.files_source = files_target
        self.meta = meta_key
        self.src_args = src_args
        self.target_args = target_args
        self.extract_date = ''
        self.extract_date_list = ''
        self.meta_update_list = ''

    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass
