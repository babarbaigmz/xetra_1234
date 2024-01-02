"""
File to test various code snippets
"""

import pandas
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List


def get_arguments(date_format_param: str, days_delta_param: int, default_date_param: str = None) -> datetime:
    try:
        parser = argparse.ArgumentParser(description='Process date to select data folders')
        parser.add_argument('--proc_date', metavar='YYYY-MM-DD', help='Process Date', type=str)
        args = parser.parse_args()
        if args.proc_date:
            process_date = datetime.strptime(args.proc_date, date_format_param).date() - timedelta(
                days=days_delta_param)
        else:
            process_date = datetime.now().date() - timedelta(days=days_delta_param)
        return process_date
    except ValueError:
        raise Exception


def get_files_paths_list(file_path: Path, file_string: str = '2022-03-15', file_extension: str = None) -> List:
    """ Read the list of csv files from the main folder"""
    files_path_list = []
    for files in file_path.iterdir():
        # print(files)
        # if file_string in files.as_posix():
        for fpath in file_path.glob(f'*{file_string}/*'):
            # if fpath.name.endswith(file_extension):
            # print(fpath)
            files_path_list.append(fpath)
    return files_path_list


def read_files(files_path: Path):
    print(get_files_paths_list(files_path))


def main():
    """ Main function to execute the process"""
    # Set variables
    data_files_main_path = Path(r'D:\OneDrive\Babar\Main\Python\Projects\xetra_project\resources\dataset')
    meta_file_path = Path(r'D:\OneDrive\Babar\Main\Python\Projects\xetra_project\meta_file.csv')
    output_file_path = Path(r'D:\OneDrive\Babar\Main\Python\Projects\xetra_project\resources\dataset\output')
    default_date = '2022-03-15'
    date_format = '%Y-%m-%d'
    file_delimiter = ','
    parquet_filename = 'main_data.parquet'
    days_delta = 1
    file_extension = 'csv'

    print(get_arguments(date_format, days_delta, default_date))
    # read_files(data_files_main_path)


if __name__ == '__main__':
    main()
