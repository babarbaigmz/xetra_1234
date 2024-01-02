import pandas
from pathlib import Path
import boto3
import argparse
from typing import List
from datetime import datetime, timedelta


# Adapter Layer
# def get_s3_items():
#     s3_resource = boto3.resource('s3')
#     bucket = s3_resource.Bucket('xetra-1234')
#     bucket_obj = bucket.objects.filter(Prefix='2022-01-28/')
#     for items in bucket_obj:
#         print(items)


def get_arguments(p_date_format: str, p_days_delta: int, p_default_date: str = None) -> datetime:
    try:
        parser = argparse.ArgumentParser(description='Process date to select data folders')
        parser.add_argument('--process_dt', default=p_default_date, metavar='YYYY-MM-DD', help='Process Date', type=str)
        args = parser.parse_args()
        if args.process_dt is None:
            process_date = datetime.now().date() - timedelta(days=p_days_delta)
        else:
            process_date = datetime.strptime(args.process_dt, p_date_format).date() - timedelta(days=p_days_delta)
        return process_date
    except ValueError:
        raise Exception


# Old method with generator
# def get_files_list(p_file_path: Path, p_file_extension, p_process_date: datetime, p_date_format: str):
#     for files in p_file_path.iterdir():
#         try:
#             if datetime.strptime(files.stem, p_date_format).date() >= p_process_date:
#                 for f in Path(p_file_path).glob(f'*{files.stem}/*'):
#                     if f.name.endswith(p_file_extension):
#                         yield f
#         except ValueError:
#             continue


def get_files_paths_list(p_file_path: Path, p_file_string: str, p_file_extension: str):
    files_path_list = []
    for files in p_file_path.iterdir():
        if p_file_string in files.as_posix():
            for fpath in Path(p_file_path).glob(f'*{p_file_string}/*'):
                if fpath.name.endswith(p_file_extension):
                    files_path_list.append(fpath)
    return files_path_list


def read_file_dataframe(p_file_path: str, p_file_delimiter: str):
    try:
        data_frame = pandas.read_csv(filepath_or_buffer=Path(p_file_path), delimiter=p_file_delimiter)
        return data_frame
    except Exception:
        raise


def load_files_data(p_file_path: Path, p_data_frame):
    p_data_frame.to_parquet(path=p_file_path, index=False)


def write_csv_files_data(p_file_path: Path, p_data_frame):
    p_data_frame.to_csv(path_or_buf=p_file_path, index=False)


# Application Layer

def extract_all(p_files_path: Path, p_file_delimiter: str, p_file_extension: str, p_process_date_list: List):
    files_path = [fpath for date_string in p_process_date_list for fpath in
                  get_files_paths_list(p_files_path, date_string, p_file_extension)]

    main_data_frame = pandas.concat(
        objs=[read_file_dataframe(paths, p_file_delimiter) for paths in files_path],
        ignore_index=True)
    return main_data_frame


def transform_data(data_frame: pandas.DataFrame, p_process_date: datetime, p_date_format):
    data_frame.dropna(inplace=True)
    data_frame['opening_price'] = data_frame.sort_values(by=['Time']).groupby(['ISIN', 'Date'])[
        'StartPrice'].transform('first')
    data_frame['closing_price'] = data_frame.sort_values(by=['Time']).groupby(['ISIN', 'Date'])[
        'StartPrice'].transform('last')
    data_frame = data_frame.groupby(['ISIN', 'Date'], as_index=False).agg(
        opening_price_eur=('opening_price', 'min'),
        closing_price_eur=('closing_price', 'min'),
        minimum_price_eur=('MinPrice', 'min'),
        maximum_price_eur=('MaxPrice', 'max')
    )

    data_frame['previous_closing_price'] = data_frame.sort_values(by=['Date']).groupby(['ISIN'])[
        'closing_price_eur'].shift(1)
    data_frame['change_prev_closing_%'] = (data_frame['closing_price_eur'] - data_frame[
        'previous_closing_price']) / data_frame['previous_closing_price'] * 100
    data_frame.drop(columns=['previous_closing_price'], inplace=True)
    data_frame = data_frame.round(decimals=2)
    # data_frame = data_frame[data_frame['Date'] >= p_process_date.strftime(p_date_format)]
    return data_frame


def etl_process(p_data_set_path: str, p_output_file_path: str,p_meta_file_path:str, p_process_dates_list: List, p_default_date: str,
                p_date_format: str,
                p_days_delta: int, p_file_delimiter: str, p_file_extension: str, parquet_filename: str):
    process_date = get_arguments(p_date_format, p_days_delta, p_default_date)
    print(f"Starting data load process since: {process_date}")

    # extract dates to be processed

    # extract all files into data frame
    data_frame_all = extract_all(Path(p_data_set_path), p_file_delimiter, p_file_extension, p_process_dates_list)
    # transform extracted data
    data_frame_all = transform_data(data_frame_all, process_date, p_date_format)

    # output data frame to folder
    load_files_data(Path(p_output_file_path, parquet_filename), data_frame_all)
    update_meta_file(p_meta_file_path, p_file_delimiter, p_process_dates_list)


def return_dates_list(p_file_path: str, p_file_delimiter: str, p_process_date: str, p_date_format: str):
    today = datetime.today().date()
    min_date = datetime.strptime(p_process_date, p_date_format).date() - timedelta(days=1)
    try:
        df_meta = read_file_dataframe(Path(p_file_path), p_file_delimiter)
        dates = [(min_date + timedelta(days=x)) for x in range(0, (today - min_date).days + 1)]
        src_dates = set(pandas.to_datetime(df_meta['source_date']).dt.date)
        dates_missing = set(dates[1:]) - src_dates
        if dates_missing:
            min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
            return_dates = [date.strftime(p_date_format) for date in dates if date >= min_date]
            return_min_date = (min_date + timedelta(days=1)).strftime(p_date_format)
        else:
            return_dates = []
            return_min_date = datetime(2200, 1, 1).date()
    except Exception:
        return_dates = [(min_date + timedelta(days=x)).strftime(p_date_format) for x in
                        range(0, (today - min_date).days + 1)]
        return_min_date = p_process_date
    return return_min_date, return_dates


def update_meta_file(p_file_path: str, p_file_delimiter, extracted_date_list):
    dataframe_new = pandas.DataFrame(columns=['source_date', 'datetime_of_processing'])
    dataframe_new['source_date'] = extracted_date_list
    dataframe_new['datetime_of_processing'] = datetime.today().strftime('%Y-%m-%d')
    dataframe_old = read_file_dataframe(p_file_path, p_file_delimiter)
    dataframe_all = pandas.concat(dataframe_old, dataframe_new)
    write_csv_files_data(p_file_path, dataframe_all)


def main():
    data_set_path = r'D:\OneDrive\Babar\Main\Python\Projects\xetra_project\Resources\dataset'
    meta_file_path = r'D:\OneDrive\Babar\Main\Python\Projects\xetra_project\meta_file.csv'
    output_file_path = r'D:\OneDrive\Babar\Main\Python\Projects\xetra_project\Resources\dataset\output'
    default_date = '2022-03-15'
    date_format = '%Y-%m-%d'
    file_delimiter = ','
    parquet_filename = 'main_data.parquet'
    days_delta = 1
    file_extension = 'csv'

    extract_date, process_dates_list = return_dates_list(meta_file_path, file_delimiter, default_date, date_format)

    etl_process(p_data_set_path=data_set_path,
                p_output_file_path=output_file_path,
                p_meta_file_path=meta_file_path,
                p_process_dates_list=process_dates_list,
                p_default_date=extract_date,
                p_date_format=date_format,
                p_days_delta=days_delta,
                p_file_delimiter=file_delimiter,
                p_file_extension=file_extension,
                parquet_filename=parquet_filename
                )


if __name__ == '__main__':
    main()
