"""
Main xetra ETL process using function based approach
"""
import pandas
import boto3
import argparse

from io import StringIO, BytesIO
from datetime import datetime, timedelta
from typing import List


# Adapter Layer
def create_s3_resource(resource_name):
    return boto3.resource(resource_name)


def create_bucket(s3_resource, bucket_name):
    return s3_resource.Bucket(bucket_name)


def read_csv_df(bucket_resource, file_key: str, file_delimiter: str = ',', encoding: str = 'utf-8'):
    """
    Load csv file into data frame
    :param bucket_resource:
    :param file_key:
    :param file_delimiter:
    :param encoding:
    :return:
    """
    try:
        csv_object = bucket_resource.Object(key=file_key).get().get('Body').read().decode(encoding)
        csv_content = StringIO(csv_object)
        data_frame = pandas.read_csv(filepath_or_buffer=csv_content, delimiter=file_delimiter)
        return data_frame
    except Exception:
        raise


def write_df_s3(bucket_resource, data_frame, file_key):
    """
    Upload transformed parquet file to target s3 bucket
    :param bucket_resource:
    :param data_frame:
    :param file_key:
    :return:
    """
    try:
        out_buffer = BytesIO()
        data_frame.to_parquet(out_buffer, index=False)
        bucket_resource.put_object(Body=out_buffer.getvalue(), Key=file_key)
    except Exception:
        raise


def convert_date_string(date_string: str, date_time_format: str) -> datetime:
    """
    Convert string to a specified date time format
    :param date_string:
    :param date_time_format:
    :return: date
    """

    try:
        return datetime.strptime(date_string, date_time_format).date()
    except ValueError:
        raise Exception


def get_process_date(date_time_format: str, days_difference: int, default_date: str = None) -> datetime:
    try:
        parser = argparse.ArgumentParser(description='Process date to select data folders')
        parser.add_argument('--proc_date', default=default_date, metavar='YYYY-MM-DD', help='Process Date',
                            type=str)
        args = parser.parse_args()
        if args.proc_date:
            process_date = convert_date_string(args.proc_date, date_time_format) - timedelta(days=days_difference)
        else:
            process_date = datetime.now().date() - timedelta(days=days_difference)
        return process_date
    except Exception:
        raise


def update_meta_file():
    pass


def return_bucket_objects(bucket_resource, prefix=None):
    """
    Extract all files list from s3 bucket
    :param prefix:
    :param bucket_resource:
    :return:
    """

    bucket_objects = [obj.key for obj in bucket_resource.objects.filter(Prefix=prefix)]
    return bucket_objects


# Application Layer not core
def return_dates_list(process_date, date_time_format):
    today = datetime.today().date()
    dates_list = [(process_date + timedelta(days=i)).strftime(date_time_format) for i in
                  range(0, (today - process_date).days + 1)]
    return dates_list


# Application Layer
def extract_files_data(bucket_resource, dates_list):
    """
    Extract files data into a main data frame
    :param dates_list:
    :param bucket_resource:
    :return:
    """
    files_list = [files for k in dates_list for files in return_bucket_objects(bucket_resource, k)]
    data_frame = pandas.concat(objs=[read_csv_df(bucket_resource, f) for f in files_list],
                               ignore_index=True)
    return data_frame


def data_transformation(data_frame, process_date, date_time_format, columns):
    """
    Transform csv data
    :param date_time_format:
    :param columns:
    :param data_frame:
    :param process_date:
    :return: Data Frame
    """
    data_frame = data_frame.loc[:, columns]
    data_frame.dropna(inplace=True)

    # add column opening price
    data_frame['opening_price'] = data_frame.sort_values(by=['Time']).groupby(['ISIN', 'Date'])[
        'StartPrice'].transform('first')

    # add column closing price
    data_frame['closing_price'] = data_frame.sort_values(by=['Time']).groupby(['ISIN', 'Date'])[
        'StartPrice'].transform('last')

    # data aggregation
    data_frame = data_frame.groupby(['ISIN', 'Date'], as_index=False).agg(
        opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'),
        minimum_price_eur=('MinPrice', 'min'),
        maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume', 'sum'))

    # percent change previous closing price
    data_frame['previous_closing_price'] = data_frame.sort_values(by=['Date']).groupby(['ISIN'])[
        'closing_price_eur'].shift(1)
    data_frame['change_prev_closing_%'] = (data_frame['closing_price_eur'] - data_frame[
        'previous_closing_price']) / data_frame['previous_closing_price'] * 100
    data_frame.drop(columns=['previous_closing_price'], inplace=True)
    data_frame = data_frame.round(decimals=2)
    # data_frame['Date'] = pandas.to_datetime(data_frame['Date'], format=date_time_format)
    # data_frame = data_frame[data_frame.Date >= process_date]

    return data_frame


def load_data(bucket_resource, data_frame, parquet_file_key, date_time_format):
    file_key = f"{datetime.today().strftime(date_time_format)}/{parquet_file_key}_{datetime.today().strftime('%Y%m%d_%H%M%S')}.parquet"
    write_df_s3(bucket_resource, data_frame, file_key)


# Data Pipeline
def etl_process(source_bucket_resource: str, target_bucket_resource: str, dates_list, process_date,
                date_time_format: str, columns: List, parquet_file_key: str):
    """"
    Main ETL pipeline process
    """
    print("Starting main ETL process")

    # Data extraction process
    print(f"Starting data extraction since: {process_date}")
    data_frame = extract_files_data(source_bucket_resource, dates_list)
    print(data_frame.shape)

    # Data transformation
    print(f"Starting data transformation")
    final_data_frame = data_transformation(data_frame, process_date, date_time_format, columns)
    print(final_data_frame.shape)

    # Load final data after transformation
    print(f"Starting data load")
    # load_data(target_bucket_resource, final_data_frame, parquet_file_key)


def main():
    """
    Main method
    :return: None
    """
    # define variables and parameters
    source_bucket = 'xetra-1234'
    target_bucket = 'xetra1-project-bucket'
    date_time_format = '%Y-%m-%d'
    default_date = '2021-04-22'
    days_delta = 1
    columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']
    parquet_file_key = 'xetra_daily_report'

    # Initializing s3 bucket resources
    s3 = create_s3_resource('s3')
    source_bucket_resource = create_bucket(s3, source_bucket)
    target_bucket_resource = create_bucket(s3, target_bucket)

    # Get process date
    process_date = get_process_date(date_time_format, days_delta, default_date)

    # List of all dates since process date till today
    # dates_list = return_dates_list(process_date, date_time_format)
    ########################################################################################################
    ########################################################################################################
    ########################################################################################################

    df_meta = read_csv_df(target_bucket_resource, 'meta_file.csv')
    print(df_meta)
    today = datetime.today().date()
    dates_list = [(process_date + timedelta(days=i)) for i in range(0, (today - process_date).days + 1)]

    # unique processed dates from meta file
    source_dates = set(pandas.to_datetime(df_meta['source_date']).dt.date)
    min_process_date=
    print(source_dates)
    print(dates_list)
    exit(1)
    # main etl pipeline process
    etl_process(source_bucket_resource, target_bucket_resource, dates_list, process_date, date_time_format, columns,
                parquet_file_key)


if __name__ == '__main__':
    main()
