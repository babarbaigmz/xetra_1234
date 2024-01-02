"""
Running the xetra ETL application
"""
import boto3

s3_bucket_name = 'xetra-1234'


# s3_resource = boto3.resource('s3')


def get_bucket_contents(s3_client_param, bucket_name_param, prefix_param=''):
    s3_response = s3_client_param.list_objects_v2(Bucket=bucket_name_param, Prefix=prefix_param)
    return s3_response


def main():
    s3_response = get_bucket_contents(boto3.client('s3'), s3_bucket_name, '2022-03-15')



if __name__ == '__main__':
    main()
