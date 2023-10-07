
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import glob
import os


conn_name = 'aws_s3'
hook = S3Hook(conn_name)
bucket = 'moviedb'
json_files_path = '/opt/airflow/dags/moviedb/data/process/'

def get_files_list():
    print('> Getting list of files to upload.')
    file_pattern = f'{json_files_path}/moviedb_*.json'
    return glob.glob(file_pattern)

def s3_upload_fle(file_path, file_name):
    print(f'> Uploading file "{file_name}" to s3.')
    hook.load_file(
        filename=file_path,
        key=file_name,
        bucket_name=bucket,
        replace = False
    ) 

def uploadJson():
    print('> Starting update process.')
    files = get_files_list()
    for file_path in files:
        file_name = os.path.basename(file_path)
        s3_upload_fle(file_path, file_name)
    print('> Upload process finished.')