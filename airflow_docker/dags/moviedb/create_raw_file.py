import os
import pandas as pd
import json
import glob
from datetime import datetime

input_path = '/opt/airflow/dags/moviedb/data/process/'
output_path = '/opt/airflow/dags/moviedb/data/output/'

def get_files_list():
    print('> Getting file paths.')
    file_pattern = f'{input_path}/moviedb_*.json'
    return glob.glob(file_pattern)

def open_json(file_path):
    print(f'> Opening json file "{file_path}"')
    with open(file_path, 'r') as file:
        json_data = json.load(file)
    return pd.DataFrame(json_data)

def backdrop_prefix(backdrop_path):
    if pd.notna(backdrop_path):
        return f'https://image.tmdb.org/t/p/original{str(backdrop_path)}'
    else:
        return backdrop_path

def poster_prefix(poster_path):
    if pd.notna(poster_path):
        return f'https://image.tmdb.org/t/p/original{str(poster_path)}'
    else:
        return poster_path

def columns_select(df):
    columns = ['rank', 'id', 'title', 'original_language', 'overview', 'poster', 'backdrop', 'popularity', 'vote_average', 'vote_count', 'created_date']
    return df[columns]

def save_csv(df_sanitized, file_name_csv):
    df_sanitized.to_csv(f'{output_path}{file_name_csv}', sep='|', index=False)  
    print(f'> File succesfully created {output_path}{file_name_csv}')

def createCsv():
    files = get_files_list()
    for file_path in files:

        print(f'Starting process for file "{file_path}"')
        df = open_json(file_path)

        print('> Adjusting dataframe columns.')
        df['rank'] = range(1, len(df) + 1)
        df['created_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['backdrop_path'] = df['backdrop_path'].apply(backdrop_prefix)
        df['poster_path'] = df['poster_path'].apply(poster_prefix)
        df = df.rename(columns={'poster_path': 'poster', 'backdrop_path': 'backdrop'})
        df['overview'] = df['overview'].str.replace('"', '')

        print('> Selecting columns.')
        df_sanitized = columns_select(df)
        
        print('> Creating csv file.')
        file_name = os.path.basename(file_path)
        file_name_csv = f'{os.path.splitext(file_name)[0]}.csv'
        save_csv(df_sanitized, file_name_csv)