#import psycopg2
import csv
from airflow.hooks.postgres_hook import PostgresHook
import glob
import os

postgres_conn_id='postgres_db',
schema='moviedb'
csv_files_path = '/opt/airflow/dags/moviedb/data/output/'
table = 'movies_rank'

def getConn():
    print('> Creating connection.')
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='moviedb'
    )
    return pg_hook.get_conn()

def truncTable(conn, cursor):
    print(f'> Truncating table "{table}"')
    truncate_query = f'TRUNCATE TABLE {table}'
    cursor.execute(truncate_query)
    conn.commit()

def get_latest_csv_file():
    print('> Getting latest file.')
    file_pattern = f'{csv_files_path}/moviedb_*.csv'
    files = glob.glob(file_pattern)
    files.sort(key=os.path.getmtime)
    return files[-1]

def insert_data(conn, cursor, last_csv_file):
    print(f'> Inserting latest csv file data "{last_csv_file}" into {table}')
    with open(last_csv_file, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter="|")
        print('> Csv readed.')
        next(csv_reader)  

        for row in csv_reader:
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f'INSERT INTO {table} VALUES ({placeholders});'
            cursor.execute(insert_query, row)

    conn.commit()

    cursor.close()
    conn.close()

    print('> Csv data inserted into PostgreSQL successfully.')

def insertData():
    conn = getConn()
    cursor = conn.cursor()
    truncTable(conn, cursor)
    last_csv_file = get_latest_csv_file()
    insert_data(conn, cursor, last_csv_file)