from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from moviedb.get_moviedb_json import createJson
from moviedb.upload_file_s3 import uploadJson
from moviedb.create_raw_file import createCsv
from moviedb.insert_data_psql import insertData

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def validateTask1(**kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='t1_create_json')
    
    if result: 
        return 't2_file_created'
    else:
        return 't3_no_file'

with DAG('etl_moviedb', start_date = datetime(2023,9,26), 
        schedule_interval= '30 00 * * *', catchup=False) as dag:

    t1_create_json = PythonOperator(
        task_id = 't1_create_json',
        python_callable = createJson
    ) 

    branch_t1 = BranchPythonOperator(
        task_id='branch_t1',
        python_callable=validateTask1,
        provide_context=True
    )

    t2_file_created = BashOperator(
        task_id = 't2_file_created',
        bash_command = 'echo "File succesfully created."'
    )
    
    t3_no_file = BashOperator(
        task_id = 't3_no_file',
        bash_command = 'echo "No file was created, possibly due to a lack of response from the TMDB API."'
    )

    dummy_t3_no_file = DummyOperator(
        task_id='dummy_t3_no_file'
    )

    t4_upload_json = PythonOperator(
        task_id = 't4_upload_json',
        python_callable = uploadJson
    ) 

    t5_create_raw_csv = PythonOperator(
        task_id = 't5_create_raw_csv',
        python_callable = createCsv
    ) 

    t6_insert_data_psql = PythonOperator(
        task_id = 't6_insert_data_psql',
        python_callable = insertData
    ) 

    t7_clean_process_dir = BashOperator(
        task_id = 't7_clean_process_dir',
        bash_command = 'find /opt/airflow/dags/moviedb/data/process/ -type f -exec echo "Deleting file: {}" \; -exec rm -f {} \;'
    ) 

    t8_clean_output_dir = BashOperator(
        task_id = 't8_clean_output_dir',
        bash_command = 'find /opt/airflow/dags/moviedb/data/output/ -type f -exec echo "Deleting file: {}" \; -exec rm -f {} \;'
    ) 

    t1_create_json >> branch_t1 >> [t2_file_created, t3_no_file]
    t2_file_created >> t4_upload_json >> t5_create_raw_csv >> t6_insert_data_psql >> t7_clean_process_dir >> t8_clean_output_dir
    t3_no_file >> dummy_t3_no_file