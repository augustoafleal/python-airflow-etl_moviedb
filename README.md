# ETL MOVIEDB

## Description

In this project, an ETL process is performed on data corresponding to the collection of data from the top 100 movies in the [The Movie DB](https://www.themoviedb.org) ranking. This data is stored in an AWS S3 bucket and processed to generate a raw .csv file, which is then persisted in a PostgreSQL database.

The technologies used in the project include:
- Python
- Airflow
- Docker
- LocalStack
- Pandas
- PostgreSQL

## Project Structure

```md
- airflow_docker
  - config
  - dags
    - etl_moviedb.py
    - moviedb
      - __init__.py
      - create_raw_file.py
      - get_moviedb_json.py
      - insert_data_psql.py
      - upload_file_s3.py
      - data
        - process
          - moviedb_20231005_231741.json
        - output
          - moviedb_20231005_231741.csv
  - logs
  - plugins
  - docker-compose.yaml
- DB
  - moviedb.sql
  - movies_rank.sql
```

## Project Steps

Below are the steps of the ETL orchestrated by Airflow using the DAG [etl_moviedb.py](https://github.com/augustoafleal/python-airflow-etl_moviedb/blob/main/airflow_docker/dags/etl_moviedb.py):

| #️⃣ | Task | Task Name | Description |
|---| ---- | ----------- | ----- |
| 1️⃣ | **TASK 1** | t1_create_json | An API request is made to The Movie DB, and a .json file containing the data of the top 100 most voted movies is structured. The movie data is saved as shown in the example file [moviedb_20231005_231741.json](https://github.com/augustoafleal/python-airflow-etl_moviedb/blob/main/airflow_docker/dags/moviedb/data/process/moviedb_20231005_231741.json). |
| - | **BRANCH TASK 1** | branch_t1 | Validates if the file was generated and saved correctly. If saved correctly, it proceeds to TASK 1.2. If not saved, TASK 3 is triggered. |
| 2️⃣ | **TASK 2** | t2_file_created | Confirms the creation of the file by notifying through a Bash operator. |
| 3️⃣ | **TASK 3** | t3_no_file | Indicates the non-generation of the file, also notifying through a Bash operator. |
| - |   dummy_t3_no_file | - | An empty task signaling the end of processing in case no file was created. |
| 4️⃣ | **TASK 4** | t4_upload_json | Uploads the JSON file generated in TASK 1 to an AWS S3 bucket named *moviedb* using the internally configured Airflow connection and the S3Hook. In this case, instead of using AWS services, they are emulated by LocalStack to facilitate development and testing without the need to use AWS resources in production. |
| 5️⃣ | **TASK 5** | t5_create_raw_csv | The file generated in TASK 1 is processed as a DataFrame using the Pandas library to generate a raw CSV file, following the format of the file [moviedb_20231005_231741.csv](https://github.com/augustoafleal/python-airflow-etl_moviedb/blob/main/airflow_docker/dags/moviedb/data/output/moviedb_20231005_231741.csv). In this task, several tasks are performed, such as: <ul><li>Adjustment of data related to the image URLs in the poster and backdrop_prefix columns.</li><li>Selection and ordering of columns to be displayed in the final file, along with a column indicating the ingestion date (created_date).</li></ul> |
| 6️⃣ | **TASK 6** | t6_insert_data_psql | The processed data generated in the .csv file is inserted into a PostgreSQL database using the configured Airflow connection and the PostgresHook. <ul><li>Database creation script [moviedb](https://github.com/augustoafleal/python-airflow-etl_moviedb/blob/main/DB/moviedb.sql).</li><li>Table creation script [movies_rank](https://github.com/augustoafleal/python-airflow-etl_moviedb/blob/main/DB/moviedb.sql), where the data is persisted.</li></ul> |
| 7️⃣ | **TASK 7** | t7_clean_process_dir | Cleans up files located in the /opt/airflow/dags/moviedb/data/process/ directory. |
| 8️⃣ | **TASK 8** | t8_clean_output_dir | Deletes files located in the /opt/airflow/dags/moviedb/data/output/ directory. |

## Observations

- Although the .json and .csv files are deleted at the end of the process, in this project, one of each has been left in their respective folders to demonstrate their generation and structure, in order to facilitate understanding of the described process.

- While it is feasible in certain situations to generate only the .csv file for later consumption, even sending it to a specific recipient, in this project, it was chosen to persist the data in a database located in PostgreSQL as a way to leverage the use of tools for this process.

