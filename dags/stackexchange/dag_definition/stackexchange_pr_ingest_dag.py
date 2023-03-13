from airflow import models
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from pendulum import datetime

import pandas as pd
from io import StringIO
from google.cloud import storage
import py7zr
import requests
import os

def get_list_of_7z_files(source_url:str) -> list:
    response = requests.get(f"{source_url}stackexchange_files.xml")
    if response.status_code == 200:
        data_string = response.text
        df = pd.read_xml(data_string)
        df =  df[df['name'].str.endswith('7z')]
        file_list = df.name.values.tolist()
        return file_list
    else:
        print(f"Cannot list files, status code {response.status_code}")


def get_files(bucket_name, source_url, bucket_path):
    list_of_files_to_get =  get_list_of_7z_files(source_url)
    if list_of_files_to_get is not None:
        for file_name in list_of_files_to_get:
            file_response = requests.get(f"{source_url}{file_name}", stream=True)
            if file_response.status_code == 200:
                output_temp_path = f"templates/files/{file_name}/"
                with open(file_name, 'wb') as out:
                    out.write(file_response.content)
                with py7zr.SevenZipFile(file_name, 'r') as archive:
                    archive.extractall(output_temp_path)
                move_files_to_bucket(bucket_name, output_temp_path, bucket_path)
            else:
                print(f'Failed to get data {file_response.status_code}')
    else:
        print("List of files to get is empty")
    



def move_files_to_bucket(bucket_name, file_location, output_path):
    gcs = storage.Client()
    bucket = gcs.bucket(bucket_name)
    for file_name in os.listdir(file_location):
        full_file_path = f"{file_location}{file_name}"
        output_file = f"{file_location}{file_name.split('.')[0]}_output.parquet"
        data_df = pd.read_xml(full_file_path)
        data_df.to_parquet(output_file)
        blob = bucket.blob(f"{output_path}{output_file}")
        blob.upload_from_filename(output_file)




default_args = {
    "start_date": datetime(2015, 12, 1, tz="UTC"),
    "depends_on_past": False,
    "retries": 1
}

dag = models.DAG(
    default_args=default_args,
    dag_id = "stachexchage_ingest"
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)


parse_files_and_save_to_bucket = PythonOperator(
    task_id = "get_files",
    provide_context = True,
    python_callable=get_files,
    op_kwargs={
        'bucket': 'de-practice-run-bucket',
        'source_url':'https://archive.org/download/stackexchange/',
        'bucket_path': 'ingest/'
    },
    dag=dag

)


start >> parse_files_and_save_to_bucket >> end