from airflow.models import DAG
from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import io
import pandas as pd

S3_BUCKET = 'aiwins'
S3_KEY = 'Hotel Reservations.csv'  
LOCAL_FILE = '/temp/Hotel_Reservations.csv'
TRANSFORMED_FILE = '/temp/transformed-file.csv'  

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetchfile(ti):
    
    
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_obj = s3_hook.get_key(S3_KEY, bucket_name=S3_BUCKET)
    file_content = s3_obj.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(file_content))
    print(f"Original DataFrame:\n{df.head(10)}")
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    ti.xcom_push(key='data', value=csv_buffer.getvalue())

    
    return df

def transform(ti):
    
    # Pulling data from the Xcom object
    data = ti.xcom_pull(task_ids='gettingfile', key='data')
    df = pd.read_csv(io.StringIO(data))
    print(f"Original DataFrame:\n{df.head(10)}")
    
    # Fill column-level null values with column median
    df.fillna(df.median(numeric_only=True), inplace=True)
    print("DataFrame after replacing null values with column medians:")
    print(df)
    
    # Remove Duplicates from the DataFrame
    duplicates = df.duplicated()
    print("Number of duplicate rows:", duplicates.sum())
    df.drop_duplicates(inplace=True)
    csv_buffer2 = io.StringIO()
    df.to_csv(csv_buffer2, index=False)
    ti.xcom_push(key='data2', value=csv_buffer2.getvalue())
    
    
    
def saving_to_s3(ti):
    
    data = ti.xcom_pull(task_ids="transformations",key="data2")
    
    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_hook.load_string(
        string_data=data,
        key='transformed-file.csv',  
        bucket_name=S3_BUCKET,
        replace=True  
    )

with DAG(
    dag_id='Hotel_bookings',
    default_args=default_args,
    description='Hotel Bookings',
    catchup=False,
    schedule_interval= "@once",
    start_date=datetime(2024,1,1),
    tags=['Hotel_Bookings'],
) as dag:
    
    getting_file_from_s3 = PythonOperator(
        task_id='gettingfile',
        python_callable=fetchfile,
        dag=dag
    )

    transformations = PythonOperator(
        task_id='transformations',
        python_callable=transform,
        provide_context=True,
        dag=dag
    )
    
    saving_to_s3 = PythonOperator(
        task_id='saving_to_s3',
        python_callable=saving_to_s3,
        dag=dag)
    
    getting_file_from_s3 >> transformations >> saving_to_s3

    
    