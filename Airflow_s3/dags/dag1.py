from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.connections.s3connections import fetch_s3_file, upload_to_s3
from include.transformations.transformations import transform_data
import pandas as pd
import io

S3_KEY = 'Hotel Reservations.csv'
TRANSFORMED_KEY = 'transformed-file.csv'

default_args = {
    'owner': 'aiwin_manuel',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetchfile(ti):
    # Fetch the file from S3 and push the DataFrame as a CSV string to XCom
    df = fetch_s3_file(S3_KEY)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    ti.xcom_push(key='data', value=csv_buffer.getvalue())

def transform(ti):
    # Pull the CSV string from XCom and transform the data
    data = ti.xcom_pull(task_ids='gettingfile', key='data')
    df = pd.read_csv(io.StringIO(data))
    transformed_data = transform_data(df)
    ti.xcom_push(key='transformed_data', value=transformed_data)

def saving_to_s3(ti):
    # Pull the transformed CSV string from XCom and upload it to S3
    transformed_data = ti.xcom_pull(task_ids='transformations', key='transformed_data')
    upload_to_s3(transformed_data, TRANSFORMED_KEY)

with DAG(
    dag_id='Hotel_Bookings',
    default_args=default_args,
    description='Hotel Bookings DAG',
    catchup=False,
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    tags=['Hotel_Bookings'], 
) as dag:

    getting_file_from_s3 = PythonOperator(
        task_id='gettingfile',
        python_callable=fetchfile,
    )

    transformations = PythonOperator(
        task_id='transformations',        
        python_callable=transform,
    )
    
    saving_to_s3 = PythonOperator(
        task_id='saving_to_s3',
        python_callable=saving_to_s3,
    )
    
    getting_file_from_s3 >> transformations >> saving_to_s3
