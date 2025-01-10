from airflow.models import DAG
from datetime import datetime , timedelta
from airflow.providers.amazon.aws.hooks.s3 import Hook
from airflow.operators.python import PythonOperator



aws_connection_id = 'aws_connection'
bucket_name = 'aiwins'

def uploadtos3():
    hook = Hook(aws_conn_id=aws_connection_id)

defualt_args = {
    "owner":"aiwinmanuel",
    "start_date":datetime(25,1,1),
    "retires":1,
    "retry_delay" : timedelta(minutes=1),
    
}

with DAG(
    "upload_to_s3",
    default_args=defualt_args,
    tags = "S3 upload company employee details",
    catchup= False,
    description="uploading company employee details to s3 bucket",
    schedule_interval=None,
    ) as dag:
    
    uploadtos3 = PythonOperator(
        task_id = "uploadtos3",
        python_callable=uploadtos3
    )
    
    