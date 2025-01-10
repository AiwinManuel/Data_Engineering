from airflow.models import DAG
from datetime import datetime , timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import os 



aws_connection_id = 'aws_connection'
bucket_name = 'aiwins'
local_directory = "/Users/aiwin/Documents/Projects/aws_redshift/Data_Generation/Company_Employee_Details"

def uploadtos3():
    hook = S3Hook(aws_conn_id=aws_connection_id)
    for file_name in os.listdir(local_directory):
        file_path = os.path.join(local_directory, file_name)

        if os.path.isfile(file_path): 
            with open(file_path, "rb") as file_obj:
                hook._upload_file_obj(  
                    file_obj=file_obj,
                    key=f"Company_Employee_Details/{file_name}",
                    bucket_name=bucket_name,
                    replace=True  
                )
            print(f"Uploaded {file_name} to S3")

    

defualt_args = {
    "owner":"aiwinmanuel",
    "start_date":datetime(2025, 1, 1),
    "retries":1,
    "retry_delay" : timedelta(minutes=1),
    
}

with DAG(
    "upload_to_s3",
    default_args=defualt_args,
    tags = ["S3 upload company employee details"],
    catchup= False,
    description="uploading company employee details to s3 bucket",
    schedule_interval=None,
    ) as dag:
    
    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=uploadtos3
    )
    
    upload_to_s3_task
    
    