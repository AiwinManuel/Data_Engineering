import io
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = 'aiwins'

def fetch_s3_file(s3_key, aws_conn_id='s3_connection'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_obj = s3_hook.get_key(s3_key, bucket_name=S3_BUCKET)
    file_content = s3_obj.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(file_content))
    print(f"Fetched DataFrame from S3:\n{df.head(10)}")
    return df

def upload_to_s3(data, s3_key, aws_conn_id='s3_connection'):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(
        string_data=data,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True
    )
    print(f"Data uploaded successfully to S3 with key: {s3_key}")
