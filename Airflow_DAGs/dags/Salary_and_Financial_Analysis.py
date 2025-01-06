from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io




aws_connection_id = "aws_connection"
aws_bucket_name = "aiwins"
salary_history_table_key = "Company_Employee_Details/salary_history.csv"
performance_reviews_table_key = 'Company_Employee_Details/performance_reviews.csv'
employee_table_key = 'Company_Employee_Details/employee.csv'


def extract_data(**kwargs):
    
    hook = S3Hook(aws_conn_id=aws_connection_id)
    salary_history_data =hook.read_key(salary_history_table_key,bucket_name=aws_bucket_name)
    df_salary_history = pd.read_csv(io.StringIO(salary_history_data))
    
    performance_reviews_data = hook.read_key(performance_reviews_table_key, bucket_name=aws_bucket_name)
    df_performance_reviews = pd.read_csv(io.StringIO(performance_reviews_data))
    
    employee_data = hook.read_key(employee_table_key, bucket_name=aws_bucket_name)
    df_employee = pd.read_csv(io.StringIO(employee_data))
    
    kwargs['ti'].xcom_push(key='employee', value=df_employee.to_dict(orient="records"))
    kwargs['ti'].xcom_push(key='performance_reviews',value=df_performance_reviews.to_dict(orient="records"))
    kwargs['ti'].xcom_push(key='salary_history',value=df_salary_history.to_dict(orient="records"))

default_args = {
    "owner":"aiwinmanuel",
    "retries" : 1,
    "start_date" : datetime(2025,1,1),
    "retry_delay" : timedelta(minutes=1),
}

with DAG(
    "Salary_and_Financial_Analysis",
    default_args=default_args,
    schedule_interval=None,
    description="DAG to analyze salary and financial data",
    tags=['salary_analysis'],
    catchup = False,
    ) as dag:
    
    extracting_task = PythonOperator(
        task_id = "extracting_data",
        python_callable = extract_data
    )
    
    
    
    extracting_task
    