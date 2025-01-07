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



exchange_rates = {
    'India': 0.012,      # 1 INR ≈ 0.012 USD
    'USA': 1.0,          # 1 USD = 1 USD
    'Australia': 0.65,   # 1 AUD ≈ 0.65 USD
    'Canada': 0.75       # 1 CAD ≈ 0.75 USD
}


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
    

def currrency_conversion(**kwargs):
    
    ti = kwargs['ti']
    df_salary_history = pd.DataFrame(ti.xcom_pull(task_ids='extracting_data',key='salary_history'))
    df_employee = pd.DataFrame(ti.xcom_pull(task_ids='extracting_data',key='employee'))
    
    df_employee['ExchangeRate'] = df_employee['Location'].map(exchange_rates)
    df_employee['SalaryUSD'] = df_employee['SalaryUSD'] = df_employee['Salary'] * df_employee['ExchangeRate']
    
    df_salary_history['ExchangeRate'] = df_salary_history['Location'].map(exchange_rates)
    df_salary_history['PreviousSalaryUSD'] = df_salary_history['PreviousSalary'] * df_salary_history['ExchangeRate']  
    df_salary_history['UpdatedSalaryUSD'] = df_salary_history['UpdatedSalary'] * df_salary_history['ExchangeRate']

    ti.xcom_push(key='salary_history', value = df_salary_history.to_dict(orient="record"))
    ti.xcom_push(key='employee', value = df_employee.to_dict(orient="record"))

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
    
    coverting_currency_task = PythonOperator(
        task_id = "currency_converstion",
        python_callable=currrency_conversion
    )
    
    
    
    extracting_task >> coverting_currency_task
    