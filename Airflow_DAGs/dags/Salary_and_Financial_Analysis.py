from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator




def extract_data():
    pass

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
    