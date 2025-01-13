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
employee_table_key = 'Company_Employee_Details/employees.csv'
departments_table_key = 'Company_Employee_Details/departments.csv'



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
    
    departments_data = hook.read_key(departments_table_key, bucket_name=aws_bucket_name)
    df_departments = pd.read_csv(io.StringIO(departments_data))
    
    kwargs['ti'].xcom_push(key='employee', value=df_employee.to_dict(orient='records'))
    kwargs['ti'].xcom_push(key='performance_reviews',value=df_performance_reviews.to_dict(orient='records'))
    kwargs['ti'].xcom_push(key='salary_history',value=df_salary_history.to_dict(orient='records'))
    kwargs['ti'].xcom_push(key='departments', value = df_departments.to_dict(orient="records"))
    

def currrency_conversion(**kwargs):
    
    ti = kwargs['ti']
    df_salary_history = pd.DataFrame(ti.xcom_pull(task_ids='extracting_data',key='salary_history'))
    df_employee = pd.DataFrame(ti.xcom_pull(task_ids='extracting_data',key='employee'))
    
    df_employee['ExchangeRate'] = df_employee['Location'].map(exchange_rates)
    df_employee['SalaryUSD'] = df_employee['Salary'] * df_employee['ExchangeRate']
    
    df_salary_history['ExchangeRate'] = df_salary_history['Location'].map(exchange_rates)
    df_salary_history['PreviousSalaryUSD'] = df_salary_history['PreviousSalary'] * df_salary_history['ExchangeRate']  
    df_salary_history['UpdatedSalaryUSD'] = df_salary_history['UpdatedSalary'] * df_salary_history['ExchangeRate']

    ti.xcom_push(key='salary_history', value = df_salary_history.to_dict(orient="records"))
    ti.xcom_push(key='employee', value = df_employee.to_dict(orient="records"))
    
def dateFormat(**kwargs):
    ti = kwargs['ti']
    df_salary_history = pd.DataFrame(ti.xcom_pull(task_ids='currency_conversion',key='salary_history'))
    df_employee = pd.DataFrame(ti.xcom_pull(task_ids='currency_conversion',key='employee'))
    print("Columns in df_employee:", df_employee.columns)

    df_employee['HireDate'] = pd.to_datetime(df_employee['HireDate'], format='%Y-%m-%d', errors='coerce')
    df_employee['DateOfBirth'] = pd.to_datetime(df_employee['DateOfBirth'],format='%Y-%m-%d', errors='coerce')
    
    df_salary_history['EffectiveDate'] = pd.to_datetime(df_salary_history['EffectiveDate'],format='%Y-%m-%d', errors='coerce')
    df_employee['HireDate'] = df_employee['HireDate'].dt.strftime('%Y-%m-%d')
    df_employee['DateOfBirth'] = df_employee['DateOfBirth'].dt.strftime('%Y-%m-%d')
    df_salary_history['EffectiveDate'] = df_salary_history['EffectiveDate'].dt.strftime('%Y-%m-%d')
    
    ti.xcom_push(key='employee',value = df_employee.to_dict(orient='records'))
    ti.xcom_push(key='salary_history',value = df_salary_history.to_dict(orient='records'))

def salaryBand(**kwargs):
    ti = kwargs['ti']
    df_salary_history = pd.DataFrame(ti.xcom_pull(task_ids='date_formatting',key='salary_history'))
    
    df_salary_history['SalaryBand'] = pd.cut(df_salary_history['UpdatedSalaryUSD'], bins=[0, 50000, 100000, 150000, 200000, 250000], labels=['Low', 'Medium', 'Above Medium', 'High', 'Very High'])
    
    ti.xcom_push(key='salary_history', value = df_salary_history.to_dict(orient='records'))
    
def average_salary(**kwargs):
    ti=kwargs['ti']
    df_employee = pd.DataFrame(ti.xcom_pull(task_ids = 'date_formatting', key = 'employee'))
    df_salary_history = pd.DataFrame(ti.xcom_pull(task_ids='date_formatting',key='salary_history'))
    df_departments = pd.DataFrame(ti.xcom_pull(task_ids = 'extracting_data', key = 'departments'))
    df_latest_salary = df_salary_history.sort_values(by=['EmployeeID', 'EffectiveDate']).groupby('EmployeeID').last().reset_index()
    df_employee_salary = pd.merge(df_employee,df_latest_salary[['EmployeeID', 'UpdatedSalary']], on='EmployeeID', how='left' )
    df_employee_salary= pd.merge(df_employee_salary,df_departments[['DepartmentID', 'DepartmentName']], on = 'DepartmentID', how='left' )
    df_avg_salary = df_employee_salary.groupby(['DepartmentID', 'DepartmentName', 'PositionID'])['UpdatedSalary'].mean().reset_index()
    df_avg_salary.rename(columns={'UpdatedSalary': 'AvgSalary'}, inplace=True)
    print(df_avg_salary)    
    
    ti.xcom_push(key="average_salary", value = df_avg_salary.to_dict(orient='records'))
    
    
        

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
        task_id = "currency_conversion",
        python_callable=currrency_conversion
    )
    
    date_formatting_task = PythonOperator(
        task_id = "date_formatting",
        python_callable=dateFormat
    )
    
    salary_band_task = PythonOperator(
        task_id = "salary_band",
        python_callable=salaryBand
    )
    
    average_salary_task = PythonOperator(
        task_id = "average_salary",
        python_callable=average_salary
    )
    
    
    
    extracting_task >> coverting_currency_task >> date_formatting_task >> salary_band_task >> average_salary_task 
    