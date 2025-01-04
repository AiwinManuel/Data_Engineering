from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import io
from google.cloud import bigquery
import pandas as pd




s3_bucket_name = 'aiwins'
employees_table_key = 'Company_Employee_Details/employees.csv'
departments_table_key = 'Company_Employee_Details/departments.csv'
bigquery_project_id = 'gcpairflow'
bigquery_dataset_id = 'Employee_Demographics_and_Analysis'
bigquery_table_id = 'employees_final'



def data_extractions(**kwargs):
        #Fetching Employees Table
        hook = S3Hook(aws_conn_id='s3_connection')
        employeeTable = hook.read_key(employees_table_key,bucket_name=s3_bucket_name)
        df_employees = pd.read_csv(io.StringIO(employeeTable))
        
        #Fetching Departments Table
        departmentsTable = hook.read_key(departments_table_key,bucket_name=s3_bucket_name)
        df_departments = pd.read_csv(io.StringIO(departmentsTable))
        
        kwargs['ti'].xcom_push(key="employees", value=df_employees.to_dict(orient='records'))
        kwargs['ti'].xcom_push(key="departments", value=df_departments.to_dict(orient='records'))
        
def standarize_gender_values(**kwargs):
    #Standardizing Gender Values
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="extract_data", key="employees"))
    df_employees['Gender'] = df_employees['Gender'].str.capitalize().replace({'M': 'Male', 'F': 'Female'})
    ti.xcom_push(key="employees_standardized", value=df_employees.to_dict(orient='records'))

def calculate_age(**kwargs):
    #Calculating Age
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="standardize_gender", key="employees_standardized"))
    current_year = datetime.now().year
    df_employees['Age'] = current_year - pd.to_datetime(df_employees['DateOfBirth']).dt.year
    ti.xcom_push(key="employees_with_age", value=df_employees.to_dict(orient='records'))


def categorize_age_group(**kwargs):
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="calculate_age", key="employees_with_age"))
    bins = [0, 30, 40, 50, 60, 100]
    labels = ['20-30', '31-40', '41-50', '51-60', '60+']
    df_employees['AgeGroup'] = pd.cut(df_employees['Age'], bins=bins, labels=labels, right=False)
    ti.xcom_push(key="employees_age_grouped", value=df_employees.to_dict(orient='records'))

def calculate_tenure(**kwargs):
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="categorize_age_group", key="employees_age_grouped"))
    current_year = datetime.now().year
    df_employees['Tenure'] = current_year - pd.to_datetime(df_employees['HireDate']).dt.year
    ti.xcom_push(key="employees_with_tenure", value=df_employees.to_dict(orient='records'))



def categorize_tenure_group(**kwargs):
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="calculate_tenure", key="employees_with_tenure"))
    tenure_bins = [0, 2, 5, 10, 100]
    tenure_labels = ['0-2 years', '3-5 years', '6-10 years', '10+ years']
    df_employees['TenureGroup'] = pd.cut(df_employees['Tenure'], bins=tenure_bins, labels=tenure_labels, right=False)
    ti.xcom_push(key="employees_tenure_grouped", value=df_employees.to_dict(orient='records'))


def count_employees_per_department(**kwargs):
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="extract_data", key="employees"))
    department_size = df_employees.groupby('DepartmentID')['EmployeeID'].count().reset_index()
    department_size.rename(columns={'EmployeeID': 'EmployeeCount'}, inplace=True)
    ti.xcom_push(key="department_size", value=department_size.to_dict(orient='records'))

def merge_department_names(**kwargs):
    ti = kwargs['ti']
    df_employees = pd.DataFrame(ti.xcom_pull(task_ids="categorize_tenure_group", key="employees_tenure_grouped"))
    df_departments = pd.DataFrame(ti.xcom_pull(task_ids="extract_data", key="departments"))
    df_departments.rename(columns={"ManagerID": "DepartmentManagerID"}, inplace=True)

    df_employees = df_employees.merge(
        df_departments[['DepartmentID', 'DepartmentName', 'DepartmentManagerID', 'Budget', 'Location', 'EstablishedDate']], 
        on='DepartmentID', how='left'
    )
    
    
    df_employees.fillna("", inplace=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df_employees)
    ti.xcom_push(key="employees_final", value=df_employees.to_dict(orient='records'))
    
def create_bigquery_table():
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_connection", use_legacy_sql=False)
    
    client = bq_hook.get_client()
    
    dataset_ref = client.dataset(bigquery_dataset_id,project=bigquery_project_id)
    table_ref = dataset_ref.table(bigquery_table_id)

    schema = [
    bigquery.SchemaField("EmployeeID", "STRING"),
    bigquery.SchemaField("FirstName", "STRING"),
    bigquery.SchemaField("LastName", "STRING"),
    bigquery.SchemaField("Gender", "STRING"),
    bigquery.SchemaField("Age", "INTEGER"),
    bigquery.SchemaField("AgeGroup", "STRING"),
    bigquery.SchemaField("DateOfBirth", "DATE"),
    bigquery.SchemaField("HireDate", "DATE"),
    bigquery.SchemaField("Tenure", "INTEGER"),
    bigquery.SchemaField("TenureGroup", "STRING"),
    bigquery.SchemaField("DepartmentID", "INTEGER"),
    bigquery.SchemaField("DepartmentName", "STRING"),
    bigquery.SchemaField("DepartmentManagerID", "STRING"),  
    bigquery.SchemaField("Budget", "INTEGER"),
    bigquery.SchemaField("Location", "STRING"),
    bigquery.SchemaField("EstablishedDate", "DATE"),
    bigquery.SchemaField("PositionID", "STRING"), 
    bigquery.SchemaField("Salary", "FLOAT"),
    bigquery.SchemaField("PerformanceScore", "STRING"),
    bigquery.SchemaField("Email", "STRING"),
    bigquery.SchemaField("PhoneNumber", "STRING"),
    bigquery.SchemaField("EmploymentType", "STRING")
]

    table_ref = dataset_ref.table("employees_final")
    
    try:
        client.get_table(table_ref)  
        print("Table already exists.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print("Table created successfully.")
    
def upload_to_bigquery(**kwargs):
    ti = kwargs['ti']
    df_final = pd.DataFrame(ti.xcom_pull(task_ids="merge_department_names", key="employees_final")) 
    
    expected_columns = [
        "EmployeeID", "FirstName", "LastName", "Gender", "Age", "AgeGroup",
        "DateOfBirth", "HireDate", "Tenure", "TenureGroup", "DepartmentID",
        "DepartmentName", "DepartmentManagerID", "Budget", "Location", "EstablishedDate",
        "PositionID", "Salary", "PerformanceScore", "Email", "PhoneNumber", "EmploymentType"
    ] 
    df_final = df_final[expected_columns]
    df_final["Salary"] = pd.to_numeric(df_final["Salary"], errors="coerce")
    bq_hook = BigQueryHook(gcp_conn_id="google_cloud_connection", use_legacy_sql=False)
    client = bq_hook.get_client()
    table_ref = client.dataset(bigquery_dataset_id, project=bigquery_project_id).table(bigquery_table_id)
    rows_to_insert = df_final.to_dict(orient="records")

    errors = client.insert_rows_json(table_ref, rows_to_insert)  
    if errors:
        print(f"BigQuery insert errors: {errors}")
    else:
        print("Data successfully inserted into BigQuery.")


    

default_args = {
    'owner': 'aiwin_manuel',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'catchup': False,

    }


with DAG(
    "Employee_Demographics_and_Analysis",
    default_args=default_args,
    description="DAG to analyze employee demographics",
    schedule_interval=None,
    tags=['employee_analysis'],
    ) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=data_extractions,
        
    )

    standardize_gender_task = PythonOperator(
        task_id="standardize_gender",
        python_callable=standarize_gender_values,
        
    )

    calculate_age_task = PythonOperator(
        task_id="calculate_age",
        python_callable=calculate_age,
        
    )

    categorize_age_group_task = PythonOperator(
        task_id="categorize_age_group",
        python_callable=categorize_age_group,
        
    )

    calculate_tenure_task = PythonOperator(
        task_id="calculate_tenure",
        python_callable=calculate_tenure,
        
    )

    categorize_tenure_task = PythonOperator(
        task_id="categorize_tenure_group",
        python_callable=categorize_tenure_group,
        
    )

    count_department_task = PythonOperator(
        task_id="count_employees_per_department",
        python_callable=count_employees_per_department,
        
    )

    merge_department_task = PythonOperator(
        task_id="merge_department_names",
        python_callable=merge_department_names,
        
    )
    
    create_table_task = PythonOperator(
    task_id="create_bigquery_table",
    python_callable=create_bigquery_table,
)
    
    upload_to_bigquery_task = PythonOperator(
        task_id="upload_to_bigquery",
        python_callable=upload_to_bigquery,)
    
    
extract_task >> standardize_gender_task >> calculate_age_task >> categorize_age_group_task >> calculate_tenure_task >> categorize_tenure_task >> count_department_task >> merge_department_task >> create_table_task  >> upload_to_bigquery_task
        