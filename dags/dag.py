from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from utils.dim_model_functions import DimModelFunctions
import duckdb


SCHEMA = 'snow'

# Default args
default_args = {
    'owner':'vkampah',
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}

# Define functions to perform etl task
def create_database_connection():
    return duckdb.connect('work_station_db')

def create_schema():
    con = create_database_connection()
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    
def extract_create_table():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.create_raw_comp_data_table()
    
def explore_raw_table():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.explore_raw_comp_data()
    
def fetch_columns():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.describe_raw_comp_data()
    
def clean_data():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.transform_raw_comp_data()

def create_snow_model():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.create_compustatistics_table(SCHEMA)
    newdb.create_ceo_table(SCHEMA)
    newdb.create_locations_table(SCHEMA)
    
def create_snow_model_two():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.create_companies_table(SCHEMA)
    newdb.create_product_table(SCHEMA)
    newdb.create_integrations_table(SCHEMA)
    newdb.create_years_table(SCHEMA)
    newdb.create_workstation_sales_table(SCHEMA)
    
def load_snow_model():
    con = create_database_connection()
    newdb = DimModelFunctions(con)
    newdb.load_data_compustatistics(SCHEMA)
    newdb.load_ceos_table(SCHEMA)
    newdb.load_locations_table(SCHEMA)
    newdb.load_companies_table(SCHEMA)
    newdb.load_product_table(SCHEMA)
    newdb.load_integrations_table(SCHEMA)
    newdb.load_years_table(SCHEMA)
    newdb.load_workstation_sales_data(SCHEMA)

#instantiate a DAG
with DAG(
    dag_id= 'ETL_snow_pipleline',
    description= 'Building a snowflake data model',
    start_date= datetime(2023,7,20,2),
    schedule_interval= '@daily',
    default_args = default_args
) as dag:
    #instantiate an operator to define a task
    task_create_database=PythonOperator (
        task_id='create database',
        python_callable=create_database_connection,
        provide_context=False

    )
    task_create_schema=PythonOperator (
        task_id='create schema',
        python_callable=create_schema,
        provide_context=False

        
    )
    task_load_cvs_to_table = PythonOperator (
        task_id = 'load csv data into duckdb table',
        python_callable = extract_create_table,
        provide_context=False

    )
    task_explore_raw_data=PythonOperator (
        task_id='explore raw data',
        python_callable=explore_raw_table,
        provide_context=False

    )
    task_explore_columns=PythonOperator (
        task_id='fetch columns',
        python_callable=fetch_columns,
        provide_context=False

    )
    task_clean_data = PythonOperator (
        task_id = 'clean data',
        python_callable = clean_data,
        provide_context=False

    )
    task_create_pri_tables=PythonOperator (
        task_id='create primary tables',
        python_callable=create_snow_model,
        provide_context=False

    )
    task_create_sec_tables=PythonOperator (
        task_id='create secondary tables',
        python_callable=create_snow_model_two,
        provide_context=False

    )
    task_load_to_tables = PythonOperator (
        task_id = 'load data into schema tables',
        python_callable = load_snow_model,
        provide_context=False

    )
   
   
# Defining task dependendencies

task_create_database >> task_create_schema >> task_load_cvs_to_table
task_load_cvs_to_table >> [task_explore_raw_data, task_explore_columns] >> task_clean_data
task_clean_data >> task_create_pri_tables >> task_create_sec_tables >> task_load_to_tables 