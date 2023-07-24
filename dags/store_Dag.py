from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datacleaner import data_cleaner

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023,7,22),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('store_dag', default_args = default_args, schedule_interval = '@daily', template_searchpath=['/usr/local/airflow/sql_files'], catchup = False) as dag:

    #t1 = BashOperator(task_id = 'check_file_exists', bash_command = 'shasum ~/store_files_airflow/raw_store_transactions.csv', retries = 2, 
    #retry_delay = timedelta(seconds = 15))

    t1 = FileSensor(
        task_id = 'check_files_exists',
        filepath = '/usr/local/airflow/store_files_airflow/raw_store_transactions.csv',
        fs_conn_id= 'fs_default',
        poke_interval = 10,
        timeout = 150,
        soft_fail = True)

    t2 = PythonOperator(task_id = 'clean_raw_csv', python_callable = data_cleaner)

    t3 = PostgresOperator(task_id = 'create_postgres_table', postgres_conn_id = 'postgres_conn', sql = "create_table.sql")

    t4 = PostgresOperator(task_id = 'insert_into_table', postgres_conn_id = 'postgres_conn', sql = "insert_into_table.sql")

    t1 >> t2 >> t3 >> t4 # For concurrent operations using [] e.g [t5,t6]

