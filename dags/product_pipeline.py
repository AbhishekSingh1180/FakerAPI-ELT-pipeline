import sys
import pendulum
import os
from airflow import DAG
from datetime import datetime as dt
sys.path.append('/opt/airflow/packages/')
from fakerAPI import extract_data
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable as var
from airflow.utils.task_group import TaskGroup

product_data_local_path = var.get('product_data_local_path')
product_data_gcs_bucket = var.get('product_data_gcs_bucket')
product_data_gcs_destination_path = f"{dt.utcnow().strftime('%Y/%m/%d')}/"
copy_into_refined_products_stmt = "CALL PRODUCT_DB.DW_APPL.SP_PRODUCT_STAGE_LOAD_TO_REFINED_PRODUCTS();"
refined_to_products_confirmed_stmt = "CALL PRODUCT_DB.DW_APPL.SP_PRODUCT_REFINED_LOAD_TO_CONFIRMED_PRODUCTS();"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.DateTime(2024,2,29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG('product_pipeline', default_args=default_args, schedule=None) as dag:
    
    # dummy start
    start = EmptyOperator(task_id = 'start')

    with TaskGroup(group_id='EXTRACT') as extract:
        # extract API data into local in csv file
        FAKER_API = PythonOperator(
            task_id = 'FAKER_API',
            python_callable=extract_data
        )

    with TaskGroup(group_id='LOAD') as load:
        # upload csv file to gcs bucket / snowflake external stage
        EXTERNAL_STAGE = LocalFilesystemToGCSOperator( 
            task_id = 'EXTERNAL_STAGE',
            gcp_conn_id='google_cloud_default',
            src=product_data_local_path,
            dst=product_data_gcs_destination_path,
            bucket=product_data_gcs_bucket
        )

        # copy csv data from current day path to refined product table
        PRODUCTS_REFINED = SnowflakeOperator(
            task_id='PRODUCTS_REFINED',
            sql=copy_into_refined_products_stmt,
            snowflake_conn_id='snowflake_default_conn',
        )

    with TaskGroup(group_id='TRANSFORM') as transform:    
        # transfer/tranform data from refined product table to confirmed product table
        PRODUCTS_CONFIRMED = SnowflakeOperator(
            task_id='PRODUCTS_CONFIRMED',
            sql=refined_to_products_confirmed_stmt,
            snowflake_conn_id='snowflake_default_conn',
        )

    stop = EmptyOperator(task_id = 'stop')

start >> FAKER_API >> EXTERNAL_STAGE >> PRODUCTS_REFINED >> PRODUCTS_CONFIRMED >> stop

start >> extract >> load >> transform >> stop