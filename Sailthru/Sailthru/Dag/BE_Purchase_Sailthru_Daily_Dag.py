import logging
import os
import boto3
from datetime import datetime,timedelta
from be_api_utils import start_date,end_date,start_date_inc,\
                         end_date_inc,file_date
from sail_thru_process.BE_Purchase_Api import purchasing_api,be_purchase_truncate,be_purchase_copy_command,\
                            be_purchase_insert_datalake,be_purchase_int_del_sailthru,\
                            be_purchase_int_ins_sailthru
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

        
default_args = {
    "owner": "Brilliant Earth ",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)

}

enddate=end_date_inc(1)
def sailthruclient_extraction():

    purchasing_api(startdate=enddate,enddate=enddate)
    logging.info('sailthru extraction is completed')

def truncate_landing():
    be_purchase_truncate()
    logging.info('sailthru data truncated')
    
def copy_command():
    be_purchase_copy_command()
    logging.info('sailthru copy command is completed')
    
def Archive():
    be_purchase_insert_datalake()
    logging.info('sailthru data lake is completed')
    
def integration_delete():
    be_purchase_int_del_sailthru()
    logging.info('sailthru integration data deleted')
    
def integration_insert():
    be_purchase_int_ins_sailthru()
    logging.info('sailthru integration data inserted')

with DAG('BE_Sailthru_Purchase_Daily_Dag', default_args=default_args, schedule_interval='20 8 * * *',catchup=False) as dag:

    BE_Sailthru_Extract_Query = PythonOperator(
            task_id='BE_Sailthru_Extract_Query',                        
            python_callable=sailthruclient_extraction,            
            
    )
    
    BE_Sailthru_Truncate_query = PythonOperator(
            task_id='BE_Sailthru_Truncate_query',                        
            python_callable=truncate_landing,            
            
    )
    
    BE_Sailthru_Copy_Command = PythonOperator(
            task_id='BE_Sailthru_Copy_Command',                        
            python_callable=copy_command,            
            
    )
    
    
    BE_Sailthru_Archive = PythonOperator(
            task_id='BE_Sailthru_Archive',                        
            python_callable=Archive,            
            
    )
    
    BE_Sailthru_Integration_Delete = PythonOperator(
            task_id='BE_Sailthru_Integration_Delete',                        
            python_callable=integration_delete
    )

    BE_Sailthru_Integration = PythonOperator(
            task_id='BE_Sailthru_Integration',                        
            python_callable=integration_insert,            
            
    )

   
    
    BE_Sailthru_Extract_Query>>BE_Sailthru_Truncate_query>>BE_Sailthru_Copy_Command>>BE_Sailthru_Archive>>BE_Sailthru_Integration_Delete>>BE_Sailthru_Integration
 