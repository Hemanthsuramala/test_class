from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from pytz import timezone
import pytz
from datetime import datetime,timedelta
import logging
import os
import boto3

from sail_thru_process.BE_Sailthru_Extraction import extraction_api,be_api_integration,truncate

object_name='Active_Engaged_Users'

date_format='%Y%m%d'                                                                          #date_format ex: yyyymmdd
current_date = datetime.now(tz=pytz.utc)                                                      #time by zone
enddate = current_date.astimezone(timezone('US/Pacific'))                                     #conversion utc to pst
pst_start_date=enddate.strftime(date_format)
pst_end_date=enddate.strftime(date_format)


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

def sail_thru_extraction():
    print('current_date:',current_date)
    print('enddate:',enddate)
    print('pst_start_date:',pst_start_date)
    logging.info('current_date:',current_date)
    logging.info('enddate:',enddate)
    logging.info('pst_start_date:',pst_start_date)
    extraction_api(object_name=object_name,startdate=pst_start_date)
    logging.info('Sail_Thru Extarction Data')
    
def sail_thru_truncate():
    truncate(object_name=object_name)
    logging.info('Truncating table is completed:', object_name)
    
def sail_thru_copy_command():
    be_api_integration(object_name=object_name,startdate=pst_start_date,task='Copy_Command')
    logging.info('Sail_Thru Landning schema')
    
def sail_thru_Archive():
    be_api_integration(object_name=object_name,startdate=pst_start_date,task='Archive')
    logging.info('Sail_Thru Archive schema')
    
def sail_thru_integrtion():
    be_api_integration(object_name=object_name,startdate=pst_start_date,task='Integration')
    logging.info('Sail_Thru Integration schema')
    

with DAG('BE_Active_Engaged_Users_Dag', default_args=default_args, schedule_interval='30 18 * * *',catchup=False) as dag:

    BE_Sail_Thru_Extraction=PythonOperator(task_id='BE_Sail_Thru_Extraction',
                                           python_callable=sail_thru_extraction)
        
    BE_Sail_Thru_Trucate_Table=PythonOperator(task_id='BE_Sail_Thru_Trucate_Table',
                                           python_callable=sail_thru_truncate)    
        
    BE_Sail_Thru_Landing=PythonOperator(task_id='BE_Sail_Thru_Landing',
                                           python_callable=sail_thru_copy_command)    
            
    BE_Sail_Thru_Archive=PythonOperator(task_id='BE_Sail_Thru_Archive',
                                           python_callable=sail_thru_Archive)
    
    BE_Sail_Thru_Integration=PythonOperator(task_id='BE_Sail_Thru_Integration',
                                           python_callable=sail_thru_integrtion)
    
    BE_Sail_Thru_Extraction>>BE_Sail_Thru_Trucate_Table>>BE_Sail_Thru_Landing>>BE_Sail_Thru_Archive>>BE_Sail_Thru_Integration