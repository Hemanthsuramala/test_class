import logging
import os
import boto3
from pytz import timezone
import datetime
from datetime import date, timedelta
from datetime import datetime
from pytz import timezone
import pytz

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from be_api_utils import start_date,end_date
from sail_thru_process.BE_Purchase_Api import purchasing_api,be_purchase_truncate,be_purchase_copy_command,\
                            be_purchase_insert_datalake,be_purchase_int_del_sailthru,\
                            be_purchase_int_ins_sailthru

date_format='%Y%m%d'
current_date = datetime.now(tz=pytz.utc)
enddate = current_date.astimezone(timezone('US/Pacific'))
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


def sailthruclient_extraction():
    print(f'Sailthru Extraction for Purchase utc date is {current_date} coonverted to pst date {pst_start_date},end_date is {pst_end_date}')
    purchasing_api(startdate=pst_start_date,enddate=pst_end_date)
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

with DAG('BE_Purchase_Sailthru_Dag', default_args=default_args, schedule_interval='*/15 * * * *',catchup=False) as dag:

    BE_Sailthru_Extract_Query = PythonOperator(
            task_id='BE_Sailthru_Extract_Query',
            python_callable=sailthruclient_extraction,
            op_kwargs={
                'ts_nodash': '{{ ts_nodash }}'
            }
    )

    BE_Sailthru_Truncate_query = PythonOperator(
            task_id='BE_Sailthru_Truncate_query',
            python_callable=truncate_landing,
            provide_context =   True
    )

    BE_Sailthru_Copy_Command = PythonOperator(
            task_id='BE_Sailthru_Copy_Command',
            python_callable=copy_command,
            provide_context =   True
    )


    BE_Sailthru_Archive = PythonOperator(
            task_id='BE_Sailthru_Archive',
            python_callable=Archive
    )

    BE_Sailthru_Integration_Delete = PythonOperator(
            task_id='BE_Sailthru_Integration_Delete',
            python_callable=integration_delete
    )

    BE_Sailthru_Integration = PythonOperator(
            task_id='BE_Sailthru_Integration',
            python_callable=integration_insert,
            op_kwargs={
                'ts_nodash': '{{ ts_nodash }}'
            }
    )


    BE_Sailthru_Extract_Query>>BE_Sailthru_Truncate_query>>BE_Sailthru_Copy_Command>>BE_Sailthru_Archive>>BE_Sailthru_Integration_Delete>>BE_Sailthru_Integration
