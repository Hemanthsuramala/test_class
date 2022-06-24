
from imp import source_from_cache
import logging
from os import path, truncate
import time as t
import yaml
import json
import boto3
from datetime import date, timedelta
from botocore.exceptions import NoCredentialsError
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_response import SailthruResponseError
from sailthru.sailthru_error import SailthruClientError
from be_api_utils import start_date,end_date,etl_process_audit,etl_process_id,\
                        etl_process_update,insert_audit_api,update_audit_api,\
                        file_date,sf_execute_query,upload_file_to_S3,start_date,\
                        start_date_inc,end_date_inc,get_db_config,sf_execute_count,\
                        error_to_audit_api


                      
# CONFIG_FILE_PATH = "D:/BE/BE_API/be_api.yaml"
CONFIG_FILE_PATH="/usr/local/airflow/dags/be_api.yaml"
s3 = boto3.client('s3')
def load_config():
    """
        load the configuration from yaml
    """
    with open(CONFIG_FILE_PATH) as f:
        return yaml.safe_load(f.read())['info']

def get_config(source):
    """
        get the config as per the input source 
    """
    data= load_config()    
    return data[source]

constant=get_config('constant')
apikey=constant['apikey']
apisecret=constant['apisecret'] 


def sailthruclient(api_key=apikey,api_secret=apisecret):
    client = SailthruClient(api_key, api_secret)
    return client


def extraction_api(object_name,startdate):
    
    """
        Generate ETL Process ID
        
    """
    etl_process_audit(workflow=object_name)
    process_id=etl_process_id(object_name=object_name)
    insert_audit_api(object_name=object_name,
                     dag='Extraction',
                     source_type='API',
                     target_type='JSON',
                     process_id=process_id)    
    sailthru_client=sailthruclient()
    if'Active_Engaged_Users'== object_name:   #Active_Engaged_Users
        job_id ={
                "stat":"list",
                "list":"Active Engaged Users - 30 Days Engaged", 
                "date":startdate
            }
    elif 'All_Subcribers'==object_name:   #All_Subcribers
        job_id={"stat":"list",
                "list":"ALL subscribers: excluding opt-outs",
                "date":startdate
                }
    else:
        print('No Object defined')
        logging.info('No Object defined')
        return
    try:
        job_id_response = sailthru_client.api_get("stats", job_id)
        job_id_body = job_id_response.get_body()
        file_date=startdate
        file_name=f'{object_name}_{file_date}'
        with open(file_name,'w') as file:
                json.dump(job_id_body, file,indent=4) 
        #file_name,bucket_name,folder/object_name
        upload_file_to_S3(file_name=file_name,bucket='be-edw',object_name=f'Sail_Thru/{object_name}/{object_name}_{file_date}.json')
    except Exception as e:
        print(e)
        logging.info(e)
        error_to_audit_api(object=object_name,task='Extraction',PROCESS_ID=process_id)
        return
    update_audit_api(object_name=object_name,url="Null",
                      filename=file_name,dag='Extraction')

def truncate(object_name):
    if 'Active_Engaged_Users'==object_name:
        query="""TRUNCATE TABLE LANDING.SAILTHRU_ACTIVE_ENGAGED_USERS_30_DAYS_ENGAGED"""
    elif 'All_Subcribers'==object_name:
        query="""TRUNCATE TABLE LANDING.SAILTHRU_ALL_SUBSCRIBERS_EXCLUDING_OPT_OUTS"""
    else:
        print('No Object is found for truncating table')
        logging.info('No Object is found for truncating table')
        return
    try:
        logging.info(query)
        sf_execute_query(query=query)
        logging.info(f'Truncating table is completed for {object_name}')
    except Exception as e:
        logging.info(f'Failed to Truncating table  for {object_name}')
        return
    
    
def be_api_integration(object_name,startdate,task):
    process_id=etl_process_id(object_name=object_name)
    logging.info(object_name,process_id)
    if 'Copy_Command'==task:
        db_query=get_db_config(objectname=f'{object_name}',source='query')
        copy_commad_query=db_query[task]
        query=copy_commad_query.format(filedate=startdate,PROCESS_ID=process_id)
        logging.info('Copy_Command :',copy_commad_query)
        source_type='S3'
        target_type="SnowFlake"
    elif 'Archive'==task:
        db_query=get_db_config(objectname=f'{object_name}',source='query')
        query=db_query[task]
        source_type='SnowFlake'
        target_type='SnowFlake'
    elif 'Integration'==task:
        db_query=get_db_config(objectname=f'{object_name}',source='query')
        query=db_query[task]
        source_type='SnowFlake'
        target_type='SnowFlake'
    insert_audit_api(object_name=object_name,
                     dag=task,
                     source_type=source_type,
                     target_type=target_type,
                     process_id=process_id)
    try:
        print(query)
        sf_execute_query(query=query)
        # int_insert_count=db_query['int_insert_count']
        # int_update_count=db_query['int_update_count'] 
        # rows_inserted=sf_execute_count(query=int_insert_count)
        # rows_updated=sf_execute_count(query=int_update_count)
        update_audit_api(object_name=object_name,
                         url='',
                         filename='',
                         dag=task)
    except Exception as e:
        print(e)
        print('failed')
        logging.info(e)
        error_to_audit_api(object=object_name,
                       task=task,
                       PROCESS_ID=process_id)

    