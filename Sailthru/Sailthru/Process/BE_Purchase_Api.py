from cmath import inf
import logging
from os import path, truncate
import time as t
import yaml
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_response import SailthruResponseError
from sailthru.sailthru_error import SailthruClientError
from be_api_utils import start_date,end_date,etl_process_audit,etl_process_id,\
                        etl_process_update,Purchase_position,\
                        start_date_inc,end_date_inc,purchace_fechting_url,\
                        generate_csv,insert_audit_api,update_audit_api,\
                        file_date,sf_execute_query,s3_file_name,error_to_audit_api


# CONFIG_FILE_PATH = "D:/BE/BE_API/be_api.yaml"
CONFIG_FILE_PATH="/usr/local/airflow/dags/be_api.yaml"

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

db_query=get_config('query')
truncate_table=db_query['truncate_landing']
copy_command=db_query['copy_command']     #pass process_id ,filedate
insert_datalake=db_query['Archive_ins']     
integration_del_sailthru=db_query['integration_del_sailthru']     
integration_ins_sailthru=db_query['integration_ins_sailthru']    
filedate=file_date()


def sailthruclient(api_key=apikey,api_secret=apisecret):
    client = SailthruClient(api_key, api_secret)
    return client

def purchasing_api(startdate,enddate):
    """
        Generate ETL Process ID
        
    """
    etl_process_audit(workflow='Purchase')
    process_id=etl_process_id(object_name='Purchase')
    insert_audit_api(object_name='Purchase',dag='Extraction',source_type='API',target_type='CSV',process_id=process_id)    
    try:
        sailthru_client=sailthruclient()
        job_id=Purchase_position(purchase_log='export_purchase_log',
                                sailthru_client=sailthru_client,
                                start_date=startdate,
                                end_date=enddate)
        print(Purchase_position)
        print(job_id)
        t.sleep(40)
        url=purchace_fechting_url(sailthru_client=sailthru_client,generated_job_id=job_id)
        file_name=f'Sailthru_Purchase_{filedate}.csv'
        aws_path=f's3://be-edw/Sailthru/{file_name}'
        generate_csv(url=url,path=aws_path)
        update_audit_api(object_name='Purchase',url=url,filename=file_name,dag='Extraction')
    except Exception as e:
        logging.info(e)
        logging.info('Falied at extraction data from API')
        error_to_audit_api(object='Purchase',task='Extraction',PROCESS_ID=process_id)
    

def be_purchase_truncate():  
    try:
        sf_execute_query(query=truncate_table)
        logging.info('Truncating Table is completed') 
    except Exception as e:
        logging.info(e)
        logging.info('Falied at Truncating Table')
    
def be_purchase_copy_command():  
    process_id=etl_process_id(object_name='Purchase')
    filename=s3_file_name()
    insert_audit_api(object_name='Purchase',dag='Copy Command',source_type='S3',target_type='Snowflake',process_id=process_id)    
    try:
        sf_execute_query(query=copy_command.format(filename=filename,process_id=process_id)) 
        logging.info('Copy command is completed ') 
        update_audit_api(object_name='Purchase',url='',filename=filename,dag='Copy Command')    
    except Exception as e:
        logging.info(e)
        logging.info('Failed to insert into Copy_Command')
        error_to_audit_api(object='Purchase',task="Copy_Command",PROCESS_ID=process_id)
        
def be_purchase_insert_datalake():  
    process_id=etl_process_id(object_name='Purchase')
    insert_audit_api(object_name='Purchase',dag='Archive',source_type='SnowFlake',target_type='SnowFlake',process_id=process_id)    
    try:
        sf_execute_query(query=insert_datalake) 
        logging.info('Inserting into Archive is completed ')      
        update_audit_api(object_name='Purchase',url='',filename=' ',dag='Archive')  
    except Exception as e:
        logging.info(e)
        logging.info('Failed to insert into Archive')
        error_to_audit_api(object='Purchase',task="Archive",PROCESS_ID=process_id)
    
def be_purchase_int_del_sailthru():  
    sf_execute_query(query=integration_del_sailthru)   

    
def be_purchase_int_ins_sailthru(): 
    process_id=etl_process_id(object_name='Purchase') 
    insert_audit_api(object_name='Purchase',dag='Integration',source_type='SnowFlake',target_type='SnowFlake',process_id=process_id)    
    integration_query=integration_ins_sailthru.format(PROCESS_ID=process_id)
    print(integration_query)
    logging.info(integration_query)
    try:
        sf_execute_query(query=integration_query)       
        logging.info('Inserting into Integration is completed ')      
        update_audit_api(object_name='Purchase',url='',filename=' ',dag='Integration')   
    except Exception as e:
        logging.info(e)
        logging.info('Failed to insert into Intergration')
        error_to_audit_api(object='Purchase',task="Integration",PROCESS_ID=process_id)