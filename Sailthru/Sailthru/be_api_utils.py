import datetime
import pandas as pd
import time
import logging
import yaml
import snowflake.connector as sf
import boto3
from datetime import date, timedelta
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_response import SailthruResponseError
from sailthru.sailthru_error import SailthruClientError

# used to fetch historical dates by calculating days 
def start_date_inc():
    current_date = date.today() 
    inc_date=current_date.strftime("%Y%m%d")
    return inc_date

def end_date_inc(day):
    current_date = date.today() - timedelta(days = day)   
    inc_date=current_date.strftime("%Y%m%d")
    return inc_date
#used to current date
def start_date():
    ts = time.time()
    inc_date=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    return inc_date
def end_date():
    ts = time.time()
    end_date=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    return end_date
#used to store filename

def file_date():
    ts = time.time()
    inc_date=datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M')
    return inc_date

# CONFIG_FILE_PATH = "D:/BE/BE_API/be_api.yaml"
CONFIG_FILE_PATH="/usr/local/airflow/dags/be_api.yaml"

def myLogger(logfile="/usr/local/airflow/dags/default_logs.log"):    
    logging.basicConfig(filename=logfile, format='%(asctime)s %(message)s', filemode='a')
    logger=logging.getLogger() 
    logger.setLevel(logging.INFO)
    return logger


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

def load_db_config(objectname):
    """
        load the configuration from yaml
    """
    # with open(f'D:/BE/BE_API/{objectname}.yaml') as f:
    #     # return yaml.safe_load(f.read())['info']
    with open(f'/usr/local/airflow/dags/sailthru_db_query/{objectname}.yaml') as f:
        return yaml.safe_load(f.read())['info']        

def get_db_config(objectname,source):
    """
        get the config as per the input source 
    """
    db_data= load_db_config(objectname) 
    # print(db_data)
    # logging.info(data)    
    return db_data[source]


snowdb=get_config('s_f')
user=snowdb['user']
account=snowdb['account']
database=snowdb['database']
password=snowdb['password']
role=snowdb['role']
warehouse=snowdb['warehouse']
landingschema=snowdb['landingschema']
schema_audit=snowdb['audit']


def get_sf_connect():
    
    snowdb=get_config('s_f')
    conn=sf.connect(
                  
                    user= user ,     
                    password=password,
                    account=account,
                    role=role,
                    warehouse=warehouse,
                    database=database,
                    schema=landingschema
                    )
    return conn


def Purchase_position(purchase_log,sailthru_client,start_date,end_date):
    """
        Job IDs are generated depending on the date, which must and should be fewer than 31 days. 
    """
    try:
        data = {
        "job":purchase_log,
        "start_date":start_date,
        "end_date":end_date
            }
        print(data)
        response = sailthru_client.api_post("job", data)
        if response.is_ok():
            body = response.get_body()
            print(body)
            generating_job_id=body['job_id']  
        else:
            error = response.get_error()
            print(error)
            print ("Error: " + error.get_message())
            print ("Status Code: " + str(response.get_status_code()))
            print ("Error Code: " + str(error.get_error_code()))
    except SailthruClientError as e:
        print ("Exception")
        print (e)
    return generating_job_id
    
    
def purchace_fechting_url(sailthru_client,generated_job_id):
    try:
        job_id ={
                    "job_id": generated_job_id
                }

        job_id_response = sailthru_client.api_get("job", job_id)

        if job_id_response.is_ok():
            print("job_id_response is ok")
            job_id_body = job_id_response.get_body()
            print(job_id_body)
            purchased_url_job_id=job_id_body['export_url'] 
            print(purchased_url_job_id)          
        else:
            error = job_id_response.get_error()
            print ("Error: " + error.get_message())
            print ("Status Code: " + str(job_id_response.get_status_code()))
            print ("Error Code: " + str(error.get_error_code()))
    except SailthruClientError as e:
        # Handle exceptions
        print ("Exception")
        print (e)
    return purchased_url_job_id

def generate_csv(url,path):
    out=pd.read_csv(url)
    out['Date'] = pd.to_datetime(out['Date'])
    out.to_csv(path,index=False)
    
def insert_audit_api(object_name,dag,source_type,target_type,process_id):    
    con=get_sf_connect()
    cursor=con.cursor()
    insert_audit_query=f"""INSERT INTO AUDIT.BE_ELT_AUDIT_API (PACKAGE,OBJECT,DAG,SOURCE_TYPE,TARGET_TYPE,START_TIME,STATUS,PROCESS_ID )
                          VALUES('Sailthru','{object_name}','{dag}','{source_type}','{target_type}',CURRENT_TIMESTAMP,'Running',{process_id})
                        """
    try:
        logging.info(insert_audit_query)
        print(insert_audit_query)
        cursor.execute(insert_audit_query)
        logging.info('Inserted to Audit table succeeded')
    except Exception as e:
        logging.info('faild',insert_audit_query)
        logging.info("Insertion to Audit table is Failed ::\n",insert_audit_query)
        logging.error(e)

def update_audit_api(object_name,url,filename,dag):
    con=get_sf_connect()
    cursor=con.cursor()
    update_audit_query=f"""update AUDIT.BE_ELT_AUDIT_API set STATUS='Completed',END_TIME=CURRENT_TIMESTAMP,url='{url}',FIle_name='{filename}' where DAG='{dag}' and PROCESS_ID in (select MAX(ID) from  "AUDIT"."BE_ELT_PROCESS_API"  where  PLATFORM='Sailthru_Api' AND WORKFLOWNAME='{object_name}')"""
    try:
        logging.info(update_audit_query)
        print(update_audit_query)
        cursor.execute(update_audit_query)
        logging.info('update to Audit table succeeded')
    except Exception as e:
        logging.info('failed',update_audit_query)
        logging.info("update to Audit table is Failed ::\n",update_audit_query)
        logging.error(e)
    
def sf_execute_query(query):
    con=get_sf_connect()
    cursor=con.cursor()
    try:
        logging.info(query)
        print(query)
        cursor.execute(query)
        logging.info(f'query execute {query} is completed')
    except Exception as e:
        print('failed at {query}')
        logging.info(f'query execute {query} is failed')
        
def etl_process_audit(workflow):
    start_date=end_date()
    insert_command=f"""insert into "AUDIT"."BE_ELT_PROCESS_API"
          (PLATFORM,WORKFLOWNAME,START_TIME,STATUS)            
          VALUES('Sailthru_Api','{workflow}',CURRENT_TIMESTAMP,'Running');"""
    con=get_sf_connect()
    cursor=con.cursor()
    logging.info(insert_command)
    print(insert_command)
    cursor.execute(insert_command)
    print('Inserted to Audit table is completed ')
    logging.info('Inserted to Audit table is completed ')

def etl_process_id(object_name):
    con=get_sf_connect()
    sf_cursor=con.cursor()
    process_id_query=f"""select MAX(ID) from  "AUDIT"."BE_ELT_PROCESS_API"  where  PLATFORM='Sailthru_Api' AND WORKFLOWNAME='{object_name}' """
    print(process_id_query)
    logging.info(process_id_query)
    sf_cursor.execute(process_id_query)
    process_id=sf_cursor.fetchone()[0]
    return process_id


def etl_process_update(process_id):
    sf_conn=get_sf_connect()
    sf_cursor=sf_conn.cursor()
    try:
        etl_process_update=f"""update "AUDIT"."BE_ELT_PROCESS_API"  set STATUS='COMPLETED' WHERE WORKFLOWNAME='Purchase' and ID ={process_id}"""
        
        try:
            print(etl_process_update)
            sf_cursor.execute(etl_process_update)
            logging.info(etl_process_update)
            logging.info('Updating ETL process is Completed')
    
        except Exception as e:
            logging.info('faild',etl_process_update)
            logging.info("etl_process_update is Failed ::\n",etl_process_update)
            logging.error(e)                                         
                            
    except Exception as e:
        logging.info(e)
        logging.info('Falied at  Etl_process_update')    
        logging.info('Falied at  Etl_process_update process')
        return    
    
def s3_file_name():
        con=get_sf_connect()
        sf_cursor=con.cursor()
        filemname=f"""SELECT FILE_NAME FROM  AUDIT.BE_ELT_AUDIT_API WHERE PROCESS_ID in (select MAX(ID) AS PROCESS_ID from  "AUDIT"."BE_ELT_PROCESS_API"  where  PLATFORM='Sailthru_Api' AND WORKFLOWNAME='Purchase')"""
        print(filemname)
        logging.info(filemname)
        sf_cursor.execute(filemname)
        file_name=sf_cursor.fetchone()[0]
        return file_name

def upload_file_to_S3(file_name, bucket,object_name):
    """Upload a file to an S3 bucket
            :param file_name: File to upload
            :param bucket: Bucket to upload to
            :param object_name: S3 object name. If not specified then file_name is used
            :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket,object_name)
    except Exception as e:
        logging.info(e)
        logging.info(e)
        return False
    return True

def sf_execute_count(query):
    sf_conn=get_sf_connect()
    sf_cursor=sf_conn.cursor()
    try:
        print(query)
        sf_cursor.execute(query)
        data=sf_cursor.fetchone()[0]
        print(data)
    except Exception as e:
            logging.info(e)
            logging.info('Falied at Integration process')    
            logging.info('Falied at  Integration process')
            # ns_failed_constant=ns_failed_content.format(task='Integration',transaction_date=end_date(),job=" Falied at Integration process ",e=e)
            # send_email(content=ns_failed_constant ,Subject=ns_failed_subject)
            return
    return data

def error_to_audit_api(object,task,PROCESS_ID):
    """
        insert to error table
    """

    conn = get_sf_connect();
    cursor=conn.cursor()


    update_error=f"""update AUDIT.BE_ELT_AUDIT_API
        set STATUS='Failed',END_TIME= CURRENT_TIMESTAMP where 
         id = (select max(id) from AUDIT.BE_ELT_AUDIT_API 
                                         where PACKAGE ='Sailthru_Api' and OBJECT='{object}' and DAG='{task}' and process_id={PROCESS_ID} );
        """

    try:
        logging.info(update_error)
        cursor.execute(update_error)
        logging.info('Updated Audit tables succeeded')
    except Exception as e:
         logging.info(update_error)
         logging.info(e)

