#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 13 11:57:13 2018

"""


####################################################################################################################################
# Airflow module
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.sensors import TimeDeltaSensor
from airflow.models import DAG
import logging


#regular module
import bson
import io
import os
from pymongo import MongoClient
import shutil
try:
    to_unicode = unicode
except NameError:
    to_unicode = str
    
import pandas as pd
import zipfile as zf
from sqlalchemy import create_engine
import psycopg2


# time module
import datetime
####################################################################################################################################
os.chdir('/home/ec2-user')    
####################################################################################################################################
def download_mongo_dump_normalize_to_s3(ds, **kwargs):
    # Task to download the latest Mongo DB dump data file and title file  
    
    import boto
    from boto.s3.key import Key    
    
    run_date = format(datetime.datetime.now().strftime('%Y-%m-%d'))  
    
    logging.info("Running for date : {}".format(run_date))
    
    # username and password for Amazon
    aws_access_key_id = kwargs['params']['aws_access_key_id']
    aws_access_key = kwargs['params']['aws_access_key']    
    s3_input_bucket = kwargs['params']['s3_input_bucket']   
    s3_output_bucket = kwargs['params']['s3_output_bucket']    
   
    key = boto.connect_s3(aws_access_key_id, aws_access_key)
    bucket = key.lookup(s3_input_bucket)    
            
    # # Download the latest mondo dump from S3
    
    # modified_date= [(file.last_modified, file) for file in bucket]
    # file_to_download = sorted(modified_date, cmp=lambda x,y: cmp(x[0], y[0]))[-1][1]
    
    os.chdir('/home/ec2-user/airflow_dags_results/title_to_bisac_ingest/')
    
    # logging.info("Downloading MongoDB data dump")
    
    # file_to_download.get_contents_to_filename('mongo_dump.zip')

    # logging.info("Unzip MongoDB data dump")

    # # Unzip data dump
    # zip_handle = zf.ZipFile('mongo_dump.zip')
    # zip_handle.extractall()
    
    # # Load books
    # with open('edison-nye/books.bson','rb') as f:
    #      data = bson.decode_all(f.read())

    # Connect to production client and get data from db.collection
    
    logging.info("Connectting to the mongodb server")
    client_production = MongoClient('")
    db = client_production['edison-nye']
    collection = db.books
    
    logging.info("Getting data from edison-nye/books")
    data = list(collection.find())
         
    # Data reshaping
    result = []
    for item in data:
        dict = {}
        if item.get('bisacs') is not None:
            dict['bisacs'] = item.get('bisacs')
        else: 
            dict['bisacs'] = []
        if item.get('primary_isbn') is not None:
            dict['primaryisbn13']=item.get('primary_isbn')
        else:
            dict['primaryisbn13'] = ''
        if item.get('title') is not None:
            dict['title'] = item.get('title')
        else:
            dict['title'] = ''
        result.append(dict)
        
        
    # Unnest json
    df_bisacs_primaryisbn13 = pd.io.json.json_normalize(result, record_path = 'bisacs',
                                                        meta = ['primaryisbn13','title'])
    
    df_bisacs_primaryisbn13 = df_bisacs_primaryisbn13.rename(columns = {0:'bisac',
                                                                        'primaryisbn13':'primary_isbn13'})
    
    # Convert nan to empty string
    df_bisacs_primaryisbn13 = df_bisacs_primaryisbn13.replace(['nan','None'],'')
    df_bisacs_primaryisbn13 = df_bisacs_primaryisbn13.fillna('')
    
    #     
    df_bisacs_primaryisbn13.to_csv('df_bisac_primaryisbn13.csv', encoding = 'utf-8', index = False)
    
         
    # Upload title data file to S3 bucket
    conn = boto.connect_s3(aws_access_key_id, aws_access_key)
    bucket = conn.get_bucket(s3_output_bucket, validate = False)
    #Upload to S3
    upload = Key(bucket)    
    
    #Save updated ebb list data to S3
    csv_buffer = io.BytesIO()
    df_bisacs_primaryisbn13.to_csv(csv_buffer, index=False, encoding='utf8')
    csv_buffer.seek(0)
    
    #Save the complete ebb list data load file to S3
    upload.key = 'mongodb/title_to_bisac.csv'
    
    logging.info("Uploading file - {} to S3".format(upload.key))
    
    upload.set_contents_from_string(csv_buffer.getvalue())  

    logging.info("Removing the intermediate Mongo dump ")

    # remove the mongo dump file
    # os.remove("/home/ec2-user/airflow_dags_results/title_to_bisac_ingest/mongo_dump.zip")  
    
    # remove the unzip file directory
    # shutil.rmtree("/home/ec2-user/airflow_dags_results/title_to_bisac_ingest/edison-nye")

    logging.info("Task completed successfully ")


def title_data_to_redshift_and_mysql(ds, **kwargs):
    # Load the Redshift and MysSQL table with latest title data
 
    redshift_user = kwargs['params']['redshift_user']
    redshift_password = kwargs['params']['redshift_password']
    
    os.chdir('/home/ec2-user/airflow_dags_results/title_to_bisac_ingest')
            
    logging.info("Loading data to MySQL ")
        
    # Creating a SQL engine for dumping data
    engine = create_engine('mysql+mysqlconnector://'+ mysql_user +':'+ mysql_password +'@orim-internal-db01.cqbltkaqn0z7.us-east-1.rds.amazonaws.com/openroad_internal', echo=False)
    conn = engine.connect()   
        
    # Delete the data from table and load recent title bisac data to MySQL
    conn.execute('truncate table title_to_bisac')
        
    # Insert data
    sql = """
            LOAD DATA LOCAL INFILE 'df_bisac_primaryisbn13.csv'
            INTO TABLE title_to_bisac
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES (bisac, title, primary_isbn13);
          """
    conn.execute(sql)
    
    logging.info("MySQL table title_to_bisac loaded successfully")
    
    logging.info("Loading data to Redshift ")
    
    # Connect to RedShift
    dbname = "sailthrudata"
    host = "sailthru-data.cmpnfzedptft.us-east-1.redshift.amazonaws.com"        
    conn_string = "dbname=%s port='5439' user=%s password=%s host=%s" %(dbname, redshift_user, redshift_password, host)
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Delete existing data from table
    sql = "truncate table title_to_bisac"
    cursor.execute(sql)
                                
    # Ingesting to redshift"
        
    logging.info("Ingesting all authors data to Redshift")    
    
    sql = """
            COPY title_to_bisac FROM 's3://orim-misc-data/mongodb/title_to_bisac.csv'
            CREDENTIALS 'aws_iam_role=arn:aws:iam::822605674378:role/DataPipelineRole'
            DELIMITER ','
            IGNOREHEADER 1
            EMPTYASNULL
            QUOTE '"'
            CSV
            REGION 'us-east-1';
         """
    cursor.execute(sql)
    conn.commit()
    
    logging.info("Redshift table title_to_bisac loaded successfully")
   
####################################################################################################################################
# Convert dag start date from local timezone to utc
dag_start_date_utc = datetime.datetime.strptime('2018-08-28', '%Y-%m-%d')


####################################################################################################################################
# Load env file
with open(os.environ['HOME']+'/env.txt') as f:
    env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)

####################################################################################################################################

# Define DAG
args = {
        'owner': 'sbhange',
        'start_date': dag_start_date_utc,
        'email':emaillist.split(','),
        'email_on_failure': True,
        'email_on_retry': True,
        'catchup': False
        }

dag = DAG(
        dag_id='title_to_bisac_ingest_v2', 
        default_args=args,
        catchup=False,
        schedule_interval='50 10 * * *')

####################################################################################################################################

download_mongo_dump_normalize_to_s3 = PythonOperator(
        task_id = 'download_mongo_dump_normalize_to_s3',
        python_callable = download_mongo_dump_normalize_to_s3,
        provide_context=True,
        params={
                'aws_access_key_id': aws_access_key_id,
                'aws_access_key': aws_access_key,
                's3_input_bucket': 'orion-mongodb-backup',
                's3_output_bucket': 'orim-misc-data'
     			},        
        dag = dag)

title_data_to_redshift_and_mysql = PythonOperator(
        task_id = 'title_data_to_redshift_and_mysql',
        python_callable = title_data_to_redshift_and_mysql,
        provide_context=True,
        params={
                'aws_access_key_id': aws_access_key_id,
                'aws_access_key': aws_access_key,
                'redshift_user': redshift_user,
                'redshift_password': redshift_password,                
                's3_bucket': 'orim-misc-data'
     			},         
        dag = dag)

# Execute DAG
download_mongo_dump_normalize_to_s3 >>  title_data_to_redshift_and_mysql

