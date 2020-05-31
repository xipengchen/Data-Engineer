#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 12:53:41 2019

@author: xchen
"""
#####################################################################################################################################
# Airflow module
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

# Amazon s3 module
import boto
from boto.s3.key import Key
import boto3

# Amazon Reshift module

import psycopg2
from sqlalchemy import create_engine
# regular module
import re
import pandas as pd
import io
import os
import sys

# os.chdir('/home/ec2-user/retailer_pos_data_module/')
# sys.path.insert(0, '/home/ec2-user/retailer_pos_data_module/')
os.chdir('/Users/xchen/')
sys.path.insert(0, '/Users/xchen/')


import os.path
import logging

# time module
from time import sleep
from datetime import datetime, timedelta

# Amazon traffic module dir(Amazon_traffic_fetch)
import Amazon_traffic_fetch


####################################################################################################################################
# input customized date
start_date = '2018-01-01'

####################################################################################################################################
def determine_traffic_date_to_fetch(ds, **kwargs):
    # function to fetch the date
    # connect to Amazon s3
    engine = create_engine("engine link")
    
    table_dates_query = ("select distinct(date) from amazon_traffic_diagnostic_monthly")
    
    table_dates = pd.read_sql_query(table_dates_query, engine)  
    
    table_dates = table_dates.iloc[:,0].tolist()
    
    
    end_date = str(datetime.now().date())
    
    # function to get how many days between two dates
    
    def days_between(d1,d2):
        d1 = datetime.strptime(d1,"%Y-%m-%d")
        d2 = datetime.strptime(d2,"%Y-%m-%d")
        return abs((d2 - d1)).days
    
    
    # to check the start date and end date months are existed in table_dates
    start_to_end_days = days_between(str(end_date), str(start_date))
                                  
    dates_calendar = [datetime.strptime(end_date, '%Y-%m-%d').date() - timedelta(days=x) for x in range(0, start_to_end_days + 1)]
    
    #fetch the dates of 15th of every month 
    dates_to_fetch = []
    for date in list(set(dates_calendar) - set(table_dates)):
        if date.day == 1 and (date.year != datetime.now().date().year or date.month != datetime.now().date().month):
            dates_to_fetch.append(date)

    # print out the type and value inside dates to fetch 
    logging.info("Dates to fetch: {}".format(dates_to_fetch))
    
    # push the value to xcom 
    kwargs['ti'].xcom_push(key='dates_to_fetch', value=dates_to_fetch)


####################################################################################################################################
def download_Amazon_traffic_data_upload_to_s3(ds, **kwargs):
    # function to upload the data in the missing date
    # pull value from xcom
    ti = kwargs['ti']
    xcom_task_ids = 'determine_traffic_date_to_fetch'
    xcom_keys = 'dates_to_fetch'
    # fetch dates_to_fetch data and check the value in Airflow log
    dates_to_fetch = ti.xcom_pull(task_ids = xcom_task_ids, key = xcom_keys)
    logging.info("Dates to fetch: {}".format(dates_to_fetch))

    # username and password for Amazon
    username = amazon_vc_user_zsong
    password = amazon_vc_password_zsong
    country = 'US'
    
    # Access key and ID 
    aws_access_key_id =  kwargs['params']['aws_access_key_id']
    aws_access_key =  kwargs['params']['aws_access_key']

    # Dashboard login url
    url = 'https://arap.amazon.com/dashboard/trafficDiagnostic'
    
    dates_to_upload = []
     
    # call module dir(Amazon_fetch)
    Amazon_fetch = Amazon_traffic_fetch.upload_data_to_s3(aws_access_key_id, aws_access_key, gmail_user_openroadintegrated, gmail_password_openroadintegrated)

    # Run if there are dates to process else exit 
    if len(dates_to_fetch) > 0:
        # login to the amazon web server
        logging.info("Fetching monthly data from Amazon traffic dashboard")
        
        Amazon_fetch.login(url, username, password)   
        Amazon_fetch.define_account(country, url)
        sleep(15)          
        
        
        for input_date in sorted(dates_to_fetch):
            logging.info("Dates to download: {}".format(input_date))
            
            #Amazon_fetch.checkalert()
            Amazon_fetch.access_monthly_button()

            # check missing date
            input_day = 15
            mis_date = datetime.strftime(input_date, '%B %Y')

            for i in range(2):
                Amazon_fetch.pick_date(input_day, mis_date)
                    
            Amazon_fetch.access_apply_button()
            sleep(15)
            
            Amazon_fetch.downloadCSV()
            sleep(20)
                        
            if Amazon_fetch.processFile('Download_monthly',input_date) == 1:
                logging.info("Upload File{} to S3".format(input_date))
                
                Amazon_fetch.uploadFiletoS3(input_date,aws_access_key_id, aws_access_key,"retailer-pos-data", "Amazon_US_traffic/Download_monthly")
                
                dates_to_upload.append(input_date)                                
                                    
            Amazon_fetch.chromerefresh()
            
            logging.info("Finish {} Fetch".format(input_date))
            
            sleep(10)

        Amazon_fetch.chromequit()
    # push the value to xcom 
    kwargs['ti'].xcom_push(key='dates_to_upload', value=dates_to_upload)

#################################################################################################################################### 
def upload_aggregate_s3_traffic_data_to_reshift(ds, **kwargs):
    # function to upload the data in the missing date

    # pull value from xcom
    ti = kwargs['ti']
    xcom_task_ids = 'download_Amazon_traffic_data_upload_to_s3'
    xcom_keys = 'dates_to_upload'
    
    # fetch dates_to_fetch data and check the value in Airflow log
    dates_to_upload = ti.xcom_pull(task_ids = xcom_task_ids, key = xcom_keys)
    
    logging.info("Dates to fetch: {}".format(dates_to_upload))
    
    aws_access_key_id =  kwargs['params']['aws_access_key_id']
    aws_access_key =  kwargs['params']['aws_access_key']
    
    # Run if there are dates to process else exit 
    if len(dates_to_upload) > 0:
    
        for input_date in sorted(dates_to_upload):
            
            logging.info("Running for date - ",format(input_date.strftime("%Y%m%d")))
            s3_bucket_name = "retailer-pos-data"
            s3_folder = "Amazon_US_traffic/Download_monthly/"
            
            Amz_traffic_file = s3_folder + 'Amz_traffic_{}.csv'.format(input_date.strftime("%Y%m%d"))
            
            s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_access_key)
            obj = s3.get_object(Bucket='retailer-pos-data', Key=Amz_traffic_file)
                        
            Amz_traffic_read_df = pd.read_csv(io.BytesIO(obj['Body'].read()),skiprows=1,encoding='utf-8')   

            
            # % of Total GVs 
            total_gv_pct = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['% of Total GVs'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                total_gv_pct.append(num)
            # Change in GV Prior Period        
            change_in_gv_prior_period_pct = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Change in Glance View - Prior Period'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                change_in_gv_prior_period_pct.append(num)        
            
            
            # Change in GV Last Year      
            change_in_GV_last_year = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Change in GV Last Year'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                change_in_GV_last_year.append(num) 
                
                
            # Unique Visitors - Prior Period            
            unique_visitors_prior_period = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Unique Visitors - Prior Period'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                unique_visitors_prior_period.append(num)  
                
                
            # Unique Visitors - Last Year
            unique_visitors_last_year = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Unique Visitors - Last Year'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                unique_visitors_last_year.append(num)         
            
            # Conversion Percentile
            conversion_percentile = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Conversion Percentile'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                conversion_percentile.append(num)    
                # Change in Conversion Prior Period
            change_in_conversion_prior_period = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Change in Conversion - Prior Period'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                change_in_conversion_prior_period.append(num)        
                
            # Change in Conversion Last Year
            change_in_conversion_last_year = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Change in Conversion Last Year'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                change_in_conversion_last_year.append(num)        
            
            # Fast Track Glance View
            fast_track_glance_view = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Fast Track Glance View'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                fast_track_glance_view.append(num) 
    
            # Fast Track Glance View - Prior Period
            fast_track_glance_view_prior_period = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Fast Track Glance View - Prior Period'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                fast_track_glance_view_prior_period.append(num) 
    
            # Fast Track Glance View - Last Year
            fast_track_glance_view_last_year = []
            for i in range(len(Amz_traffic_read_df)):
                val = Amz_traffic_read_df['Fast Track Glance View - Last Year'][i]
                try:
                    num = ''.join((ch if ch in '-0123456789,.' else ' ') for ch in val) 
                    num = float(num.replace(',','')) 
                except:
                    num = float('nan')
                fast_track_glance_view_last_year.append(num)
  
            # Create daat frame form the formatted data
    
            aggregated_amz_traffic_df = pd.DataFrame({'asin':Amz_traffic_read_df['ASIN'],
                                                      'title':Amz_traffic_read_df['Product Title'],
                                                      'total_gv_pct':total_gv_pct,
                                                      'change_gv_prior_month':change_in_gv_prior_period_pct,
                                                      'change_gv_last_year':change_in_GV_last_year,
                                                       'uv_prior_month':unique_visitors_prior_period,
                                                       'uv_last_year':unique_visitors_last_year,
                                                       'conv_pctl':conversion_percentile,
                                                       'change_conv_prior_month':change_in_conversion_prior_period,
                                                       'change_conv_last_year':change_in_conversion_last_year,  
                                                       'fast_track_gv':fast_track_glance_view,
                                                       'fast_track_gv_prior_month':fast_track_glance_view_prior_period,
                                                       'fast_track_gv_last_year':fast_track_glance_view_last_year,                                                    
                                                       })
            # Set the processing date field value
            aggregated_amz_traffic_df['date'] = input_date.strftime("%Y-%m-%d")
            
            # Reorder the dataframe columns
            aggregated_amz_traffic_df = aggregated_amz_traffic_df[['date',
                                                                   'asin',
                                                                   'title',
                                                                   'total_gv_pct',
                                                                   'change_gv_prior_month',
                                                                   'change_gv_last_year',
                                                                   'uv_prior_month',
                                                                   'uv_last_year',
                                                                   'conv_pctl',
                                                                   'change_conv_prior_month',                                                               
                                                                   'change_conv_last_year',
                                                                   'fast_track_gv',
                                                                   'fast_track_gv_prior_month',
                                                                   'fast_track_gv_last_year']]
            # upload aggregated data to S3
            s3_bucket_name = "retailer-pos-data"
            s3_folder = "Amazon_US_traffic/Aggregation_monthly"
            
            aggregated_amz_traffic_data = io.BytesIO()
            aggregated_amz_traffic_df.to_csv(aggregated_amz_traffic_data, index=False, encoding='utf8')
            aggregated_amz_traffic_data.seek(0)        
            
            s3_file = 'Amz_traffic_{}.csv'.format(input_date.strftime("%Y%m%d"))
            # Update variables for S3 connection
            
            connec = boto.connect_s3(aws_access_key_id, aws_access_key)
            bucket = connec.get_bucket(s3_bucket_name, validate = False)
            
            #Upload to S3
            upload = Key(bucket)        
            #Save the udpated and new ebb list data records to S3
            upload.key = '{0}/{1}'.format(s3_folder, s3_file)
            upload.set_contents_from_string(aggregated_amz_traffic_data.getvalue())
            
            logging.info("File created in Aggregation folder -".format(s3_file))
            
            # Loading data to Redshift table 
            # Set up variables for Redshift API    
            
            dbname = "sailthrudata"
            host = "sailthru-data.cmpnfzedptft.us-east-1.redshift.amazonaws.com"    
            
            # Connect to RedShift
            conn_string = "dbname=%s port='5439' user=%s password=%s host=%s" %(dbname, redshift_user, redshift_password, host)
            # logging.info("Connecting to database\n        -> %s" % (conn_string))
            conn = psycopg2.connect(conn_string)
            
            cursor = conn.cursor()    
            
            # logging.info("Ingesting file {} to redshift table".format(s3_file))
            sql = """COPY amazon_traffic_diagnostic_monthly FROM 's3://retailer-pos-data/Amazon_US_traffic/Aggregation_monthly/%s' CREDENTIALS 'aws_iam_role=arn:aws:iam::822605674378:role/DataPipelineRole' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'YYYY-MM-DD HH24:MI:SS' EMPTYASNULL QUOTE AS '"' CSV REGION 'us-east-1';"""%(s3_file)
            cursor.execute(sql)
            conn.commit()
            
            # Removing the intermediate file from the EC2 server                           
#            delete_file = '/home/ec2-user/airflow_dags_results/amazon_US_traffic/Amz_traffic_{}.csv'.format(input_date.strftime("%Y%m%d"))
#            os.remove(delete_file)        
                                
            logging.info("S3 file {} has been ingested to redshift table - amazon_traffic_diagnostic".format(s3_file))        
        
        
####################################################################################################################################
# Convert dag start date from local timezone to utc
dag_start_date_utc = datetime.strptime('2019-09-20', '%Y-%m-%d')


# Load env file
with open(os.environ['HOME']+'/env.txt') as f:
    env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)

# Define DAG
args = {
        'owner': 'xchen',
        'start_date': dag_start_date_utc,
        'email':emaillist.split(','),
        'email_on_failure': True,
        'email_on_retry': True,
        'catchup': False
        }

dag = DAG(
        dag_id='amazon_traffic_data_redshift_monthly_v1',
        default_args=args,
        catchup=False,
        schedule_interval='00 21 * * *'
        )

####################################################################################################################################


determine_traffic_date_to_fetch = PythonOperator(
        task_id='determine_traffic_date_to_fetch',
        provide_context=True,
        python_callable = determine_traffic_date_to_fetch,
        xcom_push=True,
        dag=dag)

download_Amazon_traffic_data_upload_to_s3 = PythonOperator(
        task_id = 'download_Amazon_traffic_data_upload_to_s3',
        provide_context=True,
        params={
            'aws_access_key_id': aws_access_key_id,
            'aws_access_key': aws_access_key,         
 			},         
        python_callable = download_Amazon_traffic_data_upload_to_s3,
        dag = dag)

upload_aggregate_s3_traffic_data_to_reshift = PythonOperator(
        task_id = 'upload_aggregate_s3_traffic_data_to_reshift',
        provide_context=True,
        params={
            'aws_access_key_id': aws_access_key_id,
            'aws_access_key': aws_access_key,         
 			},        
        python_callable = upload_aggregate_s3_traffic_data_to_reshift,
        dag = dag)

determine_traffic_date_to_fetch  >> download_Amazon_traffic_data_upload_to_s3 >> upload_aggregate_s3_traffic_data_to_reshift
        
        
        
        
