####################################################### module #################################################################
# Airflow module
from __future__ import print_function
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime

# Application modeul
import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
import sqlalchemy
import os
import numpy as np
import logging

####################################################### env.file ###################################################################
# Load env file

with open(os.environ['HOME']+'/env.txt') as f:
    env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)

os.chdir('/home/ec2-user')


####################################################### Function  ###################################################################

def ingest_promo_grid(ds, **kwargs):
    
    campaigns = 'https://docs.google.com/spreadsheets/d/1oYOXYLd_AU11zDxpgDxSDZ8CWmBRZE9oOrfJw3cpNoQ/pub?output=xlsx'    
    urllib.urlretrieve(campaigns, "airflow_dags_results/title_campaign/promo_grid.xlsx")
    months = pd.ExcelFile("airflow_dags_results/title_campaign/promo_grid.xlsx")
    
    
    campaigns = pd.DataFrame()
    for mon in months.sheet_names:
        # exclude non-campaign-related tabs and tabs that before 2019
        if ('Form' not in mon and 'Metadata' not in mon and int(mon.split(' ')[1]) >= 2019) or (mon in ('September 2018', 'Ocotober 2018', 'November 2018', 'December 2018')) :
            campaigns_temp = pd.read_excel(months, mon, converters={'Primary ISBN':str})
            campaigns_temp = campaigns_temp.loc[campaigns_temp['Promo Date'] != 'TBD']
            campaigns_temp['tab'] = mon
            campaigns = campaigns.append(campaigns_temp)
    
    
    
    campaigns = campaigns[['Campaign Type', 'Country', 'Promo Date', 'End Date','Primary ISBN' ,'Promo Price', 'Retailer', 'Title List']]
    campaigns.columns = ['campaign', 'country', 'start_date', 'end_date', 'primaryisbn13', 'promo_price', 'retailer', 'title_list']
    
    campaigns_ls = campaigns.to_dict(orient='records')
    campaigns_ls_explode = []
    for cam in campaigns_ls:
    	logging.info(cam)
        d = {}
        d['campaign'] = cam['campaign']
        d['country'] = [ca.strip() for ca in cam['country'].split(',')]
        d['title_list'] = cam['title_list']
        d['start_date'] = cam['start_date']
        d['end_date'] = cam['end_date']
        d['primaryisbn13'] = cam['primaryisbn13']
        d['promo_price'] = cam['promo_price']
        d['retailer'] = [ca.strip() for ca in cam['retailer'].split(',')]
        campaigns_ls_explode.append(d)
        
    campaigns_ls_explode_df_1 =pd.io.json.json_normalize(campaigns_ls_explode,record_path='country', meta=['campaign','title_list','start_date','end_date','primaryisbn13','promo_price'])
    campaigns_ls_explode_df_1.columns = ['country', 'campaign', 'end_date', 'title_list', 'promo_price', 'primaryisbn13', 'start_date']
    
    campaigns_ls_explode_df_2 =pd.io.json.json_normalize(campaigns_ls_explode,record_path='retailer', meta=['campaign','title_list','start_date','end_date','primaryisbn13','promo_price'])
    campaigns_ls_explode_df_2.columns = ['retailer', 'campaign', 'end_date', 'title_list', 'promo_price', 'primaryisbn13', 'start_date']
    
    
    campaigns_ls_explode_df = campaigns_ls_explode_df_1.merge(campaigns_ls_explode_df_2, how = 'outer', on = ['campaign','title_list','start_date','end_date','primaryisbn13','promo_price'])
    
    
    campaigns_ls_explode_df = campaigns_ls_explode_df[['campaign', 'country', 'start_date', 'end_date', 'primaryisbn13', 'promo_price', 'retailer', 'title_list']]
    
    campaigns_ls_explode_df = campaigns_ls_explode_df.drop_duplicates(subset= ['campaign', 'country','start_date', 'end_date','primaryisbn13', 'promo_price','retailer'])
    
    campaigns_ls_explode_df['retailer'] = campaigns_ls_explode_df['retailer'].str.replace('B&N', 'Barnes & Noble')
    
    engine = create_engine('mysql+mysqlconnector://%s:%s@orim-internal-db01.cqbltkaqn0z7.us-east-1.rds.amazonaws.com/openroad_internal' % (mysql_user, mysql_password), echo=False)
    con = engine.connect()

    mysql_table = 'title_campaign'
    mysql_staging = mysql_table + '_staging'
    sql = 'DROP TABLE IF EXISTS {0}'.format(mysql_staging)
    con.execute(sql)
    sql = 'CREATE TABLE {0} LIKE {1};'.format(mysql_staging,mysql_table)
    con.execute(sql)
    campaigns_ls_explode_df.to_sql(con=con, name = mysql_staging, if_exists='append', index = False, chunksize = 20000, 
                             dtype={
                                     'campaign': sqlalchemy.types.NVARCHAR(length=100),
                                     'country': sqlalchemy.types.NVARCHAR(length=100),
                                     'start_date': sqlalchemy.types.DATE,
                                     'end_date': sqlalchemy.types.DATE,
                                     'primaryisbn13': sqlalchemy.types.NVARCHAR(length=13),
                                     'promo_price': sqlalchemy.types.DECIMAL(8,2),
                                     'retailer': sqlalchemy.types.NVARCHAR(length=100),
                                     'title_list': sqlalchemy.types.NVARCHAR(length=100)
                             })

    sql = """DROP TABLE {}""".format(mysql_table)
    con.execute(sql)
    sql = """ALTER TABLE {0} RENAME TO {1}""".format(mysql_staging,mysql_table)
    con.execute(sql)

    engine.dispose()
    
dag_start_date_utc = datetime.strptime('2019-05-22', '%Y-%m-%d')

args = {
        'owner': 'zsong',
        'start_date': dag_start_date_utc,
        'email':emaillist.split(','),
        'email_on_failure': True,
        'email_on_retry': True,
        'catchup': False
        }

dag = DAG(
        dag_id='title_campaign_v1',
        default_args=args,
        catchup=False,
        schedule_interval='15 5 * * *'
        )
        
ingest_promo_grid = PythonOperator(
        task_id = 'ingest_promo_grid',
        python_callable = ingest_promo_grid,
        provide_context=True,
        dag = dag)
        
ingest_promo_grid
