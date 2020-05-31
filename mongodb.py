#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 2018
"""
####################################################################################################################################
# Airflow module
from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

# Regular module
import os
import logging
import time
import pandas as pd
import numpy as np
import pandas.io.sql as sqlio
from cStringIO import StringIO
from datetime import datetime, timedelta

# SQL module
import psycopg2
from sqlalchemy import create_engine
import sqlalchemy
import pymysql.cursors

# Amazon_s3
import boto
from boto.s3.key import Key

# Mongo module
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
####################################################################################################################################
# Load env file
with open(os.environ['HOME']+'/env.txt') as f:
    env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)
####################################################################################################################################
def fetch_ebb_nomination(ds, **kwargs):

    def redshift_table(redshift_user, redshift_password):
        # fetch data from redshift 

        dbname = "sailthrudata"
        host = "sailthru-data.cmpnfzedptft.us-east-1.redshift.amazonaws.com" 

        # create connection
        conn_string = "dbname=%s port='5439' user=%s password=%s host=%s" %(dbname, redshift_user, redshift_password, host)
        conn = psycopg2.connect(conn_string)

        sql =   """
                select catalog.primary_isbn13, title, publisher, bisac_status, bisac, bisac_all, ignition, public_domain, most_recent_promo_date, promo_times, latest_rating, latest_reviews, live_clickers, live_subscribers, cast(live_clickers as float) / cast(live_subscribers as float) *100 ebb_saturation, authors, authors_firebrand_id, authors_conv_avg, editors, editors_firebrand_id, editors_conv_avg, amz_con, number_of_con, dates, sale_dlps, current_date fetch_date
                from
                (
                -- Catalog
                select distinct title, primary_isbn13, publisher, bisac_status, 
                (case when marketingservices = 'Y' then 1 else 0 end) ignition, 
                (case when publicdomain = 'N' then 0 when publicdomain = 'Y' then 1 else null end) public_domain,' ' pk
                from title_links_feed
                where asin_active = 1
                and bisac_status in ('active', 'not yet published')
                and partner_title = 'N'
                ) catalog

                left join
                (
                -- Most recent scrape
                select primary_isbn13, avg(rating) latest_rating, avg(reviews) latest_reviews
                from
                (
                    select asin, max(created_time) max_created_time
                    from retailer_site_data_mozenda
                    group by asin
                ) max_scrape
                left join retailer_site_data_mozenda scrape
                on max_scrape.asin = scrape.asin and max_scrape.max_created_time = scrape.created_time
                left join 
                (
                    select distinct primary_isbn13, asin from title_links_feed
                ) tlf
                on max_scrape.asin = tlf.asin
                group by primary_isbn13
                ) scrape
                on catalog.primary_isbn13 = scrape.primary_isbn13


                left join
                (
                -- EBB performance
                select schedule.primary_isbn13, most_recent_promo_date, promo_times
                from
                (
                    select primary_isbn13, max(date) most_recent_promo_date, count(distinct date) promo_times  
                    from ebb_attribution_new_model 
                    group by primary_isbn13
                ) schedule
                ) ebb_perf
                on catalog.primary_isbn13 = ebb_perf.primary_isbn13

                left join
                (
                -- EBB live clickers
                select primaryisbn13, count(distinct profile_id) live_clickers
                from
                (
                    select primaryisbn13, eld.*
                    from ebb_click_data ecd
                    left join 
                    (
                      select profile_id, engagement
                      from ebb_list_data
                      where engagement not in ('hardbounce', 'dormant', 'optout')
                    ) eld
                    on ecd.profile_id = eld.profile_id
                ) click
                group by primaryisbn13
                ) clicker
                on clicker.primaryisbn13 = catalog.primary_isbn13

                left join 
                (
                -- EBB live subscribers
                select ' ' pk, count(profile_id) live_subscribers
                from ebb_list_data
                where engagement not in ('hardbounce', 'dormant', 'optout')
                ) eld
                on eld.pk = catalog.pk

                left join
                (
                --- Bisac
                select distinct primary_isbn13,
                concat(concat(split_part(listagg((case when rank =1 then bisac end), ''),'/', 1), '/'), split_part(listagg((case when rank =1 then bisac end), ''),'/', 2)) bisac
                from 
                (
                    select primary_isbn13, title, bisac, row_number() over (partition by primary_isbn13) rank
                    from title_to_bisac
                )
                group by primary_isbn13
                ) bisac
                on bisac.primary_isbn13 = catalog.primary_isbn13
                
                left join
                (
                --- Bisac for Tech: Tech team requires a complete list of bisacs
                select distinct primary_isbn13, listagg(bisac, ', ') bisac_all
                from 
                (
                    select primary_isbn13, title, bisac, row_number() over (partition by primary_isbn13) rank
                    from title_to_bisac
                )
                group by primary_isbn13
                ) bisac_all
                on bisac_all.primary_isbn13 = catalog.primary_isbn13

                left join
                (
                --- Author Preformance
                select author.primary_isbn13, authors, authors_firebrand_id, authors_conv_avg, editors, editors_firebrand_id, editors_conv_avg, amz_con, number_of_con, dates, sale_dlps
                from
                (
                    --- Author, Editor, Firebrand_ID and Name
                    select distinct title_link.primary_isbn13, authors, authors_firebrand_id, authors_conv_avg, editors, editors_firebrand_id, editors_conv_avg
                    from
                    (
                        --- Active ISBN13
                        select primary_isbn13 
                        from title_links_feed
                        where asin_active = 1 and bisac_status in ('active', 'not yet published') and partner_title = 'N'
                    ) title_link
                    left join
                    (
                        select author_role.primary_isbn13, authors, authors_firebrand_id, authors_conv_avg, editors, editors_firebrand_id, editors_conv_avg
                        from
                        (	
                            --- Author name, firebrand_id and conversion rate
                            select primary_isbn13, 
                            listagg(title_detail .author, ', ') within group (order by role) over(partition by primary_isbn13) as authors, 
                            listagg(firebrand_id, ', ') within group(order by role) over(partition by primary_isbn13) as authors_firebrand_id, 
                            listagg(author_avg_conv , ', ') within group(order by role) over(partition by primary_isbn13) as authors_conv_avg 
                            from
                            (
                                select primary_isbn13, author, firebrand_id, role 
                                from title_to_author 
                                where role = 1
                            ) title_detail 
                            left join
                            (
                                select author, isnull(1.0*sum(attributed_units)/sum(clicks), 0) author_avg_conv
                                from 
                                (
                                    select author, primary_isbn13
                                    from title_to_author 
                                    where role = 1
                                ) author_isbn
                                left join
                                (
                                    select primary_isbn13, attributed_units, (clicks)
                                    from 
                                      (select att.*, click.clicks from 
                                          (select date,primary_isbn13,attributed_units, attributed_proceeds 
                                          from ebb_attribution_new_model 
                                          where property_last_touch = 'EBB NL' and title is not null) att
                                        left join 
                                          (select sum(click) clicks, date(schedule_time) date, primaryisbn13 
                                            from ebb_blast_click_stats 
                                            group by schedule_time, primaryisbn13
                                            having  primaryisbn13 != '' and date is not null) click
                                        on att.primary_isbn13 =click.primaryisbn13
                                        and att.date= click.date)
                                    where clicks > 0 
                                ) author_click
                                on author_click.primary_isbn13= author_isbn.primary_isbn13
                                group by author 
                            ) author_con
                            on author_con.author = title_detail.author
                        ) author_role

                        left join
                        (
                            --- Editor name, firebrand_id and conversion rate
                            select primary_isbn13, 
                            listagg(title_detail .author, ', ') within group (order by role) over(partition by primary_isbn13) as editors, 
                            listagg(firebrand_id, ', ') within group(order by role) over(partition by primary_isbn13) as editors_firebrand_id, 
                            listagg(editor_avg_conv , ', ') within group(order by role) over(partition by primary_isbn13) as editors_conv_avg 
                            from
                            (
                                select primary_isbn13, author, firebrand_id, role 
                                from title_to_author 
                                where role = 0
                            ) title_detail 
                            left join
                            (
                                select author, isnull(1.0*sum(attributed_units)/sum(clicks), 0) editor_avg_conv
                                from 
                                (
                                    select author, primary_isbn13
                                    from title_to_author 
                                    where role = 1
                                ) editor_isbn
                                left join
                                (
                                    select primary_isbn13, attributed_units, (clicks)
                                    from 
                                           (select att.*, click.clicks from 
                                              (select date,primary_isbn13,attributed_units,sale_dlp, attributed_proceeds 
                                              from ebb_attribution_new_model 
                                              where property_last_touch = 'EBB NL' and title is not null) att
                                            left join 
                                              (select sum(click) clicks, primaryisbn13, date(schedule_time) date 
                                                from ebb_blast_click_stats 
                                                group by schedule_time, primaryisbn13
                                                having  primaryisbn13 != '' and date is not null) click
                                            on att.primary_isbn13 =click.primaryisbn13
                                            and att.date= click.date) 
                                    where clicks > 0
                                ) editor_click
                                on editor_click.primary_isbn13= editor_isbn.primary_isbn13
                                group by author
                            ) editor_con
                            on editor_con.author = title_detail.author
                        ) editor_role
                        on editor_role.primary_isbn13 = author_role.primary_isbn13

                    ) title_author
                    on title_link.primary_isbn13  = title_author.primary_isbn13 
                ) as author

                left join
                (
                    select amz_con.primary_isbn13 as primary_isbn13, amz_con, dates, sale_dlps, number_of_con 
                    from 
                    (
                        select distinct primary_isbn13, 
                        listagg((case when clicks > 0 then 1.0*(CASE WHEN attributed_units IS NULL THEN '0' ELSE attributed_units END) / clicks else null end),', ') 
                        within group(ORDER BY DATE) OVER (PARTITION BY primary_isbn13) amz_con,
                        listagg(date, ', ') within group (order by date) over(partition by primary_isbn13) as dates,
                        listagg(sale_dlp, ', ') within group (order by date) over(partition by primary_isbn13) as sale_dlps
                        from 
                              (select att.*, click.clicks from 
                                (select date,primary_isbn13,attributed_units,sale_dlp, attributed_proceeds 
                                from ebb_attribution_new_model 
                                where property_last_touch = 'EBB NL' and title is not null) att
                              left join 
                                (select sum(click) clicks, primaryisbn13, date(schedule_time) date 
                                  from ebb_blast_click_stats 
                                  group by schedule_time, primaryisbn13
                                  having  primaryisbn13 != '' and date is not null) click
                              on att.primary_isbn13 =click.primaryisbn13
                              and att.date= click.date) 
                    ) as amz_con
                    left join
                    (
                        select distinct primary_isbn13, 
                        listagg(number_of_con, ', ') within group (order by date) over(partition by primary_isbn13) number_of_con
                        from
                        (
                            select distinct primary_isbn13, 
                            clicks,
                            date,
                            case when attributed_units is null then '0' 
                            else attributed_units end number_of_con
                            from
                              (
                              select att.*, click.clicks from 
                                (select date,primary_isbn13,attributed_units, attributed_proceeds 
                                from ebb_attribution_new_model 
                                where property_last_touch = 'EBB NL' and title is not null) att
                              left join 
                                (select sum(click) clicks, date(schedule_time) date, primaryisbn13 
                                from ebb_blast_click_stats 
                                group by schedule_time, primaryisbn13
                                having  primaryisbn13 != '' and date is not null) click
                              on att.primary_isbn13 =click.primaryisbn13
                              and att.date= click.date
                              )
                        ) 
                    ) as num_con
                    on num_con.primary_isbn13 = amz_con.primary_isbn13
                ) conv
                on conv.primary_isbn13 = author.primary_isbn13 
                ) author_performance
                on author_performance.primary_isbn13 = catalog.primary_isbn13
                """
        # read data in pandas
        df_redshift = sqlio.read_sql_query(sql, conn)

        # close connection
        conn.close()

        return df_redshift

    def sql_table(mysql_user, mysql_password):
        # fetch data from MySQL

        # create connection
        engine = create_engine('mysql+mysqlconnector://%s:%s@orim-internal-db01.cqbltkaqn0z7.us-east-1.rds.amazonaws.com/openroad_internal' % (mysql_user, mysql_password), echo=False)
        conn = engine.connect()

        # read data in pandas
        sql = """
                SELECT isbn13,
                       batch
                FROM ignition_title
                WHERE isbn13 IN (SELECT isbn13
                                 FROM ignition_title
                                 GROUP BY isbn13
                                 HAVING COUNT(isbn13) > 1)
                AND end_date IS NULL
                UNION
                SELECT isbn13,
                       batch
                FROM ignition_title
                WHERE isbn13 NOT IN (SELECT isbn13
                                     FROM ignition_title
                                     GROUP BY isbn13
                                     HAVING COUNT(isbn13) > 1)

                """
        df_sql = pd.read_sql(sql, conn)
        df_sql = df_sql.rename(columns = {'isbn13': 'primary_isbn13'})

        # close connection
        conn.close()

        return df_sql

    def uploadFile_to_airflow(df):
        # upload file to airflow

        # find object columns
        col_obj = list(df.columns[df.dtypes=='object'])

        # replace non ascii word
        df.replace({r'[^\x00-\x7F]+':' '}, regex=True, inplace=True)
        df['publisher'].replace('Open Road Espaol','Open Road Media', regex=True, inplace=True)
        
        # modify data formate
        df['batch'] = df['batch'].astype(str)
        df['publisher-batch'] = df['publisher'] + ': ' + df['batch']

        # change type 
        col_str = ['amz_con', 'authors_conv_avg', 'number_of_con']
        for col in col_str :
            df[col] = df[col].apply(lambda x: x.replace(' ', '').split(',') if type(x) == str else [])
            df[col] = df[col].apply(lambda x: [round(float(i), 2) for i in x] if x != [] else [])
            df[col] = df[col].apply(lambda x: ', '.join(str(i) for i in x) if x !=[] else '')
        #logging.info(df[0:10])
        df.to_csv('/home/ec2-user/airflow_dags_results/ebb_nomination_tool/output.csv', encoding='utf-8', index=False)
        df = df.drop(columns=['bisac_all', 'sale_dlps'])
        df.to_csv('/home/ec2-user/airflow_dags_results/ebb_nomination_tool/ebb_nomination_{}.csv'.format(datetime.now().date().strftime("%Y%m%d")), index = False)
        #df.to_csv('/Users/intern/Desktop/output.csv', encoding='utf-8', index=False)

    def processFile():
        # change the file name
        
        try:
            #os.rename('/Users/intern/Desktop/output.csv', '/Users/intern/Desktop/ebb_nomination_{}.csv'.format(datetime.now().date().strftime("%Y%m%d")))
            os.rename('/home/ec2-user/airflow_dags_results/ebb_nomination_tool/output.csv',\
                      '/home/ec2-user/airflow_dags_results/ebb_nomination_tool/ebb_nomination_mongo_{}.csv'.format(datetime.now().date().strftime("%Y%m%d")))
            return 1
        except:
            logging.info('no file is detected')
            return 0
            
    def uploadFile_to_S3():
        # function to upload files to amazon_s3
        public_read = False
        s3_bucket_name = "ebb-nomination-tool"

        file = '/home/ec2-user/airflow_dags_results/ebb_nomination_tool/ebb_nomination_{}.csv'.format(datetime.now().date().strftime("%Y%m%d"))
        #file = '/Users/intern/Desktop/ebb_nomination_{}.csv'.format(datetime.now().date().strftime("%Y%m%d"))
        s3_file = 'ebb_nomination_{}.csv'.format(datetime.now().date().strftime("%Y%m%d"))

        conn = boto.connect_s3(aws_access_key_id, aws_access_key)
        bucket = conn.get_bucket(s3_bucket_name ,validate = False)

        with open(file,'rb') as d:
            temp_file = StringIO(d.read())
            temp_file.seek(0)
            # Upload to S3
            upload = Key(bucket)
            upload.key = '{0}'.format(s3_file)
            upload.set_contents_from_string(temp_file.getvalue())
            upload.set_contents_from_string(temp_file.getvalue())
            if public_read == True:
                permission_set = bucket.get_key('{0}'.format(s3_file))
                permission_set.set_canned_acl('public-read')
                
    def updateMongo(client):
        # Mongo Staging

        db = client['edison-nye']
        
        collection = db.BookData
        l = list(collection.find().limit(10))
              
        data = pd.read_csv('/home/ec2-user/airflow_dags_results/ebb_nomination_tool/ebb_nomination_mongo_{}.csv'.format(datetime.now().date().strftime("%Y%m%d")))
        
        data_dict = data.to_dict('records')
        
        data_dict_reshaped = []
        for da in data_dict:
            obj = {}
            
            obj['item_type'] = 'BookData'
             
            try:
                obj['bisacs'] = [x for x in da['bisac_all'].split(', ')]
            except:
                obj['bisacs'] = None
             
            # Construct ebbNewsletterData    
            try:
                amazonConv = [float(x) for x in da['amz_con'].split(', ')]
            except:
                amazonConv = [None]
            try:
                convUnits = [int(float(x)) for x in da['number_of_con'].split(', ')]
            except:
                convUnits = [None]
            try:
                runDatesEbb = [datetime.strptime(x, '%Y-%m-%d') for x in da['dates'].split(', ')]
            except:
                runDatesEbb = [None]
            try:
                saleDlp = [float(x) for x in da['sale_dlps'].split(', ')]
            except:
                saleDlp = [None]

            try:
                obj['ebbNewsletterData'] = [{'runDate': runDate, 'promoPrice': promoPrice,'amazonConv': amazonConv ,'convUnits': convUnits} for runDate, promoPrice, amazonConv, convUnits in zip(runDatesEbb, saleDlp, amazonConv, convUnits)]
            except Exception as e:
                obj['ebbNewsletterData']  = None
        
            try:
                obj['authors'] = [x for x in da['authors'].split(', ')]
            except:
                obj['authors'] = None
                
            try:
                obj['authorsFirebrandIds'] = [str(x) for x in da['authors_firebrand_id'].split(', ')]
            except:
                obj['authorsFirebrandIds']  = None
                
            try:
                obj['authorsConv'] = [float(x) for x in da['authors_conv_avg'].split(', ')]
            except:
                obj['authorsConv'] = None
                          
            try:
                obj['editors'] = [x for x in da['editors'].split(', ')]
            except:
                obj['editors'] = None
                
            try:
                obj['editorsFirebrandIds'] = [str(x) for x in da['editors_firebrand_id'].split(', ')]
            except:
                obj['editorsFirebrandIds']  = None
                             
            try:
                obj['editorsConv'] = [float(x) for x in da['editors_conv_avg'].split(', ')]
            except:
                obj['editorsConv'] = None
                
            try:
                obj['liveClickers'] = int(da['live_clickers'])
            except:
                obj['liveClickers'] = None
                   
            obj['title'] = da['title']
            obj['primaryIsbn'] = str(da['primary_isbn13'])
            
            try:
                obj['latestReviews'] = int(da['latest_reviews'])
            except:
                obj['latestReviews'] = None
                
            try:
                obj['mostRecentPromoDateEbb'] = datetime.strptime(da['most_recent_promo_date'], '%Y-%m-%d')
            except:
                obj['mostRecentPromoDateEbb'] = None
                
            try:
                obj['promoTimesEbb'] = int(da['promo_times'])
            except:
                obj['promoTimesEbb'] = None    
                
            obj['publisher'] = da['publisher']

            try:
                if da['batch'] is np.nan:
                    obj['batch'] = None
                else:
                    obj['batch'] = str(da['batch'])
            except:
                obj['batch'] = None
            
            try:
                if float(da['latest_rating']) == float(da['latest_rating']):
                    obj['latestRating'] = float(da['latest_rating'])
                else:
                    obj['latestRating'] = None
            except:
                obj['latestRating'] = None
                 
            obj['liveSubscribers'] = int(da['live_subscribers'])
            
            obj['bisacStatus'] = da['bisac_status']
            obj['updatedDate'] = datetime.strptime(da['fetch_date'], '%Y-%m-%d')
                
            obj['marketingServices'] = da['ignition']
            
            if da['ebb_saturation'] == da['ebb_saturation']:
                obj['saturationEbb'] = float(da['ebb_saturation'])
            else:
                obj['saturationEbb'] = None

            try:
                if float(da['public_domain']) == float(da['public_domain']):
                    obj['publicDomain'] = float(da['public_domain'])
                else:
                    obj['publicDomain'] = None
            except:
                obj['publicDomain'] = None

            data_dict_reshaped.append(obj)
                    
        collection.remove({})
        collection.insert(data_dict_reshaped)
        
    
    def campaignData(client):
        db = client['edison-nye']
        collection = db.BookData
        
        def ingest_to_mongodb(df, fields, mongodb_field, mongodb_field_top, groupby, scheme):
            from datetime import datetime
            
            # Enforce the date field: ensure the date field will be converted to datetime.datetime
            
            for column in df.columns:
                try:
                    # check datetime type in columns
                    test = datetime.strftime(df[column][0], '%Y-%m-%d')
                    # convert into datetime datetime
                    df[column]= df[column].apply(lambda x: datetime.combine(x, datetime.min.time()))
                except:
                    pass
                    
            # Construct mongoDB format      
           
            
            for isbn13 in df['primary_isbn13'].unique():
                
                obj = {}
                
                df_temp = df[df['primary_isbn13'] == isbn13].reset_index(drop=True)
              
                if mongodb_field:
                    if groupby:
                        for group in df_temp[groupby].unique():
                            if scheme:
                                """
                                "byRetailer" :
                                {
                                    "Bookbub":
                                    {
                                        "previousSales" :2,
                                        "previousSalesDate" : "2019-07-01",
                                        "previousSales" :3,
                                        "previousSalesDate" : "2019-07-02",
                                    },
                                }
                                """
                                # convert key value datetime to str
                                try:
                                    df_temp[scheme.keys()[0]]= df_temp[scheme.keys()[0]].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
                                except:
                                    pass
                                
                                # append mutiple rows into a dict
                                obj_temp = {}
                                for row in df_temp[fields][df_temp[groupby]==group].to_dict('records'):
                                    obj_temp.update({row[scheme.keys()[0]]: row[scheme.values()[0]]})
                                # add key value into final destination
                                obj.update({group: obj_temp})
                                
                            else:
                                """
                                "byRetailer" :
                                {
                                    "Bookbub":
                                    {
                                        "previousSales" :2,
                                        "previousSalesDate" : "2019-07-01",
                                        "previousProceeds" : 2040,    
                                    },
                                }
                                """
                                obj[group] = df_temp[fields][df_temp[groupby]==group].to_dict('records')[0]          
                    else:
                        if scheme:
                            """
                            "consumerPriceByDate" : 
                            { 
                                "2019-07-17" : 2.49,
                                "2019-07-16" : 2.49,
                                "2019-07-15" : 2.49,
                            }
                            """
                            # convert key value datetime to str
                            try:
                                df_temp[scheme.keys()[0]]= df_temp[scheme.keys()[0]].apply(lambda x: datetime.strftime(x, '%Y-%m-%d'))
                            except:
                                pass
                            
                            # append mutiple rows into a dict
                            for row in df_temp[fields].to_dict('records'):
                                obj.update({row[scheme.keys()[0]]: row[scheme.values()[0]]})
                            
                        else:
                            """
                            "rank" : 
                            {
                                "currentRankMSList" : 1245,
                                "previousRankMSList" : 353,
                                "latestRank" :4353,
                            },
                            
                            """
                            obj = df_temp[fields].to_dict('records')[0]
                    
                    # append key value
                    obj = {mongodb_field: obj}
        
                else:
                    """
                    "latestConsumerPrice": 2.99,
                    """
                    obj = df_temp[fields].to_dict('records')[0]
                    
                if mongodb_field_top:
                    #obj = {mongodb_field_top: obj}            
                    collection.update_one({'primaryIsbn': isbn13}, {"$set": {"{0}.{1}".format(mongodb_field_top, mongodb_field): obj[mongodb_field]}}, upsert=False)
                else:
                    # Per Tech team, we don't want to insert the record if we couldn't find the primaryIsbn in the collection.
                    collection.update_one({'primaryIsbn': isbn13}, {"$set": obj}, upsert=False)
         
            
        # Set connections to MySQL and Redshift
        connection = pymysql.connect(host='orim-internal-db01.cqbltkaqn0z7.us-east-1.rds.amazonaws.com',
                                     user=mysql_user,
                                     password=mysql_password,                            
                                     db='openroad_internal',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
    
    
        dbname = "sailthrudata"
        host = "sailthru-data.cmpnfzedptft.us-east-1.redshift.amazonaws.com"
        connstr = 'postgres://{0}:{1}@{2}:5439/{3}'.format(redshift_user, redshift_password,host,dbname)
        engine = create_engine(connstr)
    
        # Fetch Data 1: salesByRetailer
        query = '''
        SELECT primary_isbn13, retailername,
                            SUM(
                             CASE 
                             WHEN posdate BETWEEN CURRENT_DATE() - INTERVAL 30 DAY AND CURRENT_DATE()
                             THEN units
                             END
                             ) last30Units,
                             SUM(
                             CASE 
                             WHEN posdate BETWEEN CURRENT_DATE() - INTERVAL 60 DAY AND CURRENT_DATE()
                             THEN units
                             END
                             ) last60Units,
                             SUM(
                             CASE 
                             WHEN posdate BETWEEN CURRENT_DATE() - INTERVAL 90 DAY AND CURRENT_DATE()
                             THEN units
                             END
                             ) last90Units,
                            
                            SUM(
                             CASE 
                             WHEN posdate BETWEEN CURRENT_DATE() - INTERVAL 30 DAY AND CURRENT_DATE()
                             THEN proceeds
                             END
                             ) last30Proceeds,
                            SUM(
                             CASE 
                             WHEN posdate BETWEEN CURRENT_DATE() - INTERVAL 60 DAY AND CURRENT_DATE()
                             THEN proceeds
                             END
                             ) last60Proceeds,
                            SUM(
                             CASE 
                             WHEN posdate BETWEEN CURRENT_DATE() - INTERVAL 90 DAY AND CURRENT_DATE()
                             THEN proceeds
                             END
                             ) last90Proceeds
                            FROM retailer_pos_data rpd
                            LEFT JOIN primaryisbn13_isbn13  pi
                            ON rpd.isbn13 = pi.isbn13
                            WHERE posdate >= CURRENT_DATE() - INTERVAL 90 DAY 
                            AND posdate <=
                            (SELECT MAX(posdate) 
                            FROM retailer_pos_data 
                            WHERE retailername = 'Amazon')
                            #AND retailername IN ('Amazon', 'Barnes & Noble', 'Kobo', 'Apple', 'Google')
                            GROUP BY primary_isbn13, retailername
        ''' 
        salesByRetailer = pd.read_sql(query, con=connection)
        logging.info('Data Point: salesByRetailer')
        logging.info('Querying data from database is complete.')
        logging.info(salesByRetailer.shape)
        ingest_to_mongodb(df = salesByRetailer, fields = [f for f in list(salesByRetailer) if f <> 'primary_isbn13' or f <> 'retailername'], mongodb_field = 'salesByRetailer', mongodb_field_top = None, groupby = 'retailername', scheme = None)
        logging.info('Ingesting to mongodb is complete.')
        
        # Fetch Data 2: sales
        sales = salesByRetailer.groupby(['primary_isbn13']).sum().reset_index()
        logging.info('Data Point: sales')
        logging.info('Querying data from database is complete.')
        logging.info(sales.shape)
        ingest_to_mongodb(df = sales, fields = [f for f in list(sales) if f <> 'primary_isbn13'], mongodb_field = 'sales', mongodb_field_top = None, groupby = None, scheme = None)
        logging.info('Ingesting to mongodb is complete.')
        
        
        # Fetch Data 3: moverShaker
        query = '''
        SELECT DISTINCT primary_isbn13, rank position, sales_rank currentMSRank, previous_sales_rank peviousMSRank, date lastReportDate
        FROM amazon_mover_and_shaker ams
        INNER JOIN title_links_feed tlf
        ON ams.asin = tlf.asin
        WHERE date IN (SELECT MAX(date) FROM amazon_mover_and_shaker)
        AND partner_title = 'N'
        '''
        movershaker = pd.read_sql(query, con=connection)
        logging.info('Data Point: moverShaker')
        logging.info('Querying data from database is complete.')
        logging.info(movershaker.shape)
        ingest_to_mongodb(df = movershaker, fields = [f for f in list(movershaker) if f <> 'primary_isbn13'], mongodb_field = 'moverShaker', mongodb_field_top = None, groupby = None, scheme = None)
        logging.info('Ingesting to mongodb is complete.')
        
        
        # Fetch Data 4: consumerPriceByDate, rankByDate
        query = '''
        SELECT DATE(created_time), primary_isbn13, MIN(kindle_price) consumerPrice, MIN(overall_rank) rank
        FROM retailer_site_data_mozenda rsdm
        LEFT JOIN title_links_feed tlf
        ON rsdm.asin = tlf.asin
        WHERE DATEDIFF(day, created_time, CURRENT_DATE) <= 90
        AND primary_isbn13 IS NOT NULL
        GROUP BY DATE(created_time), primary_isbn13
        '''
        
        with engine.connect() as conn, conn.begin():
            mozendaByDate = pd.read_sql(query,conn)
               
        logging.info('Data Point: consumerPriceByDate, rankByDate')
        logging.info('Querying data from database is complete.') 
        logging.info(mozendaByDate.shape)     
        ingest_to_mongodb(df = mozendaByDate, fields = ['date', 'consumerprice'], mongodb_field = 'consumerPriceByDate', mongodb_field_top = None, groupby = None, scheme = {'date': 'consumerprice'})
        ingest_to_mongodb(df = mozendaByDate, fields = ['date', 'rank'], mongodb_field = 'rankByDate', mongodb_field_top = None, groupby = None, scheme = {'date': 'rank'})
        logging.info('Ingesting to mongodb is complete.')
        
        
        
        # Fetch Data 5: consumerPrice, rank
        query = '''
        SELECT DISTINCT primary_isbn13, currentConsumerPrice, currentRank, previousConsumerPrice, previousRank, snap.*
        FROM
        (
          -- Get the latest result by asin
          SELECT latest.asin, MIN(rsdm.kindle_price) currentConsumerPrice, MIN(rsdm.overall_rank) currentRank
          FROM retailer_site_data_mozenda rsdm
          INNER JOIN
          (
            SELECT 
            asin, max(created_time) max_created_time
            FROM retailer_site_data_mozenda
            GROUP BY asin
          ) latest
          ON rsdm.asin = latest.asin
          AND rsdm.created_time = latest.max_created_time
          GROUP BY latest.asin
        ) l
        FULL JOIN
        (
          -- Get 2nd latest result by asin
          SELECT second.asin, MIN(rsdm2.kindle_price) previousConsumerPrice, MIN(rsdm2.overall_rank) previousRank
          FROM retailer_site_data_mozenda rsdm2
          LEFT JOIN
          (
            SELECT DISTINCT asin, nth_value(created_time, 2)
            OVER (PARTITION BY asin ORDER BY created_time DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as second_last_created_time
            FROM retailer_site_data_mozenda
        
          ) second
          ON rsdm2.asin = second.asin
          AND rsdm2.created_time = second.second_last_created_time
          GROUP BY second.asin
        ) s
        ON l.asin = s.asin
        LEFT JOIN
        (
        -- Get snapshot of consumerPrice and rank
          SELECT ASIN,
          -- consumerPrice
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 30
            THEN kindle_price * 1.00
            END
          ) last30ConsumerPrice,
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 60
            THEN kindle_price * 1.00
            END
          ) last60ConsumerPrice,
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 90
            THEN kindle_price * 1.00
            END
          ) last90ConsumerPrice,
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 365
            THEN kindle_price * 1.00
            END
          ) last365ConsumerPrice,
          
          -- rank
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 30
            THEN overall_rank * 1
            END
          ) last30Rank,
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 60
            THEN overall_rank * 1
            END
          ) last60Rank,
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 90
            THEN overall_rank * 1
            END
          ) last90Rank,
          AVG(
            CASE WHEN DATEDIFF(day, created_time, CURRENT_DATE) <= 365
            THEN overall_rank * 1
            END
          ) last365Rank
          FROM retailer_site_data_mozenda
          GROUP BY asin
        ) snap
        ON snap.asin = l.asin
        INNER JOIN title_links_feed tlf
        ON l.asin = tlf.asin
         '''
        with engine.connect() as conn, conn.begin():
            mozenda = pd.read_sql(query,conn)
        
        logging.info('Data Point: consumerPrice, rank')
        logging.info('Querying data from database is complete.') 
        logging.info(mozenda.shape)    
        ingest_to_mongodb(df = mozenda, fields = [f for f in list(mozenda) if 'consumerprice' in f.lower()], mongodb_field = 'consumerPrice', mongodb_field_top = None, groupby = None, scheme = None)
        ingest_to_mongodb(df = mozenda, fields = [f for f in list(mozenda) if 'rank' in f.lower()], mongodb_field = 'rank', mongodb_field_top = None, groupby = None, scheme = None)
        logging.info('Ingesting to mongodb is complete.')
        
        
        
        # Fetch Data 6: araTraffic
        query = '''
        SELECT DISTINCT date lastReportDate, primary_isbn13, total_gv_pct lastGVPct, change_gv_prior_day lastGVChangePriorDay, change_gv_last_year lastGVChangePriorYear, "﻿conv_pctl" lastConversionPctl, change_conv_prior_day lastConversionPctlPriorDay, "﻿change_conv_last_year" lastConversionPctlPriorYear
        FROM amazon_traffic_diagnostic atd
        INNER JOIN
        (
          SELECT 
          asin, max(date) max_date
          FROM amazon_traffic_diagnostic
          GROUP BY asin
        ) latest
        ON atd.asin = latest.asin
        AND atd.date = latest.max_date
        INNER JOIN title_links_feed tlf
        ON atd.asin = tlf.asin
        '''
        with engine.connect() as conn, conn.begin():
            ara = pd.read_sql(query,conn)
        
        logging.info('Data Point: araTraffic')
        logging.info('Querying data from database is complete.')  
        logging.info(ara.shape)   
        ingest_to_mongodb(df = ara, fields = [f for f in list(ara) if f <> 'primary_isbn13'], mongodb_field = 'araTraffic', mongodb_field_top = None, groupby = None, scheme = None)
        logging.info('Ingesting to mongodb is complete.')
        
        
        # Fetch Data 5: campaignTypePerformanceData - Bookbub Featured Deals
        query = '''
        SELECT ibr.isbn13 primary_isbn13, max_featured_date lastFeaturedDate, total_sales_us estimatedSalesUS, Units dayOfUnits, Proceeds dayOfProceeds, Proceeds/2 - deal_cost orimProceeds
        FROM ingests_bookbub_report ibr
        INNER JOIN
        -- Get latest campaign date
        (
            SELECT isbn13, MAX(featured_date) max_featured_date
            FROM ingests_bookbub_report
            WHERE estimated_us_subscribers IS NOT NULL
            AND isbn13 IS NOT NULL
            GROUP BY isbn13
        ) latest
        ON ibr.isbn13 = latest.isbn13
        AND ibr.featured_date = latest.max_featured_date
        -- Get dayOfUnits and dayOfProceeds
        LEFT JOIN
        (
        	SELECT POSDate, pi.primary_isbn13, SUM(Units) Units, SUM(Proceeds) Proceeds
        	FROM retailer_pos_data rpd
        	INNER JOIN primaryisbn13_isbn13 pi
        	ON rpd.isbn13 = pi.isbn13
        	WHERE rpd.CountryOfSale = 'US'
            AND RetailerName IN ('Amazon', 'Barnes & Noble', 'Apple', 'Kobo', 'Google')
        	GROUP BY POSDate, pi.primary_isbn13
        ) sales
        ON sales.primary_isbn13 = ibr.isbn13
        AND sales.POSDate = latest.max_featured_date
        '''
        logging.info('Data Point: campaignTypePerformanceData - Bookbub Featured Deals')
        logging.info('Querying data from database is complete.') 
        bb_deals = pd.read_sql(query, con=connection)
        logging.info('Ingesting to mongodb is complete.')
        logging.info(bb_deals.shape)
        ingest_to_mongodb(df = bb_deals, fields = [f for f in list(bb_deals) if f <> 'primary_isbn13'], mongodb_field = 'Bookbub Deals', mongodb_field_top = 'campaignTypePerformanceData', groupby = None, scheme = None)
             
        
        
####################################################################################################################################
    # fetch sql data
    df_redshift = redshift_table(redshift_user, redshift_password)
    df_sql = sql_table(mysql_user, mysql_password)
    
    #####For debugging. 
    #df_redshift.to_csv('df_redshift.csv', index=False)
    #data = pd.read_csv('df_redshift.csv')
    
    # merge data
    df = df_redshift.merge(df_sql, on = 'primary_isbn13', how = 'left')    

    # upload data to Airflow 
    uploadFile_to_airflow(df)
    time.sleep(30)

    # Mongo Stag
    client_staging = MongoClient('mongodb://{}:{}@ec2-52-91-106-189.compute-1.amazonaws.com:27017/edison-nye'.format(mongodb_username_staging, mongodb_password_staging))  
    # Mongo Prod
    client_production = MongoClient('mongodb://{}:{}@10.0.1.227:27017,10.0.1.228:27017,10.0.1.229:27017/edison-nye?replicaSet=orion-rs'.format(mongodb_username_staging, mongodb_password_staging))

    # upload data to s3
    if processFile() == 1:
    	updateMongo(client_staging)
        logging.info("Finish Upload to Mongo Stagging")
        time.sleep(60)
        updateMongo(client_production)
        logging.info("Finish Upload to Mongo Production")
    	uploadFile_to_S3()
        logging.info("Finish Upload to S3")
        # logging.info("Start updating campaign data to Mongo Staging")
        # campaignData(client_staging)
        # logging.info("Finish updating campaign data to Mongo Staging")
        # logging.info("Start updating campaign data to Mongo Production")
        # campaignData(client_production)
        # logging.info("Finish updating campaign data to Mongo Production")
####################################################################################################################################
# Convert dag start date from local timezone to utc
dag_start_date_utc = datetime.strptime('2018-10-31', '%Y-%m-%d')

# Define DAG
args = {
        'owner': 'ytian',
        'start_date': dag_start_date_utc,
        #'email':['ytian@openroadmedia.com', 'zsong@openroadmedia.com', 'xgu@openroadmedia.com'],
        'email':emaillist.split(','),
        'email_on_failure': True,
        'email_on_retry': True,
        'catchup': False
        }

dag = DAG(
        dag_id='ebb_nomination_tool_v3',
        default_args=args,
        catchup=False,
        schedule_interval='0 10 * * *' 
        )
####################################################################################################################################
fetch_ebb_nomination=PythonOperator(task_id='fetch_ebb_nomination',
                                    provide_context=True,
                                    python_callable = fetch_ebb_nomination,
                                    xcom_push=True,
                                    dag=dag)

fetch_ebb_nomination
