#!/usr/bin/env python2
# -*- coding: utf-8 -*-


# Airflow module
from __future__ import print_function
from builtins import range
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

# regular module
import os
import logging
import string
import os.path
import sys
import re
from cStringIO import StringIO

# time module
import datetime
from time import sleep

# selenium module
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

#load gmail 
import gmail
import pandas as pd
import os
from sqlalchemy import create_engine
import sqlalchemy


dag_start_date_utc = datetime.datetime.strptime('2018-07-19', '%Y-%m-%d')

# Define DAG
# Load env file
#os.chdir('/home/ec2-user')
with open(os.environ['HOME']+'/env.txt') as f:
  env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)

args = {
    'owner': 'xgu',
    'start_date': dag_start_date_utc,
    'email':emaillist.split(','),
    'email_on_failure': True,
    'email_on_retry': True,
    'catchup': False
}

dag = DAG(
    dag_id='bookbub_campaign_results_v3', 
    default_args=args,
    schedule_interval='0 1 * * *')


def request_bookbub_campaign_result_report():

	#driver = webdriver.Chrome()
	def enable_download_in_headless_chrome(driver, download_dir):
		# add missing support for chrome "send_command"  to selenium webdriver
		driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

		params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
		command_result = driver.execute("send_command", params)  

	
	options = webdriver.ChromeOptions()
	options.add_argument("--start-maximized")
	options.add_argument("--headless")
	options.add_argument("--no-sandbox")         
	options.add_argument("--window-size=1920x1080")
	options.add_argument("--lang=en");
	options.add_argument("--user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'")
	prefs = {"profile.default_content_setting_values.automatic_downloads": 1,
			 "download.default_directory": r"/home/ec2-user/airflow_dags_results/bookbub_campaign_results/",
			 "download.directory_upgrade": True,
			 "safebrowsing.enabled": False,
			 "safebrowsing.disable_download_protection": True,
			 "intl.accept_languages": "en,en_US"}

	options.add_experimental_option("prefs", prefs)
	driver = webdriver.Chrome('/home/ec2-user/chromedriver', chrome_options=options)
	enable_download_in_headless_chrome(driver, download_dir='/home/ec2-user/airflow_dags_results/bookbub_campaign_results/')


	driver.get("http://partners.bookbub.com")
	# type username
	input_username = driver.find_elements_by_name('user[email_address]')
	ActionChains(driver).move_to_element(input_username[0]).click().send_keys(marketing_mail_username).perform()
	sleep(1)

	# type password
	input_password = driver.find_elements_by_name('user[password]')
	ActionChains(driver).move_to_element(input_password[0]).click().send_keys(marketing_mail_password).perform()
	sleep(1)

	# click login button
	login_button = driver.find_element_by_xpath("//form[@id='partner-sign-in']/div[@class = 'submit']/button[@type='button']")
	ActionChains(driver).move_to_element(login_button).click().perform()
	sleep(1)


	# click Featured Deals button
	featured_deals = driver.find_element_by_link_text('Featured Deals')
	ActionChains(driver).move_to_element(featured_deals).click().perform()
	sleep(1)

	# click Extract report button
	report_button = driver.find_element_by_xpath('//button[@class="csv-dropdown haml-bub-button primary dropdown-toggle"]')
	ActionChains(driver).move_to_element(report_button).click().perform()
	sleep(1)
        
	# click Campaign Results CSV
	campaign_results = driver.find_element_by_link_text('Campaign Results CSV')
	ActionChains(driver).move_to_element(campaign_results).click().perform()
	
	driver.quit()
	# wait till bookbub send the report 
	sleep(900)
        

def download_format_ingest_bookbub_report():

	os.chdir('/home/ec2-user')
	
	# Login
	g_username = gmail_user_openroadintegrated
	g_password = gmail_password_openroadintegrated
	g = gmail.login(g_username, g_password)
	g.logged_in


	# Get mails from BookBub and download the campaign results csv attachement
	today = datetime.date.today().strftime("%Y-%m-%d")
	yesterday = (datetime.date.today() +datetime.timedelta(days = -1)).strftime("%Y-%m-%d")
	
	logging.info("Today: {}".format(today))
	logging.info("Yesterday: {}".format(yesterday))
	
	subject = "FW: BookBub Featured Deal Results CSV ({})".format(yesterday)
	logging.info("Email Subject: {}".format(subject))
	emails = g.inbox().mail(unread=True,subject = subject,sender='marketing@openroadmedia.com',after=(datetime.date.today() + datetime.timedelta(days=-3)))

	timer = 0
	while len(emails) ==0 and timer <= 300:
		sleep(60)
		timer =+ 60
		emails = g.inbox().mail(unread=True,subject = subject,sender='marketing@openroadmedia.com',after=(datetime.date.today() + datetime.timedelta(days=-3)))
	
	try:	
		email = emails[0]
	except:
		sys.exit("Email Subject: {} Not Found".format(subject))
		
	email.fetch()
	file = "BookbubFeaturedDealResults_" + email.subject[-11:-1] + ".csv"
	email.attachments[0].save('airflow_dags_results/bookbub_campaign_results/' + file)
	email.read()

	sleep(50)
	# Load csv files into pandas
	path = os.getcwd() + '/airflow_dags_results/bookbub_campaign_results/'
	#files = os.listdir(path)
	bookbub = pd.read_csv(path + file)


	# Ingest data to mysql
	engine = create_engine('mysql+mysqlconnector://'+ mysql_user +':'+ mysql_password +'@orim-internal-db01.cqbltkaqn0z7.us-east-1.rds.amazonaws.com/openroad_internal', echo=False)
	conn = engine.connect()


	isbn = pd.read_sql("select distinct primary_isbn13 primary_isbn13_check, isbn13 ISBNS from ingests_firebrand_pricing where primary_isbn13 <> '' and active = 1;", engine)
	title = pd.read_sql("select distinct title Title, primary_isbn13 from ingests_firebrand_pricing;", engine)
	asinlink = pd.read_sql("select distinct primary_isbn13 ISBN_check, asin from title_links_feed where asin <> '';", engine)

	bookbub['ISBNS'] =bookbub['ISBNS'].str.replace('=', '')
	bookbub['ISBNS'] =bookbub['ISBNS'].str.replace('"', '')
	bookbub_new = bookbub

	s = bookbub_new["ISBNS"].str.split(',').apply(pd.Series,1).stack()
	s.index = s.index.droplevel(-1)
	s.name = 'ISBNS'
	del bookbub_new['ISBNS']
	bookbub_new = bookbub_new.join(s)


	a = bookbub_new["ASINS"].str.split(',').apply(pd.Series,1).stack()
	a.index = a.index.get_level_values(0)
	a.name = 'ASINS'
	del bookbub_new['ASINS']
	bookbub_new = bookbub_new.join(a)
	bookbub_new = bookbub_new.drop_duplicates()
	
	bb_join_isbn = pd.merge(bookbub_new,isbn,on="ISBNS", how="left")
	bb_join_isbn = bb_join_isbn.rename(columns={'primary_isbn13_check': 'Primary ISBN'})
	bb_join_isbn = bb_join_isbn.drop_duplicates(subset = ['Campaign ID','Title','Author','Featured Date','Deal Cost','Primary ISBN'])

	if bb_join_isbn['Primary ISBN'].isnull().sum() > 0:
		bb_join_asin = pd.merge(bb_join_isbn,asinlink,left_on = 'ASINS', right_on = 'asin', how="left")
		bb_join_asin['Primary ISBN'].fillna(bb_join_asin.ISBN_check, inplace=True)
		bb_join_asin = bb_join_asin.drop(['ISBN_check','asin'],axis = 1)
		bb_join_asin = bb_join_asin.drop_duplicates(subset = ['Campaign ID','Title','Author','Featured Date','Deal Cost','Primary ISBN'])
	else:
		bb_join_asin = bb_join_isbn               

	if bb_join_asin['Primary ISBN'].isnull().sum() > 0:
		bb_join_title = pd.merge(bb_join_asin,title,on = 'Title', how="left")
		bb_join_title['Primary ISBN'].fillna(bb_join_title.primary_isbn13, inplace=True)
		bb_join_title = bb_join_title.drop(['primary_isbn13'],axis = 1)
	else:
		bb_join_title = bb_join_asin


	df = bb_join_title.drop_duplicates()
	df = df.drop(['ISBNS','ASINS'],axis = 1)
	df = df.drop_duplicates()
	df['Deal Cost'] = df['Deal Cost'].str.replace(',', '')
	df['Deal Cost'] = df['Deal Cost'].str.replace('$', '')
	df['Deal Cost'] = pd.to_numeric(df['Deal Cost'])
	df['End Date'] = pd.to_datetime(df['End Date'], format = '%m/%d/%Y',errors='coerce')
	df['Featured Date'] = pd.to_datetime(df['Featured Date'], format = '%m/%d/%Y',errors='ignore')
	df['Submitted Date'] = pd.to_datetime(df['Submitted Date'], format = '%m/%d/%Y',errors='ignore')




	df = df.rename(columns = {'Campaign ID':'campaign_id', 
							  'Title':'title',
							  'Author': 'author',
							  'Submitted Date':'submitted_date',
							  'Featured Date':'featured_date',
							  'End Date':'end_date',                 
							  'Category':'category',
							  'Deal Cost':'deal_cost',           
							  'State': 'state',          
							  'Deal Price (US)':'deal_price_us',
							  'Deal Price (UK)':'deal_price_uk',          
							  'Deal Price (CA)':'deal_price_ca',
							  'Deal Price (IN)':'deal_price_in',          
							  'Deal Price (AU)':'deal_price_au',
							  'Estimated US Subscribers':'estimated_us_subscribers', 
							  'Estimated UK Subscribers':'estimated_uk_subscribers',
							  'Estimated CA Subscribers':'estimated_ca_subscribers', 
							  'Estimated IN Subscribers':'estimated_in_subscribers',
							  'Estimated AU Subscribers':'estimated_au_subscribers',        
							  'Total Clicks (US)':'total_clicks_us',
							  'Total Clicks (UK)':'total_clicks_uk',        
							  'Total Clicks (CA)':'total_clicks_ca',
							  'Total Clicks (IN)':'total_clicks_in',  
							  'Total Clicks (AU)':'total_clicks_au', 
							  'Total Sales (US)': 'total_sales_us',         
							  'Total Sales (UK)': 'total_sales_uk',
							  'Total Sales (CA)': 'total_sales_ca',         
							  'Total Sales (IN)': 'total_sales_in',
							  'Total Sales (AU)': 'total_sales_au',                                
							  'Primary ISBN': 'isbn13'
						 })





	df = df.drop_duplicates()
	df = df.set_index(['isbn13','featured_date','deal_price_us'])
	df.to_csv('airflow_dags_results/bookbub_campaign_results/bookbub_ingested.csv')
	os.remove('airflow_dags_results/bookbub_campaign_results/'+file)

	# Ingest data to mysql
	df.to_sql(name='ingests_bookbub_report', con=engine, if_exists = 'replace', index=True,
			  dtype={
					 'campaign_id':sqlalchemy.types.BIGINT, 
					 'title':sqlalchemy.types.NVARCHAR(length=255), 
					 'author':sqlalchemy.types.NVARCHAR(length=255), 
					 'submitted_date':sqlalchemy.types.DATE,
					 'featured_date':sqlalchemy.types.DATE, 
					 'end_date':sqlalchemy.types.DATE, 
					 'category': sqlalchemy.types.NVARCHAR(length=255), 
					 'deal_cost': sqlalchemy.types.NVARCHAR(length=255), 
					 'state': sqlalchemy.types.NVARCHAR(length=255),
					 'deal_price_us': sqlalchemy.types.DECIMAL(10,2), 
					 'deal_price_uk': sqlalchemy.types.DECIMAL(10,2), 
					 'deal_price_ca': sqlalchemy.types.DECIMAL(10,2), 
					 'deal_price_in': sqlalchemy.types.DECIMAL(10,2),
					 'deal_price_au': sqlalchemy.types.DECIMAL(10,2), 
					 'estimated_us_subscribers': sqlalchemy.types.BIGINT,
					 'estimated_uk_subscribers': sqlalchemy.types.BIGINT, 
					 'estimated_ca_subscribers': sqlalchemy.types.BIGINT,
					 'estimated_in_subscribers': sqlalchemy.types.BIGINT, 
					 'estimated_au_subscribers': sqlalchemy.types.BIGINT,
					 'total_clicks_us': sqlalchemy.types.BIGINT, 
					 'total_clicks_uk': sqlalchemy.types.BIGINT, 
					 'total_clicks_ca': sqlalchemy.types.BIGINT,
					 'total_clicks_in': sqlalchemy.types.BIGINT, 
					 'total_clicks_au': sqlalchemy.types.BIGINT, 
					 'total_sales_us': sqlalchemy.types.DECIMAL(10,2),
					 'total_sales_uk': sqlalchemy.types.DECIMAL(10,2), 
					 'total_sales_ca': sqlalchemy.types.DECIMAL(10,2), 
					 'total_sales_in': sqlalchemy.types.DECIMAL(10,2),
					 'total_sales_au': sqlalchemy.types.DECIMAL(10,2), 
					 'isbn13':sqlalchemy.types.NVARCHAR(length=13)})
	conn.close()
					 


request_bookbub_campaign_result_report = PythonOperator(
    task_id='request_bookbub_campaign_result_report',
    python_callable = request_bookbub_campaign_result_report,
    dag=dag)

download_format_ingest_bookbub_report = PythonOperator(
        task_id = 'download_format_ingest_bookbub_report',
        python_callable = download_format_ingest_bookbub_report,
        dag = dag
        )

request_bookbub_campaign_result_report >> download_format_ingest_bookbub_report
