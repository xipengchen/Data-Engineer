#!/usr/bin/env python2
# -*- coding: utf-8 -*-

####################################################################################################################################
# Airflow module
from __future__ import print_function#
from builtins import range
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

import os.path
from time import sleep
import re
from cStringIO import StringIO
import pandas as pd
from datetime import date, datetime, timedelta
import random, time
import string

import boto
from boto.s3.key import Key
import psycopg2
import gmail

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
####################################################################################################################################

# Define DAG
# Load env file
with open(os.environ['HOME']+'/env.txt') as f:
    env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)
dag_start_date_utc = datetime.strptime('2018-09-12', '%Y-%m-%d')
args = {
    'owner': 'xgu',
    'start_date': dag_start_date_utc,
    'email':emaillist.split(','),
    'email_on_failure': True,
    'email_on_retry': True,
    'catchup': False
}

dag = DAG(
    dag_id='ebb_list_user_email', 
    default_args=args,
    schedule_interval='0 20 * * *') #8pm UTC and 4pm ETC

####################################################################################################################################




def gmail_verify(g_sender):
	# function to get verfiy code

	# fetch verification code from gmail
	g_username = gmail_user_openroadintegrated
	g_password = gmail_password_openroadintegrated
	g = gmail.login(g_username, g_password)

	sleep(60)
	# get mails from goodreads and download the goodreads attachement
	emails = g.inbox().mail(sender = g_sender)

	# access the newest email
	emails[len(emails)-1].fetch()
	code_string = emails[len(emails)-1].body

	# access the email information
	res = re.search('Sailthru verification code: ', code_string).end()
	output_code = code_string[res:res+6] 
	print(output_code)
	return output_code

def enable_download_in_headless_chrome(driver, download_dir):
	# add missing support for chrome "send_command"  to selenium webdriver
	driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

	params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
	command_result = driver.execute("send_command", params)  

def login(url, username, password):
	# login to the sailthru webpage 
	global driver
	options = webdriver.ChromeOptions()
	options.add_argument("--start-maximized")
	options.add_argument("--headless")
	options.add_argument("--no-sandbox")        
	options.add_argument("--window-size=1920x1080")
	options.add_argument("--lang=en");
	options.add_argument("--user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'")
	prefs = {#"profile.default_content_settings.popups": 0,
			 "profile.default_content_setting_values.automatic_downloads": 1,
			 "download.default_directory": r"/home/ec2-user/airflow_dags_results/nl_list_user_email/",
			 "download.directory_upgrade": True,
			 "safebrowsing.enabled": False,
			 "safebrowsing.disable_download_protection": True,
			 "intl.accept_languages": "en,en_US"}

	options.add_experimental_option("prefs", prefs)
	driver = webdriver.Chrome('/home/ec2-user/chromedriver', chrome_options=options)

	enable_download_in_headless_chrome(driver, download_dir='/home/ec2-user/airflow_dags_results/nl_list_user_email/')

	driver.get(url)
	sleep(5)
	driver.set_page_load_timeout(30)

	# type username
	login_email = driver.find_elements_by_xpath("//div[@class='auth0-lock-input-block auth0-lock-input-email']")
	ActionChains(driver).move_to_element(login_email[0]).click().send_keys(username).perform()
	sleep(2)
	driver.save_screenshot('/home/ec2-user/airflow_dags_results/nl_list_user_email/username.png')

	# type password
	login_password = driver.find_elements_by_xpath("//div[@class='auth0-lock-input-show-password']")
	ActionChains(driver).move_to_element(login_password[0]).click().send_keys(password).perform()
	sleep(2)

	# click sumbit button
	sumbit_button = driver.find_elements_by_xpath("//button[@class='auth0-lock-submit']")
	ActionChains(driver).move_to_element(sumbit_button[0]).click().perform()
	sleep(10)
	driver.save_screenshot('/home/ec2-user/airflow_dags_results/nl_list_user_email/clicked_submit.png')
	
	# refresh to get rid of the popup window		
	driver.refresh()
	sleep(10)
	driver.save_screenshot('/home/ec2-user/airflow_dags_results/nl_list_user_email/clicked_submit_refreshed.png')
	


	# type login code
	login_code = driver.find_elements_by_xpath("//div[@class='auth0-lock-input-block auth0-lock-input-code']")
	driver.save_screenshot('/home/ec2-user/airflow_dags_results/nl_list_user_email/logincode.png')
	if len(login_code) > 0:
		# fetch verification code from gmail
		verify_code = gmail_verify(login_sender)
		ActionChains(driver).move_to_element(login_code[0]).click().send_keys(verify_code).perform()
		sleep(2)

		# click the login button
		login_button = driver.find_elements_by_xpath("//button[@class='auth0-lock-submit']")
		ActionChains(driver).move_to_element(login_button[0]).click().perform()
		sleep(5)
	

		

def download_csvfile(nl_url,url_download,path,name,rename):
	# function to download csvfile

	# switch account
	driver.get(nl_url)
	sleep(5)

	# access the download url
	driver.get(url_download)
	sleep(5)

	# click download csv button
	icon_button = driver.find_elements_by_xpath("//a[@title='Export your list as a CSV file']")
	ActionChains(driver).move_to_element(icon_button[0]).click().perform()
	sleep(2)

	# click select data button
	select_data = driver.find_elements_by_xpath("//input[@type='radio']")
	ActionChains(driver).move_to_element(select_data[0]).click().perform()
	sleep(2)

	# click all variable button
	driver.find_element_by_xpath('//select[@name="export_vars_type"]/option[text()="All Vars"]').click()
	sleep(2)

	# access unencrypted email button
	email_button = driver.find_elements_by_xpath('//*[@id="f_pii"]')
	ActionChains(driver).move_to_element(email_button[0]).click().perform()
	sleep(2)

	# access download csv file
	export_button = driver.find_elements_by_xpath('//input[@class="button"]')
	ActionChains(driver).move_to_element(export_button[1]).click().perform()
	sleep(10)

	# fetch verification code from gmail
	v_code = gmail_verify(download_sender)

	# type the verify code
	code = driver.find_elements_by_xpath('//*[@id="f_verify_code"]')
	ActionChains(driver).move_to_element(code[0]).click().send_keys(v_code).perform()
	driver.save_screenshot('/home/ec2-user/airflow_dags_results/nl_list_user_email/typeverifycode.png')
	sleep(2)

	
	# download file
	download_button = driver.find_elements_by_xpath("//input[@type='submit']")
	ActionChains(driver).move_to_element(download_button[1]).click().perform()
	sleep(5)

	#path = '/home/ec2-user/airflow_dags_results/nl_list_user_email/'
	#name = 'early_bird_books.csv'
	success = False

	timeout = time.time() + 90*60
	while not success:
	#print prompt.text
		if os.path.exists(path+name) == True:
			os.rename(path+name, path+rename)
			success = True
		else:
			time.sleep(60)
			if time.time() > timeout:
				time.sleep(60)
				break
		

def process_file(path,name,rename):
	# function to check whether file download or not

   # path = '/home/zhengdao/cronjob/sailthru/ebb_list_data_email/'
   # name = 'early_bird_books.csv'
   # rename = 'ebb_list_data_email.csv'

	if os.path.exists(path+name) == True:
		os.rename(path+name, path+rename)
		return 1
	else:
		return 0

def re_download_csvfile():
	# function to re dowload csv file

	# access click button
	click_button = driver.find_elements_by_xpath('//a[@href]')

	for c in click_button:
		if c.text.encode('UTF-8') == 'click here':
			ActionChains(driver).move_to_element(c).click().perform() 
 
def uploadFiletoS3(s3_bucket_name, s3_folder,s3_file,file_current):
	# function to upload files to amazon_s3

	#file = '/Users/intern/Desktop/AmzPOSAggregation_{}.csv'.format(input_date.strftime("%Y%m%d"))
	#file_current = 'ebb_list_data_email.csv'    
	#s3_file = 'ebb_list_data_email.csv'

	conn = boto.connect_s3(aws_access_key_id, aws_access_key)
	bucket = conn.get_bucket(s3_bucket_name, validate = False)

	with open(file_current,'rb') as d:
		temp_file = StringIO(d.read())
		temp_file.seek(0)
		# Upload to S3
		upload = Key(bucket)
		upload.key = '{0}/{1}'.format(s3_folder, s3_file)
		upload.set_contents_from_string(temp_file.getvalue())   

def uploadFiletoRedshift(list_data_emails_tb,s3_folder, s3_file):
	# function to upload files to redshift

	# Set up variables for Redshift API
	dbname = "sailthrudata"
	host = "sailthru-data.cmpnfzedptft.us-east-1.redshift.amazonaws.com"

	# Connect to RedShift
	conn_string = "dbname=%s port='5439' user=%s password=%s host=%s" % (dbname, redshift_user, redshift_password, host)
	#print "Connecting to database\n        ->%s" % (conn_string)
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()

	#creating redshift tables for the first time
	#cursor.execute("CREATE TABLE ebb_list_data_emails(profile_id varchar(255),Email varchar(255))")
	#conn.commit()

	sql = """truncate table {};""".format(list_data_emails_tb)
	cursor.execute(sql)
	conn.commit()


	print("ingesting to redshift")
	sql = """COPY {0} FROM 's3://sailthru-data/{1}/{2}' CREDENTIALS 'aws_iam_role=arn:aws:iam::822605674378:role/DataPipelineRole' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'YYYY-MM-DD HH24:MI:SS' EMPTYASNULL QUOTE '"' CSV REGION 'us-east-1';""".format(list_data_emails_tb,s3_folder,s3_file)
	cursor.execute(sql)
	conn.commit()
	print("done")

####################################################################################################################################
url = 'https://my.sailthru.com/login'


username = sailthru_user
password = sailthru_password

login_sender = '17329626280.19173830865.S1n6wlQLmS@txt.voice.google.com'
download_sender = '17329626280.13153143106.vFtPG753Tl@txt.voice.google.com'


s3_bucket_name = "sailthru-data"


####################################################################################################################################

def ebb_user_email_list_to_redshift():

	login(url, username, password)
	sleep(5)
	
	ebb_url = 'https://my.sailthru.com/?client=4735#pv/today'
	url_download = 'https://my.sailthru.com/lists#q%253Dearly%252520bird%252520books%2526start%253D0%2526view%253Dprimary'
	
	path = '/home/ec2-user/airflow_dags_results/nl_list_user_email/'
	name = 'early_bird_books.csv'
	rename = 'ebb_list_data_email.csv'
	
	s3_folder = "ebb_list_data_email"
	s3_file = 'ebb_list_data_email.csv'
	list_data_emails_tb = 'ebb_list_data_emails'
	
	download_csvfile(ebb_url,url_download,path,name,rename)


	file_current = path+rename

	cdf = pd.read_csv(file_current)
	df = cdf.iloc[:,0:2].copy()
	df.columns = ['profile_id','email']
	df.to_csv(file_current,index=False)

	uploadFiletoS3(s3_bucket_name, s3_folder,s3_file,file_current)
	uploadFiletoRedshift(list_data_emails_tb,s3_folder, s3_file)
	sleep(30)
	# Remove the file from the folder
	driver.quit()
	#os.remove('/home/zhengdao/cronjob/sailthru/ebb_list_data_email/ebb_list_data_email.csv')


ebb_user_email_list_to_redshift = PythonOperator(
        task_id = 'ebb_user_email_list_to_redshift',
        provide_context=False,
        python_callable = ebb_user_email_list_to_redshift,
        dag = dag
        )
        
ebb_user_email_list_to_redshift
