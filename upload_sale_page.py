# -*- coding: utf-8 -*-

#%%################################Package Used#%%################################

import os
from time import sleep
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

# Airflow module
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

# selenium module
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

#%%################################Env Setup#%%################################

with open(os.environ['HOME']+'/env.txt') as f:
  env =dict([line.rstrip('\n').split('=') for line in f])
locals().update(env)

url = 'https://earlybirdbooks.com/admin/posts/edit/deals/5daf0f78809053001bd0da2b'
file = os.environ['HOME'] + '/airflow_dags_results/update_ebb_sales_page/ebb_daily_sale.csv'

#%%################################Functions#%%################################

def check_web_load(xpath,timeout):
    try:
        element_present = EC.presence_of_element_located((By.XPATH, xpath))
        WebDriverWait(driver, timeout).until(element_present)
    except TimeoutException:
        print "Timed out waiting for page to load"  

#%%#################################Main#################################
        
def ebb_daily_sale(ds, **kwargs):       
    engine = create_engine('mysql+mysqlconnector://%s:%s@orim-internal-db01.cqbltkaqn0z7.us-east-1.rds.amazonaws.com/openroad_internal'%(mysql_user, mysql_password), echo=False)
    conn = engine.connect()

    query = '''
            SELECT 
                campaign, start_date, end_date, primaryisbn13, promo_price
            FROM
                title_campaign
            WHERE
                campaign LIKE 'ORM%Site'
                    OR campaign LIKE 'ORM%NL'
                    AND country = 'US'
                    AND primaryisbn13 != 'nan'
            GROUP BY campaign , start_date , end_date , primaryisbn13
            HAVING COUNT(DISTINCT retailer) = 5
                AND CURDATE() BETWEEN start_date AND end_date
            '''
    
    df_raw = pd.read_sql_query(query, conn)
    
    df_raw['header'] = np.nan
    df_raw['description'] = np.nan
    df_raw.rename(columns={'primaryisbn13': 'ISBN', 'promo_price': 'salePrice'}, inplace=True)
    
    df = df_raw[['ISBN','header', 'description','salePrice']]
    df.reset_index(drop = True,inplace = True)
    df.to_csv(file,index = False)
    
def upload_to_orion(ds, **kwargs):
    # Open chrome driver
    global driver
    timeout = 30
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--lang=en");
    options.add_argument("--user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36'")
    prefs = {"profile.default_content_setting_values.automatic_downloads": 1,
             "download.directory_upgrade": True,
             "safebrowsing.enabled": False,
             "safebrowsing.disable_download_protection": True,
             "intl.accept_languages": "en,en_US"}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(os.environ['HOME'] + '/chromedriver', chrome_options=options)
    #driver = webdriver.Chrome(os.environ['HOME']+'/Desktop/DataWorks/Driver&Modules/chromedriver', chrome_options=options)
    download_dir = os.environ['HOME']
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    command_result = driver.execute("send_command", params)
    
    driver.get(url)
    
    check_web_load("//input[@name='username']",timeout)
    
    # type username
    input_username = driver.find_elements_by_xpath("//input[@name='username']")
    ActionChains(driver).move_to_element(input_username[0]).click().send_keys(orion_username).perform()
    
    # type password
    input_password = driver.find_elements_by_xpath("//input[@name='password']")
    ActionChains(driver).move_to_element(input_password[0]).click().send_keys(orion_password).perform()
    
    login_button = driver.find_element_by_xpath("//button[text()='Login']")
    ActionChains(driver).move_to_element(login_button).click().perform()
    
    check_web_load("//button[text()='Add New item']",timeout)
    
    driver.get(url)
    
    check_web_load("//button[@id='addbutton']",timeout)
    
    # slove the overlapping issue
    add_button = driver.find_element_by_xpath("//button[@id='addbutton']")
    driver.execute_script("arguments[0].style.visibility='hidden'", add_button)
    
    remove_button = driver.find_element_by_xpath("//a[@class='button general float-right']")
    ActionChains(driver).move_to_element(remove_button).click().perform()
    
    driver.execute_script("arguments[0].style.visibility='visible'", add_button)
    ActionChains(driver).move_to_element(add_button).click().perform()
    driver.execute_script("arguments[0].style.visibility='hidden'", add_button)
    
    add_batch_button = driver.find_element_by_xpath("//a[text()='Book Info Batch']")
    ActionChains(driver).move_to_element(add_batch_button).click().perform()
    
    check_web_load("//a[@class ='button general remove-btn']",timeout)
    
    button = driver.find_element_by_xpath("//a[@class ='button general remove-btn']")
    ActionChains(driver).move_to_element(button).click().perform()
    
    check_web_load('//input[@type="file" and @accept = ".csv"]',timeout)
    
    driver.find_element_by_xpath('//input[@type="file" and @accept = ".csv"]').send_keys(file)
    
    check_web_load("//label[text()='Amazon Link']",timeout)
    
    save_button = driver.find_element_by_xpath("//button[@class='button publish' and text() = 'Save']")
    ActionChains(driver).move_to_element(save_button).click().perform()
    
    sleep(20)
    driver.quit()

#%%################################Airflow Setup#%%################################
    
# Convert dag start date from local timezone to utc
dag_start_date_utc = datetime.strptime('2019-10-21', '%Y-%m-%d')

# Define DAG
args = {
        'owner': 'yliu',
        'start_date': dag_start_date_utc,
        #'email':emaillist.split(','),
        'email':['yuzhouboat@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': True,
        'catchup': False
        }

dag = DAG(
        dag_id='update_ebb_sales_page_v1',
        default_args=args,
        catchup=False,
        schedule_interval='0 1 * * *')

ebb_daily_sale = PythonOperator(
        task_id = 'ebb_daily_sale',
        provide_context=True,
        python_callable = ebb_daily_sale,
        dag = dag)

upload_to_orion = PythonOperator(
        task_id = 'upload_to_orion',
        provide_context=True,
        python_callable = upload_to_orion,
        dag = dag)

ebb_daily_sale >> upload_to_orion
