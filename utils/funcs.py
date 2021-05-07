import uuid
import json
import random
import os
import sys
from time import sleep
from datetime import datetime
import requests as rt
import numpy as np


from kafka import KafkaProducer, KafkaConsumer


from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException,ElementNotInteractableException, ElementClickInterceptedException


import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


def get_browser(driver_path=r'chromedriver/chromedriver.exe', headless=False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('headless')
    options.add_argument('window-size=1200x600')
    browser = webdriver.Chrome(driver_path, options=options)
    return browser


def get_vacancies_on_page(browser):    
    #close pop-up window with suggested region (if present)
    try:
        browser.find_element_by_class_name('bloko-icon_cancel').click()
    except (NoSuchElementException, ElementNotInteractableException):
        pass
    
    vacancy_cards = browser.find_elements_by_class_name('vacancy-serp-item ')
    
    return vacancy_cards
    
    

def get_vacancy_info(card, browser, keyword, verbose=True):
    
    try:
        card.find_element_by_class_name('vacancy-serp-item__info')\
            .find_element_by_tag_name('a')\
            .send_keys(Keys.CONTROL + Keys.RETURN) #open new tab in Chrome

        sleep(2) #let it fully load
        #go to the last opened tab
        browser.switch_to.window(browser.window_handles[-1])
        
        basic_info = False
        while not basic_info:
            try:
                vacancy_title = browser.find_element_by_xpath('//div[@class="vacancy-title"]//h1').text
                company_name = browser.find_element_by_xpath('//a[@class="vacancy-company-name"]').text
                company_href_hh = browser.find_element_by_xpath('//a[@class="vacancy-company-name"]').get_attribute('href')
                publish_time = browser.find_element_by_xpath('//p[@class="vacancy-creation-time"]').text
                basic_info = True
            except:
                sleep(3)
        
        if verbose:
            print("Title: ", vacancy_title )
            print("Company: ", company_name )
            print("Company link: ", company_href_hh )
            print("Publish time: ", publish_time )

        try:
            salary = browser.find_element_by_xpath('//div[@class="vacancy-title"]//p[@class="vacancy-salary"]').text
        except NoSuchElementException :
            salary = 'не указано'
            
        
        try:
            emp_mode = browser.find_element_by_xpath('//p[@data-qa="vacancy-view-employment-mode"]').text
        except NoSuchElementException :
            emp_mode = 'не указано'
        finally:
            emp_mode = emp_mode.strip().replace('\n', ' ')
        
            
        try:
            exp = browser.find_element_by_xpath('//span[@data-qa="vacancy-experience"]').text
        except NoSuchElementException :
            exp = 'не указано'
        finally: 
            exp = exp.strip().replace('\n', ' ')
        
        try:
            company_address = browser.find_element_by_xpath('//span[@data-qa="vacancy-view-raw-address"]').text
        except NoSuchElementException:
            company_address = 'не указано'
            
        try:
            vacancy_description = browser.find_element_by_xpath('//div[@data-qa="vacancy-description"]').text
        except NoSuchElementException:
            vacancy_description = 'не указано'
        finally:
            vacancy_description = vacancy_description.replace('\n', ' ')
            
        try:
            vacancy_tags = browser.find_element_by_xpath('//div[@class="bloko-tag-list"]').text
        except NoSuchElementException:
            vacancy_tags = 'не указано'
        finally:
            vacancy_tags = vacancy_tags.replace('\n', ', ')
            
        if verbose:    
            print("Salary: ", salary )
            print("Company address: ", company_address )
            print('Experience: ', exp)
            print('Employment mode: ', emp_mode)
            print("Vacancy description: ", vacancy_description[:50] )
            print("Vacancy tags: ", vacancy_tags)

        browser.close() #close tab
        browser.switch_to.window(browser.window_handles[0]) #switch to the first tab
        
        dt = str(datetime.now())
        
        vacancy_info = {'dt': dt,
                        'keyword': keyword,
                        'vacancy_title': vacancy_title,
                       'vacancy_salary': salary,
                       'vacancy_tags': vacancy_tags,
                       'vacancy_description': vacancy_description,
                        'vacancy_experience' : exp,
                        'employment_mode': emp_mode,
                       'company_name':company_name,
                       'company_link':company_href_hh,
                       'company_address':company_address,
                       'publish_place_and_time':publish_time}
        
        return vacancy_info        

    except Exception as ex:
        print('Exeption while scraping info!')
        print(str(ex))
        return None
    
    
def send_message(producer, topic_name, msg):
    msg_id = str(uuid.uuid4())
    msg['uid'] = msg_id 
    producer.send(topic_name, msg)
    producer.flush()    
    print(f'PRODUCER: Sent message with id: {msg_id}')
    
    sleep(0.5)
    

    
    
def scrape_HH_to_kafka(browser, producer, keyword='Python', pages2scrape=3, topic_name='parsed-jobs', verbose=True):
    
    try:
    
        url = f'https://hh.ru/search/vacancy?area=1&fromSearchLine=true&st=searchVacancy&text={keyword}&from=suggest_post'
        browser.get(url)
        while pages2scrape > 0:
            vacancy_cards = get_vacancies_on_page(browser=browser)
            for card in vacancy_cards:
                vacancy_info = get_vacancy_info(card, browser=browser, keyword=keyword, verbose=verbose)          
                #sending scraping results to kafka
                send_message(producer, topic_name=topic_name, msg=vacancy_info)

            try:
                #click to the "Next" button to load other vacancies
                browser.find_element_by_xpath('//a[@data-qa="pager-next"]').click()
                   
            except (NoSuchElementException, ElementNotInteractableException):
                break
            finally:
                pages2scrape -= 1
                if pages2scrape > 0:
                     print('Go to the next page')
    
    except KeyboardInterrupt:    
        browser.quit()
        producer.close()

    
def insert_data(data, engine, table_name='HH_vacancies', schema='webscraping', verbose=True): 
    metadata = sa.MetaData(bind=engine)
    table = sa.Table(table_name, metadata, autoload=True, schema=schema)
    con = engine.connect()
    try:
        con.execute(table.insert().values(data))
        if verbose:
            print('Data inserted into table {}'.format(table_name))
    except Exception as ex:
        print('Exception while inserting data!')
        print(str(ex))
    finally:    
        con.close()

        
def consume_and_insert(consumer, engine, table_name, verbose=True):
    try:
        for msg in consumer:
            message = msg.value
            if verbose:
                print(f"CONSUMER: Consumed message with id: {message['uid']}!")
                
            insert_data(message, engine, table_name=table_name, verbose=verbose)
    except KeyboardInterrupt:
        consumer.close() 
        print("Closed consumer!")