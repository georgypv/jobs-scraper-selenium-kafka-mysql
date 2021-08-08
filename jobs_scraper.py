from time import sleep
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (NoSuchElementException,
                                        ElementNotInteractableException,
                                        )
from utils.kafka_consumer_and_producer import kafka_producer, send_to_kafka

class JobScraper:

    def __init__(self, driver_path, headless,  keyword, to_kafka=True):

        self.driver_path = driver_path
        self.headless = headless
        self.base_url = 'https://hh.ru/search/vacancy?area=1&fromSearchLine=true&st=searchVacancy&text={}&from=suggest_post'

        self.keyword = self._get_keyword(keyword)
        self.browser = self._get_browser()

        self.pages2scrape = None
        self.pages_left = None
        self.end_of_jobs = False

        self.jobs_info = []

        self.to_kafka = to_kafka
        if to_kafka:
            self.kafka_producer = kafka_producer
        else:
            self.kafka_producer = None
        self.end_scraping = False
    def _get_keyword(self, keyword):
        if len(keyword.split()) > 1:
            return "+".join(keyword.split())
        else:
            return keyword


    def _get_browser(self):
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('headless')
        options.add_argument('window-size=1200x600')
        browser = webdriver.Chrome(self.driver_path, options=options)
        print('Started browser driver')
        return browser.get(self.base_url.format(self.keyword))


    def get_jobs_on_page(self):
        #close pop-up window with suggested region (if present)
        try:
            self.browser.find_element_by_class_name('bloko-icon_cancel').click()
        except (NoSuchElementException, ElementNotInteractableException):
            pass
        job_cards = self.browser.find_elements_by_class_name('vacancy-serp-item ')
        return job_cards


    def get_job_info(self, card, verbose=True):

        try:
            card.find_element_by_class_name('vacancy-serp-item__info')\
                .find_element_by_tag_name('a')\
                .send_keys(Keys.CONTROL + Keys.RETURN) #open new tab in Chrome

            sleep(2) #let it fully load
            #go to the last opened tab
            self.browser.switch_to.window(self.browser.window_handles[-1])

            basic_info = False
            while not basic_info:
                try:
                    vacancy_title = self.browser.find_element_by_xpath('//div[@class="vacancy-title"]//h1').text
                    company_name = self.browser.find_element_by_xpath('//a[@class="vacancy-company-name"]').text
                    company_href_hh = self.browser.find_element_by_xpath('//a[@class="vacancy-company-name"]').get_attribute('href')
                    publish_time = self.browser.find_element_by_xpath('//p[@class="vacancy-creation-time"]').text
                    basic_info = True
                except:
                    sleep(3)

            if verbose:
                print("Title: ", vacancy_title )
                print("Company: ", company_name )
                print("Company link: ", company_href_hh )
                print("Publish time: ", publish_time )

            try:
                salary = self.browser.find_element_by_xpath('//div[@class="vacancy-title"]//p[@class="vacancy-salary"]').text
            except NoSuchElementException:
                salary = 'не указано'

            try:
                emp_mode = self.browser.find_element_by_xpath('//p[@data-qa="vacancy-view-employment-mode"]').text
            except NoSuchElementException :
                emp_mode = 'не указано'
            finally:
                emp_mode = emp_mode.strip().replace('\n', ' ')

            try:
                exp = self.browser.find_element_by_xpath('//span[@data-qa="vacancy-experience"]').text
            except NoSuchElementException :
                exp = 'не указано'
            finally:
                exp = exp.strip().replace('\n', ' ')

            try:
                company_address = self.browser.find_element_by_xpath('//span[@data-qa="vacancy-view-raw-address"]').text
            except NoSuchElementException:
                company_address = 'не указано'

            try:
                vacancy_description = self.browser.find_element_by_xpath('//div[@data-qa="vacancy-description"]').text
            except NoSuchElementException:
                vacancy_description = 'не указано'
            finally:
                vacancy_description = vacancy_description.replace('\n', ' ')

            try:
                vacancy_tags = self.browser.find_element_by_xpath('//div[@class="bloko-tag-list"]').text
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

            self.browser.close() #close tab
            self.browser.switch_to.window(self.browser.window_handles[0]) #switch to the first tab

            dt = str(datetime.now())

            vacancy_info = {'dt': dt,
                            'keyword': self.keyword,
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
            print('Exception while scraping info!')
            print(str(ex))
            return


    def start_scraping(self, pages, verbose=True):
        self.pages2scrape = pages
        self.pages_left = pages

        try:
            job_cards = self.get_jobs_on_page()
        except (NoSuchElementException, ElementNotInteractableException) as ex:
            raise ex

        if len(job_cards) == 0:
            print("Вакансий для запроса '{}' не найдено!".format(self.keyword))
            self.end_scraping = True
            return

        try:
            while self.pages_left > 0:
                for card in job_cards:
                    job_info = self.get_job_info(card, verbose=verbose)
                    self.jobs_info.append(job_info)

                    if self.to_kafka:
                        send_to_kafka(self.kafka_producer, job_info)

                try:
                    # click to the "Next" button to load other vacancies
                    self.browser.find_element_by_xpath('//a[@data-qa="pager-next"]').click()
                except (NoSuchElementException, ElementNotInteractableException):
                    print("Больше нет вакансий по запросу: '{}'".format(self.keyword))
                    self.end_of_jobs = True
                    self.end_scraping = True
                    break
                finally:
                    self.pages_left -= 1
                    if self.pages_left > 0:
                        print('Переходим на следующую страницу')
                    else:
                        print("Больше нет вакансий по запросу: '{}'".format(self.keyword))
                        print("Закрываем браузер!")
                        self.browser.quit()

        except KeyboardInterrupt:
            print("Прерываем парсинг и закрываем браузер!")
            self.browser.quit()



