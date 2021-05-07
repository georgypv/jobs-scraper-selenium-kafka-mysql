import sys

from utils.funcs import *


KAFKA_HOST = 'localhost:9092'

KEYWORD = sys.argv[1]
PAGES = sys.argv[2]
TOPIC_NAME = sys.argv[3]
VERBOSE = sys.argv[4]

browser = get_browser(driver_path=r'chromedriver/chromedriver.exe', headless=False)
producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

if __name__ == '__main__':
    scrape_HH_to_kafka(browser, producer, keyword=KEYWORD, pages2scrape=PAGES, topic_name=TOPIC_NAME, verbose=VERBOSE)

