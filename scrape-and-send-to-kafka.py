import sys

from utils.funcs import *

print('Loaded custom functions from utils.funcs')


KAFKA_HOST = 'localhost:9092'

KEYWORD = str(sys.argv[1])
PAGES = int(sys.argv[2])
TOPIC_NAME = str(sys.argv[3])
VERBOSE = bool(int(sys.argv[4]))

print(f'''Arguments:
                    KEYWORD: {KEYWORD}
                    PAGES: {str(PAGES)}
                    TOPIC_NAME: {TOPIC_NAME}
                    VERBOSE: {str(VERBOSE)}''')


browser = get_browser(driver_path=r'chromedriver/chromedriver.exe', headless=False)
print('Started browser driver')

producer = KafkaProducer(bootstrap_servers=KAFKA_HOST,
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
print('Initiated a Kafka Producer instance')


if __name__ == '__main__':
    print(f'Ready to scrape jobs by keyword: "{KEYWORD}" ({str(PAGES)} page(s))')
    try:
        scrape_HH_to_kafka(browser, producer, keyword=KEYWORD, pages2scrape=PAGES, topic_name=TOPIC_NAME, verbose=VERBOSE)
        producer.close()
        print('Job done!')
        print('Closed Kafka producer!')
        browser.quit()
        print('Closed browser!')

    
    except Exception as ex:
        print(str(ex))
        producer.close()
        browser.quit()

