import sys
from jobs_scraper import JobScraper
from myconfigs import DRIVER_PATH, HEADLESS


KEYWORD = str(sys.argv[1])
PAGES = int(sys.argv[2])
VERBOSE = bool(int(sys.argv[3]))

print(f'''Arguments:
                    KEYWORD: {KEYWORD}
                    PAGES: {str(PAGES)}
                    VERBOSE: {str(VERBOSE)}''')




if __name__ == '__main__':

    try:
        scraper = JobScraper(driver_path=DRIVER_PATH, headless=HEADLESS, keyword=KEYWORD)
        scraper.start_scraping(pages=PAGES, verbose=VERBOSE)

    except Exception as ex:
        print(str(ex))
        scraper.kafka_producer.close()
        scraper.browser.quit()
    

    if (scraper.pages_left <= 0) and not scraper.end_of_jobs:

        good_response = False
        while not good_response:
            response = input("All {} pages have been scraped! Scrape more pages? (y/n)")
            if response.strip().lower() == 'y':
                good_number = False
                tries = 5
                while not good_number or tries > 0:
                    add_pages = input("How many pages?")
                    try:
                        add_pages = int(add_pages)
                        good_response = True
                        good_number = True
                    except ValueError:
                        print("Input must be a number!")
                        tries -= 1
            elif response.strip().lower() == 'n':
                good_response = True
                print('Close Kafka producer!')
                scraper.kafka_producer.close()
                print('Close browser!')
                scraper.browser.quit()
            else:
                print("Input must be 'y' or 'n'!")




