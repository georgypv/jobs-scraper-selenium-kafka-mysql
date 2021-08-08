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

    scraper = JobScraper(driver_path=DRIVER_PATH, headless=HEADLESS, keyword=KEYWORD)

    pages2scrape = PAGES
    while not scraper.end_scraping:
        scraper.start_scraping(pages=pages2scrape, verbose=VERBOSE)
        if (scraper.pages_left <= 0) and not scraper.end_of_jobs:

            good_response = False
            while not good_response:

                response = input("All {} pages have been scraped! Scrape more pages? (y/n)  ".format(scraper.pages2scrape))
                if response.strip().lower() == 'y':
                    add_pages = input("How many pages?  ")
                    pages2scrape = int(add_pages)
                    good_response = True
                elif response.strip().lower() == 'n':
                    good_response = True
                    scraper.end_scraping = True
                else:
                    print("Input must be 'y' or 'n'!")
        else:
            print('Close Kafka producer!')
            scraper.kafka_producer.close()
            print('Close browser!')
            scraper.browser.quit()
            break

    try:
        print('Close Kafka producer!')
        scraper.kafka_producer.close()
        print('Close browser!')
        scraper.browser.quit()
    except:
        pass



