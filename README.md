# Scrape job descriptions with Selenium, process and store data with Kafka+MySQL

## Basic steps:

1. Create a database and a table in SQL (I chose MySQL)
2. Run Kafka on your machine (a laptop with Windows 10 WSL1 in my case)
3. Scrape job descriptions and other information from a certain website with Selenium (using Chrome driver) and send them to a Kafka topic via a KafkaProducer instance
4. Instantiate a KafkaConsumer, read job information from the Kafka topic and insert it into a MySQL table 



How to run the script:

1. Run Zookeeper and Kafka in the console
2. Run `python scrape-and-send-to-kafka.py [KEYWORD TO SERACH JOBS] [NUMBER OF PAGES TO SCRAPE] [NAME OF KAFKA TOPIC] [VERBOSE (0 OR 1)]
