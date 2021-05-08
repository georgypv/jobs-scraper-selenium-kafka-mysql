# Scrape job descriptions with Selenium, process and store data with Kafka+MySQL

## Basic steps:

1. Create a database and a table in SQL (I chose MySQL)
2. Run Kafka on your machine (a laptop with Windows 10 WSL1 in my case)
3. Scrape job descriptions and other information from a certain website with Selenium (using Chrome driver) and send them to a Kafka topic via a KafkaProducer instance
4. Instantiate a KafkaConsumer, read job information from the Kafka topic and insert it into a MySQL table 


To run the script:

1. Run Zookeeper and Kafka in the background
2. Run `python scrape-and-send-to-kafka.py [KEYWORD TO SERACH JOBS] [NUMBER OF PAGES TO SCRAPE] [NAME OF KAFKA TOPIC] [VERBOSE (0 OR 1)]`
3. Run `python read-kafka-and-insert.py [NAME OF KAFKA TOPIC] [NAME OF SQL TABLE] [VERBOSE (0 OR 1)]`

For example:

`python scrape-and-send-to-kafka.py 'Data Analyst' 10 scraped-jobs 1

python read-kafka-and-insert.py scraped-jobs scraped-jobs-data 0
`
