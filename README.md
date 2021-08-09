# Scrape job descriptions from hh.ru with Selenium, process, send data to Kafka and store it in MySQL

## Basic steps:

1. Create a database and a table in MySQL
2. Run Kafka on your machine (a laptop with Windows 10 WSL2 in my case)
3. Scrape job descriptions and other information from hh.ru with Selenium (using Chrome driver) and send them to a Kafka topic via a KafkaProducer instance
4. Instantiate a KafkaConsumer, read a json with job information from the Kafka topic and insert data into a MySQL table 


To run the script:

0. Make sure that you've created a database and a table like showed in `SQL/create-table-HH-vacancies.sql`
1. Run Zookeeper and Kafka in the background
2. (In one console) run `python scrape-and-send-to-kafka.py [KEYWORD TO SERACH JOBS] [NUMBER OF PAGES TO SCRAPE] [VERBOSE (0 OR 1)]`
3. (In another console) run `python read-kafka-and-insert.py [VERBOSE (0 OR 1)]`

For example:

```
python scrape-and-send-to-kafka.py 'Data Analyst' 10 1

python read-kafka-and-insert.py 0
```
