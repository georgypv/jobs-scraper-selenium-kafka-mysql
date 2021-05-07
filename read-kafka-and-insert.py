import sys

from utils.funcs import *

KAFKA_HOST = 'localhost:9092'

TOPIC_NAME = sys.argv[1]
TABLE_NAME = sys.argv[2]
VERBOSE = sys.argv[3]



mysql_con = 'mysql+pymysql://python_user:123@127.0.0.1:3306/webscraping?charset=utf8mb4'
consumer = KafkaConsumer(
    TOPIC_NAME, 
    bootstrap_servers=KAFKA_HOST,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))


engine = sa.create_engine(mysql_con)


if __name__ == '__main__':

    consume_and_insert(consumer, engine, table_name=TABLE_NAME verbose=VERBOSE)