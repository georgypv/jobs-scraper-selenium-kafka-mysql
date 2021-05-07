import sys

from utils.funcs import *
print('Imported custom functions from utils.funcs')

KAFKA_HOST = 'localhost:9092'

TOPIC_NAME = str(sys.argv[1])
TABLE_NAME = str(sys.argv[2])
VERBOSE = bool(int(sys.argv[3]))


print(f'''Arguments:
                    TOPIC_NAME: {TOPIC_NAME}
                    TABLE_NAME: {TABLE_NAME}
                    VERBOSE: {str(VERBOSE)}''')



mysql_con = 'mysql+pymysql://python_user:123@127.0.0.1:3306/webscraping?charset=utf8mb4'
consumer = KafkaConsumer(
    TOPIC_NAME, 
    bootstrap_servers=KAFKA_HOST,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print('Initiated Kafka Consumer instance!')

engine = sa.create_engine(mysql_con)

print('Created MySQL engine!')


if __name__ == '__main__':
    
    try:
        consume_and_insert(consumer, engine, table_name=TABLE_NAME, verbose=VERBOSE)
    except Exception as ex:
        consumer.close()
        print('Closed consumer!')
        print(str(ex))
        