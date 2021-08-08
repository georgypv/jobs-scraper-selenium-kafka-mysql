import sys

from jobs_writer import JobsWriter
from utils.kafka_consumer_and_producer import kafka_consumer
from myconfigs import MYSQL_TABLE_NAME, MYSQL_SCHEMA

VERBOSE = bool(int(sys.argv[1]))


print(f'''Arguments:
                    VERBOSE: {str(VERBOSE)}''')



if __name__ == '__main__':
    writer = JobsWriter(kafka_consumer=kafka_consumer, table_name=MYSQL_TABLE_NAME, schema=MYSQL_SCHEMA)
    writer.consume_and_insert(batch_insert=True, batch_size=30, verbose=VERBOSE)
        