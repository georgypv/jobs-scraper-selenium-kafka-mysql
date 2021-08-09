import uuid
import json
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from myconfigs import  KAFKA_TOPIC, KAFKA_HOST, GROUP_ID


def get_producer(kafka_host):
    producer = KafkaProducer(bootstrap_servers=kafka_host,
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    print('Initiated a Kafka Producer instance')
    return producer



def send_to_kafka(producer,  msg, topic_name=KAFKA_TOPIC):
    msg_id = str(uuid.uuid4())
    msg['uid'] = msg_id
    producer.send(topic_name, msg)
    producer.flush()
    sleep(0.1)


def get_consumer(kafka_topic, kafka_host, group_id):
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_HOST,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
        request_timeout_ms=3*60*1000,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    print('Initiated a Kafka Consumer instance')
    return consumer

kafka_producer = get_producer(KAFKA_HOST)

kafka_consumer = get_consumer(KAFKA_TOPIC, KAFKA_HOST, GROUP_ID)



