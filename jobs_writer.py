import sqlalchemy as sa
from myconfigs import  MYSQL_USER, MYSQL_PSWRD, MYSQL_HOST, MYSQL_PORT

class JobsWriter:

    def __init__(self, kafka_consumer, table_name, schema):

        self.kafka_consumer = kafka_consumer    

        self.table_name = table_name
        self.schema = schema

        self.mysql_con = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(MYSQL_USER, MYSQL_PSWRD, MYSQL_HOST, MYSQL_PORT, self.schema)
        self.mysql_engine = sa.create_engine(self.mysql_con)
        self.db_con = self._get_db_connection()
        
        
    def _get_db_connection(self):
        con = self.mysql_engine.connect()
        return con

    def insert_data(self, data, verbose=True):

        metadata = sa.MetaData(bind=self.mysql_engine)
        table = sa.Table(self.table_name, metadata, autoload=True, schema=self.schema)
        try:
            self.db_con.execute(table.insert().values(data))
            if verbose:
                print('Данные записаны в таблицу: {}'.format(self.table_name))
        except Exception as ex:
            print('Ошибка при записи в таблицу!')
            print(str(ex))

    def consume_and_insert(self, batch_insert=True, batch_size=30, verbose=True):
        try:
            batch = []
            for msg in self.kafka_consumer:
                message = msg.value
                if verbose:
                    print(f"CONSUMER: прочел сообщение с id: {message['uid']}!")
                if batch_insert:
                    batch.append(message)
                    if len(batch) >= batch_size:
                        self.insert_data(batch, verbose=verbose)
                        batch = []
                else:
                    self.insert_data(message, verbose=verbose)
        except KeyboardInterrupt:
            if batch_insert and len(batch) > 0:
                self.insert_data(batch, verbose=verbose)
            self.kafka_consumer.close()
            print("Closed consumer!")
            self.db_con.close()
            print("Closed connection!")