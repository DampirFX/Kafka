from kafka import KafkaProducer
from kafka.errors import KafkaError



#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
#Имя топика
topick = 'ACMSNewsMT'

message = b'{' \
          b'"symbol":"AAPL",' \
          b'"dateTime":"2019-08-28T02:55:18",' \
          b'"dividendId":1,' \
          b'"amount":1.23,' \
          b'"currency":"USD"' \
          b'}'

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

def Send_message(topick, message):
    producer = KafkaProducer(bootstrap_servers=[kafka])
    future = producer.send(topick, message).add_callback(on_send_success).add_errback(on_send_error)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

Send_message(topick,message)