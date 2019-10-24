from kafka import KafkaProducer
from kafka.errors import KafkaError



#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
#Имя топика
topic = 'Test123'

message = b'{"Account":{"Comment":"","Country":"SOM","CurrencyCode":"USD","Group":"bbook_en","ID":"5451555","IsOnline":1,"LastActivityDate":"2019-09-19T06:57:17","LastConnectIP":"172.16.81.53","Leverage":200,"LockMarker":0,"Login":700018124,"MQID":"0","RegDate":"2011-07-01T11:23:13","SOAutochargeMarker":0},"ConnectionInfo":{"Address":"172.16.81.53","ConnectionType":"USER_TYPE_CLIENT"},"EventTimeStamp":"2019-09-19T06:16:29.260","headers":{"ACCOUNT":"700018124","ACCOUNT_TYPE":"MT4_INSTANT","BROKER":"BVI","CLIENT_ID":"5451555","Content-Type":"application/json; charset=UTF-8","ENTITY_ACTION":"MT4_ACCOUNT_LOGIN","ENTITY_STATUS":"success","ENTITY_TYPE":"client","MESSAGE_CREATED":"2019-09-19T06:16:29.260","MESSAGE_FORMAT":"v1","MESSAGE_PRODUCER":"MTEventTransmitter","MESSAGE_PRODUCER_INSTANCE":"MT4_INSTANT_REAL1","MESSAGE_TYPE":"ENTITY_EVENT"}}'

key = '1234'.encode()
    # b'{' \
    #       b'"symbol":"AAPL",' \
    #       b'"dateTime":"2019-08-28T02:55:18",' \
    #       b'"dividendId":1,' \
    #       b'"amount":1.23,' \
    #       b'"currency":"USD"' \
    #       b'}'

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

def Send_message(topic, message, key):
    producer = KafkaProducer(bootstrap_servers=kafka)
    future = producer.send(topic, message, key).add_callback(on_send_success).add_errback(on_send_error)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

Send_message(topic,message,key)