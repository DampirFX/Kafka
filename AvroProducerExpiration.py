from kafka import KafkaProducer
from kafka.errors import KafkaError
import AvroUtils

#создание экземпляра авро
pathDll = './AvroUtils.dll'
pathSchema = './avro_MT_templates/EXPIRATION_START.avsc'
avroUtils = AvroUtils.JsonSerializer(pathDll)


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
#Имя топика
topic = 'expiration-service--symbolExpiration'

#исходное сообщение
message ='{"symbol": "TestA","dateTime": "2019-11-06T03:27:00","expirationId": 1,"lastContract": "TestA9V","newContract": "TestA9X"}'
print('Json: ' + message)
key = '1234'.encode()

avroMessage = avroUtils.toAvroBinaryBase64(pathSchema,message.encode())
print('Avro: ' + avroMessage.decode())

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

Send_message(topic,avroMessage,key)