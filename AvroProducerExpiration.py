from kafka import KafkaProducer
from kafka.errors import KafkaError
import AvroUtils
# status = Notice - 1, Start - 2
# kafka = dev или qa
messagetype = 2
kafka = 'qa'

#создание экземпляра авро
pathDll = './AvroUtils.dll'

if (messagetype == 1):
    pathSchema = './avro_MT_templates/EXPIRATION_START.avsc'
    status = 'Start'
elif(messagetype == 2):
    pathSchema = './avro_MT_templates/EXPIRATION_NOTICE.avsc'
    status = 'Notice'

avroUtils = AvroUtils.JsonSerializer(pathDll)


#кластер кафки
if (kafka == 'qa'):
    kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
    topic = 'expiration-service--symbolExpiration'
elif(kafka == 'dev'):
    kafka = '172.20.241.144:9092,172.20.244.128:9092,172.20.240.200:9092'
    topic = 'expiration-service--symbolExpiration'

#исходное сообщение
message ='{"symbol":"BRN","dateTime":"2019-11-25T09:17:00","expirationId":2226,"lastContract":"BRN0H","newContract":"BRN1C"}'

print('Json: ' + message)
#key = '1234'.encode()
key = ''.encode()

headers = [('MESSAGE_TYPE', str.encode('ENTITY_EVENT')), ('ENTITY_TYPE', str.encode('symbolExpiration')),
           ('ENTITY_STATUS', str.encode(status)), ('MESSAGE_PRODUCER', str.encode('ExpirationService'))]

avroMessage = avroUtils.toAvroBinary(pathSchema,message.encode())

#print('Avro: ' + avroMessage.decode())
print('Avro: ')
print(avroMessage)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

def Send_message(topic, message, key,headers):
    producer = KafkaProducer(bootstrap_servers=kafka)
    future = producer.send(topic, message, key,headers).add_callback(on_send_success).add_errback(on_send_error)

    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

Send_message(topic,avroMessage,key,headers)