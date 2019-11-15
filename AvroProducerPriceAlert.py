from kafka import KafkaProducer
from kafka.errors import KafkaError
import AvroUtils

# messagetype = created = 1, deleted - 2
# kafka = dev или qa
messagetype = 1
kafka = 'dev'

#создание экземпляра авро
pathDll = './AvroUtils.dll'

#pathSchema = './avro_MT_templates/PA_CREATE.avsc'
#pathSchema = './avro_MT_templates/PA_DELETE.avsc'

if (messagetype == 1):
    pathSchema = './avro_MT_templates/PA_CREATE_S.avsc'
    headertype = 'created'
elif(messagetype == 2):
    pathSchema = './avro_MT_templates/PA_DELETE_S.avsc'
    headertype = 'deleted'

avroUtils = AvroUtils.JsonSerializer(pathDll)

#кластер кафки
if (kafka == 'qa'):
    kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
elif(kafka == 'dev'):
    kafka = '172.20.241.144:9092,172.20.244.128:9092,172.20.240.200:9092'
    #Имя топика
    topic = 'PriceAlert_DEV_in'


#исходное сообщение
#message ='{"priceAlert":{"extId":"testQQ4","extClientId":"testQQQ","alertLevel":180.0,"instrument":"ETHUSD","direction":"MORE_THAN","created":"2019-11-14T01:23:00"}}'

if (messagetype == 1):
    message ='{"extId":"testQQ15","extClientId":"testQQQ","instrument":"EURUSD","alertLevel":400.0,"direction":"LESS_THAN","created":"2019-11-14T01:23:00"}'
elif(messagetype == 2):
    message ='{"extId":"testQQ2","extClientId":"testQQQ2"}'


print('Json: ' + message)
#key = '1234'.encode()
key = ''.encode()

headers = [('BROKER', str.encode('BVI')), ('ENTITY_TYPE', str.encode('priceAlert')),
           ('ENTITY_STATUS', str.encode(headertype)), ('MESSAGE_PRODUCER', str.encode('MESSAGE_PRODUCERs'))]


#avroMessage = avroUtils.toAvroBinaryBase64(pathSchema,message.encode())
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