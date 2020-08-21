from kafka import KafkaProducer
from kafka.errors import KafkaError
import AvroUtils
import AvroSchemaRegistry



#создание экземпляра авро
pathDll = './AvroUtils.dll'
avroUtils = AvroUtils.JsonSerializer(pathDll)


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
#Имя топика

topic = 'MTSourceReaderDevExpirationTopic'

message = '{"dateTime":"2020-05-19T07:13:00","expirationId":2134,"expirationRates":{"firstAsk":26,"firstBid":26,"firstDateTime":"2020-04-24T07:20:00","firstMid":26,"lastAsk":26,"lastBid":26,"lastDateTime":"2020-05-20T07:20:00","lastMid":26},"lastContract":"BRN0H",' \
          '"newContract":"BRN0U","sourceId":"LBX_FCIL","status":"Execute","symbol":"BRN"}'
#,"LastEventTimestamp" : "2020-03-03T12:38:07.000","headers" :{"ACCOUNT" : "995092501","BROKER" : "BVI","CLIENT_ID" : "","CONTENT_TYPE" : "application/json; charset=UTF-8","ENTITY_ACTION" : "MT4_MARGINCALL","ENTITY_STATUS" : "marginCall","ENTITY_TYPE" : "account","MESSAGE_CREATED" : "2020-03-03T09:38:59.000","MESSAGE_FORMAT" : "v1","MESSAGE_PRODUCER" : "MTEventTransmitter","MESSAGE_PRODUCER_INSTANCE" : "MT4_MARKET_REAL","MESSAGE_TYPE" : "ENTITY_EVENT"}
Url = 'https://avro-schemaregistry.qa-env.com/subjects/rmng--symbolExpiration-value/versions/latest/schema'
pathSchema = AvroSchemaRegistry.GetSchema(Url)
# pathSchema = './templates/Expiration_SymbolExpirationExecute.avsc'
print('Schema: ' + pathSchema)

#для укзекута
# topic = 'rmng--symbolExpiration'
# message = '{"dateTime":"2019-12-06T02:50:00","expirationId":2356,"expirationRates":{"firstAsk":2.443,"firstBid":2.442,"firstDateTime":"2019-12-06T02:50:04","firstMid":2.443,"lastAsk":2.443,"lastBid":2.442,"lastDateTime":"2019-12-06T02:50:04","lastMid":2.443},"lastContract":"NG0W","newContract":"NG0X","sourceId":"MT4_INSTANT_REAL1","status":"Execute","symbol":"NG"}'
# pathSchema = './templates/SymbolExpirationExecute.avsc'


#print('Json: ' + message)
#key = '1234'.encode()

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

def Send_message(topic, message):
    producer = KafkaProducer(bootstrap_servers=kafka)
    future = producer.send(topic, message).add_callback(on_send_success).add_errback(on_send_error)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

Send_message(topic,avroMessage)