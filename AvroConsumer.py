from kafka import KafkaConsumer
import AvroUtils


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'

#Имя топика
topic = 'mt4--mt4OpenedPositionsReport'

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = './AvroUtils.dll'
pathSchema = './templates/MT4_OPENED_POSITIONS_REPORT.avsc'
avroUtils = AvroUtils.JsonDeserializer(pathDll)

for msg in consumer:
    #print('Avro: ' + msg.value.decode())
    print('Avro: ')
    print(msg.value)
    try:
        print('Json: ' + avroUtils.fromAvroBinaryBase64(pathSchema,msg.value).decode() + '\n')
    except Exception:
        print('Not valid message. Skip.')