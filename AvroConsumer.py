from kafka import KafkaConsumer
import AvroUtils


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'

#Имя топика
topic = 'MT5--Dividends--Interday--Avro--V1'

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = './AvroUtils.dll'
pathSchema = './templates/MT5_DIVIDENDS.avsc'
avroUtils = AvroUtils.JsonDeserializer(pathDll)

for msg in consumer:
    print('Avro: ' + msg.value.decode())
    try:
        print('Json: ' + avroUtils.fromAvroBinaryBase64(pathSchema,msg.value).decode() + '\n')
    except Exception:
        print('Not valid message. Skip.')