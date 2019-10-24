from kafka import KafkaConsumer
import AvroUtils


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'

#Имя топика
topic = 'MT4--All--Interday--Avro--V1'

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = 'c:/SibIT/autotest/kafka/AvroUtils.dll'
pathSchema = 'C:/deploy/MT4_DIVIDENDS.avsc'
avroUtils = AvroUtils.JsonDeserializer(pathDll)

for msg in consumer:
    print('Avro: ' + msg.value.decode())
    print('Json: ' + avroUtils.fromAvroBinaryBase64(pathSchema,msg.value).decode() + '\n')
