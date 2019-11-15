from kafka import KafkaConsumer
import AvroUtils


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'

#Имя топика
topic = 'expiration-service--symbolExpiration'

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = './AvroUtils.dll'
pathSchema = './avro_MT_templates/EXPIRATION_START.avsc'
avroUtils = AvroUtils.JsonDeserializer(pathDll)

for msg in consumer:
    print('Avro: ' + msg.value.decode())
    try:
        print('Json: ' + avroUtils.fromAvroBinaryBase64(pathSchema,msg.value).decode() + '\n')
    except Exception:
        print('Not valid message. Skip.')