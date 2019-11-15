from kafka import KafkaConsumer
import AvroUtils
# messagetype = created = 1, deleted - 2, 3 - executed
# kafka = dev или qa
messagetype = 3
kafka = 'dev'

#кластер кафки
if (kafka == 'qa'):
    kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
elif(kafka == 'dev'):
    kafka = '172.20.241.144:9092,172.20.244.128:9092,172.20.240.200:9092'
    if(messagetype == 3):
        topic = 'PriceAlerts_DEV_OUT'
    else:
        topic = 'PriceAlert_DEV_in'

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = './AvroUtils.dll'
#pathSchema = './avro_MT_templates/PA_CREATE.avsc'
#pathSchema = './avro_MT_templates/PA_DELETE.avsc'

if (messagetype == 1):
    pathSchema = './avro_MT_templates/PA_CREATE_S.avsc'
elif(messagetype == 2):
    pathSchema = './avro_MT_templates/PA_DELETE_S.avsc'
elif(messagetype == 3):
    pathSchema = './avro_MT_templates/PA_EXECUTED_S.avsc'

avroUtils = AvroUtils.JsonDeserializer(pathDll)

for msg in consumer:
    #print('Avro: ' + msg.value.decode())
    print('Avro: ')
    print(msg.value)
    try:
        #print('Json: ' + avroUtils.fromAvroBinaryBase64(pathSchema,msg.value).decode() + '\n')
        print('Json: ' + avroUtils.fromAvroBinary(pathSchema,msg.value).decode() + '\n')
    except Exception:
        print('Not valid message. Skip.')