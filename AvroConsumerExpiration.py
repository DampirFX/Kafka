from kafka import KafkaConsumer
import AvroUtils

# status = Notice - 1, Start - 2
# kafka = dev или qa
messagetype = 2
kafka = 'qa'

if (kafka == 'qa'):
    kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
    topic = 'expiration-service--symbolExpiration'
elif(kafka == 'dev'):
    kafka = '172.20.241.144:9092,172.20.244.128:9092,172.20.240.200:9092'
    topic = 'expiration-service--symbolExpiration'

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = './AvroUtils.dll'

if (messagetype == 1):
    pathSchema = './avro_MT_templates/EXPIRATION_START.avsc'
    status = 'Start'
elif(messagetype == 2):
    pathSchema = './avro_MT_templates/EXPIRATION_NOTICE.avsc'
    status = 'Notice'

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