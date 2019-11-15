from kafka import KafkaConsumer
import datetime

filename = datetime.datetime.now().strftime("%Y-%m-%d")

#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'

#Имя топика
topic = 'mt4--position'
l = ''

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)

for msg in consumer:
    # f = open(filename + '.log', 'a')
    # print('Header:')
    # for l in msg.headers:
    #     print(l)
    #
    print('Body:\n'+ msg.value.decode())
    print('Key:')
    print(msg.key)
    # f.write(datetime.datetime.now().strftime("%H:%M:%S") +' - ' + msg.value.decode() + '\n')
    # f.close()

    #Распечатать все сообщение: заголовки, тело, ключи, номер в топике, топик и т.д.
    #print(msg)