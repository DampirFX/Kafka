from kafka import KafkaConsumer, TopicPartition
import datetime


filename = datetime.datetime.now().strftime("%Y-%m-%d")

#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092,1.dual.kafka.qa-fxenv.com:9092,2.dual.kafka.qa-fxenv.com:9092'

#Имя топика
# l = ''
# partition = TopicPartition('mt--quote', 0)
# start = 0
# end = 600000
# consumer = KafkaConsumer(bootstrap_servers=kafka)
# consumer.assign([partition])
# consumer.seek_to_beginning(partition)

#
topic = 'mt--quote'
consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
for msg in consumer:
        print(msg.value)
# for msg in consumer:
#     f = open(filename + '.log', 'a')
#     print('Header:')
#     for l in msg.headers:
#          print(l)
#     #
#     print('Body:\n'+ msg.value.decode())
#     print('Key:')
#     print(msg.key)
#     f.write(datetime.datetime.now().strftime("%H:%M:%S") +' - ' + msg.value.decode() + '\n')
#     f.close()

    #Распечатать все сообщение: заголовки, тело, ключи, номер в топике, топик и т.д.
    #print(msg)