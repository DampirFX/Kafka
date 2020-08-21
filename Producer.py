from kafka import KafkaProducer
from kafka.errors import KafkaError



#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
#Имя топика
topic = 'MTSourceReaderDevExpirationTopic'

message = b'\x00\x00\x00\x00f\x0eExecute\x02Z\x10LBX_FCIL&2020-04-25T07:13:00\xac!\x06Z0U\x06Z0V&2020-04-24T07:20:00\x00\x00\x00\x00\x00\x16\xb4@\x00\x00\x00\x00\x00\x14\xb4@\x00\x00\x00\x00\x00\x12\xb4@&2020-04-24T07:20:00\x00\x00\x00\x00\x00 \xb4@\x00\x00\x00\x00\x00\x1e\xb4@\x00\x00\x00\x00\x00\x1c\xb4@'
# b'{"dateTime":"2020-04-24T07:13:00","expirationId":2134,"expirationRates":{"firstAsk":5152,"firstBid":5148,"firstDateTime":"2020-04-24T07:20:00","firstMid":5150,"lastAsk":5142,"lastBid":5138,"lastDateTime":"2020-04-24T07:20:00","lastMid":5140},"lastContract":"Z0V","newContract":"Z0D","sourceId":"LBX_FCIL","status":"Execute","symbol":"Z"}'

#

#b'{"expirationExecute":{"Symbol":"ES","DateTime":"2020-04-02T05:32:00","ExpId":2074,"LastContract":"ES0H","NewContract":"ES0M","MTRates":{"LastAsk":25570,"LastMid":25570,"LastBid":25570,"NewAsk":25570,"NewMid":25570,"NewBid":25570},"LibetrexRates":{"LastAsk":25570,"LastMid":25570,"LastBid":25570,"NewAsk":25570,"NewMid":25570,"NewBid":25570}}}'
#key = '1234'.encode()
    # b'{' \
    #       b'"symbol":"AAPL",' \
    #       b'"dateTime":"2019-08-28T02:55:18",' \
    #       b'"dividendId":1,' \
    #       b'"amount":1.23,' \
    #       b'"currency":"USD"' \
    #       b'}'

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

def Send_message(topic, message):#, key
    producer = KafkaProducer(bootstrap_servers=kafka)
    future = producer.send(topic, message).add_callback(on_send_success).add_errback(on_send_error)#, key
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

Send_message(topic,message)#,key