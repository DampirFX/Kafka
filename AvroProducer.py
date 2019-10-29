from kafka import KafkaProducer
from kafka.errors import KafkaError
import AvroUtils

#создание экземпляра авро
pathDll = './AvroUtils.dll'
pathSchema = './templates/MT5_DIVIDENDS.avsc'
avroUtils = AvroUtils.JsonSerializer(pathDll)


#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'
#Имя топика
topic = 'Test123'

#исходное сообщение
message = '{"EventTimeStamp":"2019-10-28T04:09:18.234","MT5Account":{"Account":"","Address":null,"Agent":0,"Balance":null,"BalancePrevDay":1158206.49,"BalancePrevMonth":1158206.49,"CertSerialNumber":0,"City":"Barnaul","Color":0,"Comment":"test","CommissionDaily":0,"CommissionMonthly":0,"Company":"ForexClub","Country":"RUS","Credit":0,"CurrencyCode":"USD","Email":null,"Equity":null,"EquityPrevDay":1158118.58,"EquityPrevMonth":1158068.95,"Group":"instant-ru","ID":"123","InterestRate":0,"IsOnline":1,"Language":25,"LastAccess":"2018-10-30 12:54:51","LastIP":"192.168.11.175","LeadSource":"","Leverage":500,"Login":550000545,"MQID":"0","Margin":null,"Name":"test","Phone":null,"Registration":"2017-07-20 07:08:14","Rights":4451,"State":"АЛТАЙСКИЙ КРАЙ","Status":"NR","ZipCode":"74785451"},"MT5Deal":{"Action":"DEAL_DIVIDEND","Comment":"D_TC_#2490064 0.01 AAPL","Commission":0,"ContractSize":0,"Deal":1271781,"Dealer":1109,"Digits":2,"DigitsCurrency":2,"Entry":"ENTRY_IN","ExpertID":0,"ExternalID":"","Flags":0,"Gateway":"","Login":550000545,"Order":0,"PositionID":0,"Price":0,"PriceGateway":0,"PricePosition":0,"Profit":1.5,"ProfitRaw":0,"RateMargin":0,"RateProfit":0,"Reason":"DEAL_REASON_DEALER","Storage":0,"Symbol":"","TickSize":0,"TickValue":0,"TimeMsc":"2019-10-28 07:09:03.332","Volume":0,"VolumeClosed":0},"MT5DividendInfo":{"Amount":1.5,"Currency":"USD","DividendDate":"2019-10-24 03:58:00","ID":9981,"SourceSymbol":"AAPL","Symbol":"Apple","SymbolDescription":"Apple Inc."}}'
print('Json: ' + message)
key = '1234'.encode()

avroMessage = avroUtils.toAvroBinaryBase64(pathSchema,message.encode())
print('Avro: ' + avroMessage.decode())

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

def Send_message(topic, message, key):
    producer = KafkaProducer(bootstrap_servers=kafka)
    future = producer.send(topic, message, key).add_callback(on_send_success).add_errback(on_send_error)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        pass

Send_message(topic,avroMessage,key)