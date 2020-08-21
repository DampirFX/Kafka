import AvroUtils


pathDll = './AvroUtils.dll'
avroUtilstoAvro = AvroUtils.JsonSerializer(pathDll)
avroUtilsfromAvro = AvroUtils.JsonDeserializer(pathDll)

pathSchema = './templates/MT4_DIVIDENDS.avsc'

message = '{"EventTimeStamp":"2019-11-26T08:44:30.053",' \
          '"MT4Account":{' \
          '"Balance":1043.23,"Comment":"","Country":"Russia","CurrencyCode":"USD","Equity":1085.6400000000001,"Group":"bbook_es","ID":"123","IsOnline":0,' \
          '"LastActivityDate":"2019-11-18T12:05:44","LastConnectIP":"","Leverage":200,"LockMarker":0,"Login":995117015,"MQID":"D1541E31","Margin":57.289999999999999,' \
          '"RegDate":"2018-11-21T13:45:54","SOAutochargeMarker":0' \
          '},' \
          '"MT4DividendInfo":{"Amount":1.8999999999999999,"Currency":"USD","DividendDate":"2019-08-28 03:45:00.000","ID":99916,"SourceSymbol":"AAPL",' \
          '"Symbol":"AAPL","SymbolDescription":"Apple Inc."' \
          '},' \
          '"MT4Order":{"Comment":"[AAPL]_TC_#36940796 0.01","Commission":0,"OpenTime":"2019-11-26T11:44:24","OrderType":"OP_BALANCE","PL":1.8999999999999999,' \
          '"Swaps":0,"Ticket":36940864},' \
          '"headers":{"ACCOUNT":"995117015","BROKER":"BVI","CLIENT_ID":"123","CONTENT_TYPE":"application/json; charset=UTF-8","ENTITY_ACTION":"MT4_DIVIDENDS",' \
          '"ENTITY_STATUS":"success","ENTITY_TYPE":"dividends","MESSAGE_CREATED":"2019-11-26T08:44:30.053","MESSAGE_FORMAT":"v1","MESSAGE_PRODUCER":"MTEventTransmitter",' \
          '"MESSAGE_PRODUCER_INSTANCE":"MT4_INSTANT_REAL1","MESSAGE_TYPE":"ENTITY_EVENT"}}'
print(message)
res_avro_64 = avroUtilstoAvro.toAvroBinaryBase64(pathSchema,message.encode())
print(res_avro_64)

res_from_avro_64 = avroUtilsfromAvro.fromAvroBinaryBase64(pathSchema,res_avro_64)
print(res_from_avro_64.decode())

res_avro = avroUtilstoAvro.toAvroBinary(pathSchema,message.encode())
print(res_avro.decode())

res_from_avro = avroUtilsfromAvro.fromAvroBinary(pathSchema,res_avro)
print(res_from_avro.decode())