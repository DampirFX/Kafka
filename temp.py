import AvroUtils


pathDll = './AvroUtils.dll'
avroUtilstoAvro = AvroUtils.JsonSerializer(pathDll)
avroUtilsfromAvro = AvroUtils.JsonDeserializer(pathDll)

pathSchema = './templates/MT4_DIVIDENDS.avsc'

message = '{"EventTimeStamp":"2019-11-25T07:05:58.390","MT4Account":{"Balance":4699.74,"Comment":"test òåñò","Country":"Albania","CurrencyCode":"USD","Equity":6012.74,"Group":"abook_am","ID":"123","IsOnline":0,"LastActivityDate":"2019-10-28T07:55:22","LastConnectIP":"","Leverage":100,"LockMarker":0,"Login":995124959,"MQID":"D1541E31","Margin":1215.1,"RegDate":"2019-09-03T06:16:14","SOAutochargeMarker":0},"MT4DividendInfo":{"Amount":1.9,"Currency":"USD","DividendDate":"2019-08-28 03:45:00.000","ID":99928,"SourceSymbol":"AAPL","Symbol":"AAPL","SymbolDescription":"Apple Inc."},"MT4Order":{"CloseCrossPrice":null,"ClosePrice":null,"CloseTime":null,"Comment":"D_TC_#40602612 1.00 AAPL","Commission":0,"DealReason":null,"InstrumentDigits":null,"Lot":null,"Magic":null,"OpenCrossPrice":null,"OpenPrice":null,"OpenTime":"2019-11-25T10:05:55","OrderType":"OP_BALANCE","PL":190,"StopLossPrice":null,"Swaps":0,"Symbol":null,"TakeProfitPrice":null,"Ticket":40603647,"Volume":null}}'
print(message)
res_avro_64 = avroUtilstoAvro.toAvroBinaryBase64(pathSchema,message.encode())
print(res_avro_64)

res_from_avro_64 = avroUtilsfromAvro.fromAvroBinaryBase64(pathSchema,res_avro_64)
print(res_from_avro_64.decode() + '\n')

res_avro = avroUtilstoAvro.toAvroJson(pathSchema,message.encode())
print(res_avro.decode())

res_from_avro = avroUtilsfromAvro.fromAvroJson(pathSchema,res_avro)
print(res_from_avro.decode())