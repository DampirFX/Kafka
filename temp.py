import AvroUtils


pathDll = './AvroUtils.dll'
avroUtilstoAvro = AvroUtils.JsonSerializer(pathDll)
avroUtilsfromAvro = AvroUtils.JsonDeserializer(pathDll)

print(avroUtilstoAvro.version())
print(avroUtilsfromAvro.version())

pathSchema = './templates/MT4_POSITION_OPEN.avsc'
message = '{"Account":{"Balance":916276.37,"Comment":"","Country":"Russia","CurrencyCode":"USD","Equity":916223.10,"Group":"wbook_ru","ID":"112358","IsOnline":1,"LastActivityDate":"2019-10-28T09:53:59","LastConnectIP":"172.16.81.53","Leverage":500,"LockMarker":0,"Login":995092501,"MQID":"6C22DB73","Margin":205.66,"RegDate":"2016-11-28T11:09:36","SOAutochargeMarker":0},"EventTimeStamp":"2019-11-11T08:28:19.505","Order":{"Comment":"","Commission":0,"DealReason":"DEAL_REASON_CLIENT","InstrumentDigits":2,"Lot":0.20999999999999999,"Magic":0,"OpenCrossPrice":1,"OpenPrice":61.850000000000001,"OpenTime":"2019-11-11T11:21:18","OrderType":"OP_BUY","PL":-8.4000000000000004,"StopLossPrice":0,"Swaps":0,"Symbol":"BRN","TakeProfitPrice":0,"Ticket":40603089,"Volume":210},"headers":{"ACCOUNT":"995092501","ACCOUNT_TYPE":"MT4_MARKET","BROKER":"BVI","CLIENT_ID":"112358","CONTENT_TYPE":"application/json; charset=UTF-8","ENTITY_ACTION":"MT4_POSITION_OPEN","ENTITY_STATUS":"success","ENTITY_TYPE":"position","MESSAGE_CREATED":"2019-11-11T08:28:19.505","MESSAGE_FORMAT":"v1","MESSAGE_PRODUCER":"MTEventTransmitter","MESSAGE_PRODUCER_INSTANCE":"MT4_MARKET_REAL","MESSAGE_TYPE":"ENTITY_EVENT"}}'
print(message)
res_avro_64 = avroUtilstoAvro.toAvroBinaryBase64(pathSchema,message.encode())
print(res_avro_64)

res_from_avro_64 = avroUtilsfromAvro.fromAvroBinaryBase64(pathSchema,res_avro_64)
print(res_from_avro_64.decode() + '\n')

res_avro = avroUtilstoAvro.toAvroJson(pathSchema,message.encode())
print(res_avro.decode())

res_from_avro = avroUtilsfromAvro.fromAvroJson(pathSchema,res_avro)
print(res_from_avro.decode())