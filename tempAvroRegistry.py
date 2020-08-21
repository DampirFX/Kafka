import AvroUtils
import AvroSchemaRegistry


pathDll = './AvroUtils.dll'
avroUtilstoAvro = AvroUtils.JsonSerializer(pathDll)
avroUtilsfromAvro = AvroUtils.JsonDeserializer(pathDll)

Url = 'https://avro-schemaregistry.qa-env.com/subjects/mt4--split-value/versions/latest/schema'
pathSchema = AvroSchemaRegistry.GetSchema(Url)
# pathSchema = './templates/mt4--split-value.avsc'
print('Schema: ' + pathSchema)

bin = b".2020-08-14T09:03:47.776\x10\x10Aeroflot\x1cAeroflot, PJSC&2020-08-11T15:20:53\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\xa0\xdf\xd2\xae\x05\x10wbook_cn\x06USD\x02\x06AUS\x02\x00&2019-09-06T22:07:28&2020-08-14T11:00:18\x00\x00\x00\x00\x00@\x7f@\x02\x00\x02\x8f\xc2\xf5(\xfc\x97\xc2@\x02R\xb8\x1e\x85\xeb\x8c\x95@\x02R\xb8\x1e\x85\x0b\x04\xce@\x02\x020\x02\x0e7076074\x02\x00\x02\x00\x02\x02\xb6\xf7\x8c'\x02\x10Aeroflot\x02\x04\x0cOP_BUY\x02\x00\x00\x00\x00\x00\x00Y@\x02\x00\x00\x00\x00\x00\x00\xf0?&2020-08-11T15:20:00\x02\x00\x00\x00\x00\x00\xc0T@\x02\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x02&2020-08-11T15:20:54\x02a\xb6\x15\xf3\xe1+\x8c?\x02\x0e\xd2M\xae0,\x8c?\xb8\x1e\x85\xebQ\xb8\x9e\xbf\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\xc0T@\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x02*split: close position\x02$DEAL_REASON_DEALER\x02\x12720000976\x02\x14MT4_MARKET\x02\x06BVI\x0e7076074>application/json; charset=UTF-8\x12MT4_SPLIT\x0esuccess\nsplit.2020-08-14T09:03:47.776\x04v1$MTEventTransmitter\x1eMT4_MARKET_REAL\x18ENTITY_EVENT"
# avro_bin = bin[5:]
# print('\nbinary message: ')
# print(bin)
# message = b'{"Order": {"PL": 0, "Lot": 2.00, "Magic": 0, "Swaps": 0, "Symbol": "AAPL", "Ticket": 40688300, "Volume": 200, "Comment": "split: close old position AAPL", "OpenTime": "2020-06-26T05:29:57", "CloseTime": "2020-06-26T05:30:22", "OpenPrice": 480.06, "OrderType": "OP_BUY", "ClosePrice": 480.06, "Commission": -3, "DealReason": "DEAL_REASON_DEALER", "StopLossPrice": 476.46, "OpenCrossPrice": 1, "CloseCrossPrice": 1, "TakeProfitPrice": 476.46, "InstrumentDigits": 2, "SplitCorrectionType": "PositionClose"}, "Account": {"ID": "432534", "MQID": "6C22DB73", "Group": "bbook_ru_cfy", "Login": 995092501, "Equity": 1972267.81, "Margin": 0, "Balance": 1972267.81, "Comment": "test comment", "Country": "Russia", "RegDate": "2016-11-28T11:09:36", "IsOnline": 1, "Leverage": 500, "LockMarker": 0, "CurrencyCode": "USD", "LastConnectIP": "", "LastActivityDate": "2020-08-14T05:44:33", "SOAutochargeMarker": 0}, "SplitInfo": {"Symbol": "AAPL", "SplitId": 9, "SplitTime": "2020-06-26T05:30:22", "SymbolDescription": "Apple Inc.", "SplitRatioNumerator": 11, "SplitRatioDenominator": 9}, "EventTimeStamp": "2020-08-14T04:57:25.431","headers":{"BROKER": "BVI", "ACCOUNT": "720002727", "CLIENT_ID": "7149732", "ENTITY_TYPE": "split", "ACCOUNT_TYPE": "MT4_INSTANT", "CONTENT_TYPE": "application/json; charset=UTF-8", "MESSAGE_TYPE": "ENTITY_EVENT", "ENTITY_ACTION": "MT4_SPLIT", "ENTITY_STATUS": "success", "MESSAGE_FORMAT": "v1", "MESSAGE_CREATED": "2020-08-14T03:49:07.443", "MESSAGE_PRODUCER": "MTEventTransmitter", "MESSAGE_PRODUCER_INSTANCE": "MT4_DEV_192"}}'
# print('to avro bin')
# res_to_avro_bin = avroUtilstoAvro.toAvroBinary(pathSchema,message)
# print(res_to_avro_bin)
print ('from avro bin')
res_from_avro_bin = avroUtilsfromAvro.fromAvroBinary(pathSchema,bin)
print('\njson: ' + res_from_avro_bin.decode() + '\n')
