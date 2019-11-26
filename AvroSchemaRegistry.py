import requests
import json

def GetSchema(Url):
    headers = {'Content-type': 'application/vnd.schemaregistry.v1+json'}
    answer = requests.get(Url, headers=headers)
    FinishResult = answer.content.decode()
    # pathSchemaFile = open('temp.avsc','w')
    # pathSchemaFile.write(FinishResult)
    # pathSchemaFile.close()
    # pathSchema = './temp.avsc'
    return FinishResult

def PostSchema(Url, Data):
    Headers = {'Content-type': 'application/vnd.schemaregistry.v1+json'}
    answer = requests.post(url=Url, data=(Data), headers=Headers)
    FinishResult = answer.content.decode()
    return FinishResult
data = '{"schema": "{\\"namespace\\": \\"mt4.dividends\\",\\"type\\": \\"record\\",\\"name\\": \\"MT4_DIVIDENDS\\",\\"doc\\": \\"https://conf.fxclub.org/display/LIB/MT4_DIVIDENDS\\",\\"fields\\": [{\\"name\\": \\"EventTimeStamp\\",\\"type\\": \\"string\\"},{\\"name\\": \\"MT4Account\\",\\"type\\": {\\"type\\": \\"record\\",\\"name\\": \\"MT4Account\\",\\"doc\\": \\"https://conf.fxclub.org/display/LIB/MT4Account\\",\\"fields\\": [{\\"name\\": \\"Login\\",\\"type\\": \\"long\\"},{\\"name\\": \\"Group\\",\\"type\\": \\"string\\"},{\\"name\\": \\"CurrencyCode\\",\\"type\\": \\"string\\"},{\\"name\\": \\"Country\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"Comment\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"RegDate\\",\\"type\\": \\"string\\"},{\\"name\\": \\"LastActivityDate\\",\\"type\\": \\"string\\"},{\\"name\\": \\"Leverage\\",\\"type\\": \\"double\\"},{\\"name\\": \\"LastConnectIP\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"Balance\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"Margin\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"Equity\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"MQID\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"ID\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"IsOnline\\",\\"type\\": [\\"null\\", \\"int\\"],\\"default\\":null},{\\"name\\": \\"SOAutochargeMarker\\",\\"type\\": [\\"null\\", \\"int\\"],\\"default\\":null},{\\"name\\": \\"LockMarker\\",\\"type\\": [\\"null\\", \\"int\\"],\\"default\\":null}]}},{\\"name\\": \\"MT4DividendInfo\\",\\"type\\": {\\"type\\": \\"record\\",\\"name\\": \\"MT4DividendInfo\\",\\"doc\\": \\"https://conf.fxclub.org/display/LIB/MT4DividendInfo\\",\\"fields\\": [{\\"name\\": \\"ID\\",\\"type\\": \\"long\\"},{\\"name\\": \\"Symbol\\",\\"type\\": \\"string\\"},{\\"name\\": \\"SymbolDescription\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"SourceSymbol\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"DividendDate\\",\\"type\\": \\"string\\"},{\\"name\\": \\"Amount\\",\\"type\\": \\"double\\"},{\\"name\\": \\"Currency\\",\\"type\\": \\"string\\"}]}},{\\"name\\": \\"MT4Order\\",\\"type\\": {\\"type\\": \\"record\\",\\"name\\": \\"MT4Order\\",\\"doc\\": \\"https://conf.fxclub.org/display/LIB/MT4Order\\",\\"fields\\": [{\\"name\\": \\"Ticket\\",\\"type\\": \\"long\\"},{\\"name\\": \\"Symbol\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"InstrumentDigits\\",\\"type\\": [\\"null\\", \\"int\\"],\\"default\\":null},{\\"name\\": \\"OrderType\\",\\"type\\": \\"string\\"},{\\"name\\": \\"Volume\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"Lot\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"OpenTime\\",\\"type\\": \\"string\\"},{\\"name\\": \\"OpenPrice\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"StopLossPrice\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"TakeProfitPrice\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"CloseTime\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"OpenCrossPrice\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"CloseCrossPrice\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"Commission\\",\\"type\\": \\"double\\"},{\\"name\\": \\"Swaps\\",\\"type\\": \\"double\\"},{\\"name\\": \\"ClosePrice\\",\\"type\\": [\\"null\\", \\"double\\"],\\"default\\":null},{\\"name\\": \\"PL\\",\\"type\\": \\"double\\"},{\\"name\\": \\"Magic\\",\\"type\\": [\\"null\\", \\"int\\"],\\"default\\":null},{\\"name\\": \\"Comment\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null},{\\"name\\": \\"DealReason\\",\\"type\\": [\\"null\\", \\"string\\"],\\"default\\":null}]}}]}"}'
print(PostSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--dividends-value/versions',data))