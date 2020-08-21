from kafka import KafkaConsumer
import AvroUtils
import AvroSchemaRegistry
import datetime

#кластер кафки
kafka = '0.dual.kafka.qa-fxenv.com:9092','1.dual.kafka.qa-fxenv.com:9092','2.dual.kafka.qa-fxenv.com:9092'

# #open_close_position_mt4
# topic = 'mt4--position'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--position-value/versions/latest/schema')

#MT4_TECHNICAL_PING
# topic = 'mt4--ping'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--ping-value/versions/latest/schema')

# #expiration_mt4
# topic = 'mt4--expiration'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--expiration-value/versions/latest/schema')
#
#dividends_mt4
# topic = 'mt4--dividends'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--dividends-value/versions/latest/schema')
#
# #balance_change_mt4
# topic = 'mt4--balance'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--balance-value/versions/latest/schema')
#
# #account_login_mt4
# topic = 'mt4--account'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--account-value/versions/latest/schema')
#
# #mt4OpenedPositionsReport_mt4
# topic = 'mt4--mt4OpenedPositionsReport'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--mt4OpenedPositionsReport-value/versions/latest/schema')
#
# #accountGroup_mt4
# topic = 'mt4--accountGroup'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--accountGroup-value/versions/latest/schema')

# #account_login_mt5
# topic = 'mt5--account'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt5--account-value/versions/latest/schema')

# #Quotes
# topic = 'Quotes'
# Schema = './templates/quote-schema.avsc'


# #MTSourceReaderDevExpirationTopic
# topic = 'MTSourceReaderDevExpirationTopic'
#Schema = './templates/Expiration_SymbolExpirationExecute.avsc'
# Url = 'https://avro-schemaregistry.qa-env.com/subjects/rmng--symbolExpiration-value/versions/latest/schema'
# Schema = AvroSchemaRegistry.GetSchema(Url)

# #ping_mt5
# topic = 'mt5--ping'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt5--ping-value/versions/latest/schema')

# topic = 'mt5--account'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt5--account-value/versions/latest/schema')


# topic = 'mt4--split'
# Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt4--split-value/versions/latest/schema')

# expiration_mt5
topic = 'mt5--expiration'
Schema = AvroSchemaRegistry.GetSchema('https://avro-schemaregistry.qa-env.com/subjects/mt5--expiration-value/versions/latest/schema')

consumer = KafkaConsumer(topic, bootstrap_servers=kafka)
pathDll = './AvroUtils.dll'
avroUtils = AvroUtils.JsonDeserializer(pathDll)

for msg in consumer:

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") +' Avro: ')
    print(msg.value)
    print(msg.key)
    try:
        print('Json: ' + avroUtils.fromAvroBinary(Schema,msg.value[5:]).decode())
        #         print('Json: ' + avroUtils.fromAvroBinary(Schema,msg.value[5:]).decode())
    except Exception:
        print('Not valid message. Skip.')


# for msg in consumer:
#     print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") +' Avro: ')
#     print(msg.value)
#     try:
#         print('Json: ' + avroUtils.fromAvroBinary(Schema,msg.value).decode())
#     except Exception:
#         print('Not valid message. Skip.')
#         try:
#             print('Json: ' + avroUtils.fromAvroBinary(Schema,msg.value[5:]).decode())
#         except Exception:
#             print('Not valid message. Skip.')