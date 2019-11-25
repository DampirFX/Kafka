import requests


def GetSchema(Url):
    headers = {'Content-type': 'application/vnd.schemaregistry.v1+json'}
    answer = requests.get(Url, headers=headers)
    FinishResult = answer.content.decode()
    pathSchemaFile = open('temp.avsc','w')
    pathSchemaFile.write(FinishResult)
    pathSchemaFile.close()
    pathSchema = './temp.avsc'
    return pathSchema