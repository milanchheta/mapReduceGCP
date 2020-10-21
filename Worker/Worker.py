from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client

import json
from configparser import ConfigParser


##InvertedIndex mapper function
def InvertedIndexMapper(data, filename):
    data = data.lower()
    dataList = ''.join((c if c.isalpha() else ' ') for c in data).split()
    resultData = []
    for word in dataList:
        resultData.append((word, filename))
    return resultData


##WordCount mapper function
def WordCountMapper(data, filename):
    data = data.lower()
    dataList = ''.join((c if c.isalpha() else ' ') for c in data).split()
    resultData = []
    for word in dataList:
        resultData.append((word, 1))
    return resultData


##InvertedIndex reducer function
def InvertedIndexReducer(data):
    resultDict = {}
    for word in data:
        for entry in data[word]:
            if word in resultDict:
                if entry[1] in resultDict[word]:
                    resultDict[word][entry[1]] += 1
                else:
                    resultDict[word][entry[1]] = 1
            else:
                resultDict[word] = {entry[1]: 1}
    return resultDict


##WordCount reducer function
def WordCountReducer(data):
    resultDict = {}
    for word in data:
        resultDict[word] = len(data[word])
    return resultDict


##store data to key value store
def storeOutputData(dataStoreObj, result, path):
    data = 'set-data' + ' ' + path + '\n' + json.dumps(result) + '\n'
    dataStoreObj.DataStore(data)
    return


##get data from key value store
def getInputData(file, dataStoreObj):
    responseMessage = 'get-data' + '\n' + file + '\n'
    jsonData = dataStoreObj.DataStore(responseMessage)
    jsonData = json.loads(jsonData)
    return jsonData


def worker(uniqueId, worker, file, passedFunction, caller, kvIP, taskNumber=0):
    ##function map of available applications
    functionMap = {
        "mapper": {
            "InvertedIndexMapper": InvertedIndexMapper,
            "WordCountMapper": WordCountMapper
        },
        "reducer": {
            "InvertedIndexReducer": InvertedIndexReducer,
            "WordCountReducer": WordCountReducer
        }
    }
    ##connect to the keyvaluestore
    dataStoreObj = xmlrpc.client.ServerProxy(
        'http://' + str(kvIP) + ':' + parser.get('address', 'keyValuePort'),
        allow_none=True)

    result = getInputData(file, dataStoreObj)

    if caller == "mapper":
        for file in result:
            filename = file
            res = result[filename]
        resultData = functionMap[caller][passedFunction](res, filename)
        path = "Data/" + uniqueId + "/mapperOutput/output" + str(worker) + str(
            taskNumber) + ".json"
    if caller == "reducer":
        resultData = functionMap[caller][passedFunction](result)
        path = "Data/" + uniqueId + "/reducerOutput/output" + str(
            worker) + ".json"

    ##Send data to keyvalue store
    storeOutputData(dataStoreObj, resultData, path)


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('config.ini')

    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2', )

    portNumber = int(parser.get('address', 'port'))
    server = SimpleXMLRPCServer(("", portNumber),
                                requestHandler=RequestHandler,
                                allow_none=True)
    server.register_introspection_functions()

    #register functions to rpc server
    server.register_function(worker, 'worker')

    #run the rpc server
    try:
        print('Worker running')
        server.serve_forever()
    except Exception:
        print('Error while running the server')
