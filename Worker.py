from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import logging
import json
from configparser import ConfigParser
import importlib.util
import pickle

# ##InvertedIndex mapper function
# def InvertedIndexMapper(data, filename):
#     logger.info("called WordCountMapper")
#     data = data.lower()
#     dataList = ''.join((c if c.isalpha() else ' ') for c in data).split()
#     resultData = []
#     for word in dataList:
#         resultData.append((word, filename))
#     return resultData

# ##WordCount mapper function
# def WordCountMapper(data, filename):
#     logger.info("called WordCountMapper")

#     data = data.lower()
#     dataList = ''.join((c if c.isalpha() else ' ') for c in data).split()
#     resultData = []
#     for word in dataList:
#         resultData.append((word, 1))
#     return resultData

# ##InvertedIndex reducer function
# def InvertedIndexReducer(data):
#     logger.info("called InvertedIndexReducer")

#     resultDict = {}
#     for word in data:
#         for entry in data[word]:
#             if word in resultDict:
#                 if entry[1] in resultDict[word]:
#                     resultDict[word][entry[1]] += 1
#                 else:
#                     resultDict[word][entry[1]] = 1
#             else:
#                 resultDict[word] = {entry[1]: 1}
#     return resultDict

# ##WordCount reducer function
# def WordCountReducer(data):
#     logger.info("called wordcount reducer")
#     resultDict = {}
#     for word in data:
#         resultDict[word] = len(data[word])
#     return resultDict


def isWorkerConnected():
    return True


##store data to key value store
def storeOutputData(dataStoreObj, result, path):
    logger.info("storing datastore object")
    data = 'set-data' + ' ' + path + '\n' + json.dumps(result) + '\n'
    dataStoreObj.DataStore(data)
    logger.info("stored datastore object")
    return


##get data from key value store
def getInputData(file, dataStoreObj):
    logger.info("fetching datastore object")
    responseMessage = 'get-data' + '\n' + file + '\n'
    jsonData = dataStoreObj.DataStore(responseMessage)
    jsonData = json.loads(jsonData)
    logger.info("fetched datastore object")

    return jsonData


def worker(uniqueId, worker, file, passedFunction, caller, kvIP, taskNumber=0):
    logger.info("called worker with worker number %s", worker)
    logger.info("called worker with task number %s", taskNumber)
    logger.info("called worker with file name %s", file)
    logger.info("called worker with funciton %s", passedFunction)
    logger.info("called worker from %s", caller)

    ##function map of available applications
    # functionMap = {
    #     "mapper": {
    #         "InvertedIndexMapper": InvertedIndexMapper,
    #         "WordCountMapper": WordCountMapper
    #     },
    #     "reducer": {
    #         "InvertedIndexReducer": InvertedIndexReducer,
    #         "WordCountReducer": WordCountReducer
    #     }
    # }

    logger.info("creating datastore object")
    ##connect to the keyvaluestore
    dataStoreObj = xmlrpc.client.ServerProxy(
        'http://' + kvIP + ':' + parser.get('address', 'keyValuePort'),
        allow_none=True)

    result = getInputData(file, dataStoreObj)
    operateFunc = pickle.loads(passedFunction)

    if caller == "mapper":
        for file in result:
            filename = file
            res = result[filename]
        # resultData = functionMap[caller][passedFunction](res, filename)
        resultData = operateFunc(res, filename)
        path = "Data/" + uniqueId + "/mapperOutput/output" + str(worker) + str(
            taskNumber) + ".json"
    if caller == "reducer":
        # resultData = functionMap[caller][passedFunction](result)
        resultData = operateFunc(res, filename)
        path = "Data/" + uniqueId + "/reducerOutput/output" + str(
            worker) + ".json"

    logger.info("storing mapper oiutput to kv store %s", path)

    ##Send data to keyvalue store
    storeOutputData(dataStoreObj, resultData, path)

    logger.info("mapper task done")


if __name__ == '__main__':
    logger = logging.getLogger('worker-node')
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler('worker.log')
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
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
    server.register_function(isWorkerConnected, 'isWorkerConnected')

    #run the rpc server
    try:
        logger.info('Worker running')
        server.serve_forever()
    except Exception:
        logger.info('Error while running the server')
