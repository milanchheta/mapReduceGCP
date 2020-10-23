from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import logging
import json
from configparser import ConfigParser
import os, stat

from mappers.InvertedIndexMapper import InvertedIndexMapper
from mappers.WordCountMapper import WordCountMapper
from reducers.InvertedIndexReducer import InvertedIndexReducer
from reducers.WordCountReducer import WordCountReducer

i = 0


def isWorkerConnected():
    return True


##store data to key value store
def storeOutputData(dataStoreObj, result, path):
    try:
        logger.info("storing datastore object")
        data = 'set-data' + ' ' + path + '\n' + json.dumps(result) + '\n'
        dataStoreObj.DataStore(data)
        logger.info("stored datastore object")
        return
    except Exception as e:
        raise e


##get data from key value store
def getInputData(file, dataStoreObj):
    try:
        logger.info("fetching datastore object")
        responseMessage = 'get-data' + '\n' + file + '\n'
        jsonData = dataStoreObj.DataStore(responseMessage)
        jsonData = json.loads(jsonData)
        logger.info("fetched datastore object")
        return jsonData
    except Exception as e:
        raise e


def worker(uniqueId,
           worker,
           file,
           passedFunction,
           caller,
           kvIP,
           taskQueue,
           taskNumber=0):
    logger.info("called worker with worker number %s", worker)
    logger.info("called worker with task number %s", taskNumber)
    logger.info("called worker with file name %s", file)
    logger.info("called worker with funciton %s", passedFunction)
    logger.info("called worker from %s", caller)
    global i

    #function map of available applications
    functionMap = {
        "mapper": {
            "InvertedIndexMapper.py": InvertedIndexMapper,
            "WordCountMapper.py": WordCountMapper
        },
        "reducer": {
            "InvertedIndexReducer.py": InvertedIndexReducer,
            "WordCountReducer.py": WordCountReducer
        }
    }
    try:
        logger.info("creating datastore object")
        ##connect to the keyvaluestore
        dataStoreObj = xmlrpc.client.ServerProxy(
            'http://' + kvIP + ':' + parser.get('address', 'keyValuePort'),
            allow_none=True)

        result = getInputData(file, dataStoreObj)
        # operateFunc = marshal.loads(passedFunction)
        if i == 1:
            logger.info("Simulating fault tolerance test")
            taskQueue.put("ERROR")
            raise Exception('Simulating fault tolerance test')
        i += 1
        if caller == "mapper":
            for file in result:
                filename = file
                res = result[filename]
            resultData = functionMap[caller][passedFunction](res, filename)
            # resultData = operateFunc(res, filename)
            path = "Data/" + uniqueId + "/mapperOutput/output" + str(
                worker) + str(taskNumber) + ".json"
        if caller == "reducer":
            resultData = functionMap[caller][passedFunction](result)
            # resultData = operateFunc(res, filename)
            path = "Data/" + uniqueId + "/reducerOutput/output" + str(
                worker) + ".json"

        logger.info("storing mapper oiutput to kv store %s", path)

        ##Send data to keyvalue store
        storeOutputData(dataStoreObj, resultData, path)

        logger.info("mapper task done")
        taskQueue.put("DONE")

        return
    except Exception as e:
        if i == 1:
            logger.info("Simulated fault tolerance test")
        taskQueue.put("ERROR")
        raise e


if __name__ == '__main__':
    log_dir = './'
    os.chmod(log_dir, stat.S_IRWXU)
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
