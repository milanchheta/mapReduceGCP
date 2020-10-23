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
workerStatus = "IDLE"


def isWorkerConnected():
    return True


def status():
    global workerStatus
    return workerStatus


##store data to key value store
def storeOutputData(dataStoreObj, result, path):
    try:
        logger.info("STORING OUTPUT DATA")
        data = 'set-data' + ' ' + path + '\n' + json.dumps(result) + '\n'
        dataStoreObj.DataStore(data)
        return
    except Exception as e:
        logger.info("ERROR STORING OUTPUT DATA")
        raise e


##get data from key value store
def getInputData(file, dataStoreObj):
    try:
        logger.info("FETCHING INPUT DATA FOR OPERATION")
        responseMessage = 'get-data' + '\n' + file + '\n'
        jsonData = dataStoreObj.DataStore(responseMessage)
        jsonData = json.loads(jsonData)
        return jsonData
    except Exception as e:
        logger.info("ERROR FETCHING INPUT DATA FOR OPERATION")

        raise e


def worker(uniqueId, worker, file, passedFunction, caller, kvIP, taskNumber=0):

    logger.info(
        "WORKER: {} CALLED WITH TASK: {} FOR {} TASK WITH PASSED FUCNTION: {}".
        format(worker, taskNumber, caller, passedFunction))
    global i
    global workerStatus
    workerStatus = "RUNNING"

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
        ##connect to the keyvaluestore
        dataStoreObj = xmlrpc.client.ServerProxy(
            'http://' + kvIP + ':' + parser.get('address', 'keyValuePort'),
            allow_none=True)

        result = getInputData(file, dataStoreObj)

        if caller == "mapper":
            for file in result:
                filename = file
                res = result[filename]
            logger.info('CALLING FUNCTION FOR %s', passedFunction)
            resultData = functionMap[caller][passedFunction](res, filename)
            path = "Data/" + uniqueId + "/mapperOutput/output" + str(
                worker) + str(taskNumber) + ".json"

        if caller == "reducer":
            logger.info('CALLING FUNCTION FOR %s', passedFunction)
            resultData = functionMap[caller][passedFunction](result)
            path = "Data/" + uniqueId + "/reducerOutput/output" + str(
                worker) + ".json"

        ##Send data to keyvalue store
        storeOutputData(dataStoreObj, resultData, path)

        workerStatus = "FINISHED"
        logger.info('RETURNNING FROM WORKER %s ', worker)

        return
    except Exception as e:
        workerStatus = "ERROR"
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
    server.register_function(status, 'status')

    #run the rpc server
    try:
        logger.info('WORKER NODE RUNNING ON PORT: %s', str(portNumber))
        server.serve_forever()
    except Exception:
        logger.info('CLOSING THE SERVER')
