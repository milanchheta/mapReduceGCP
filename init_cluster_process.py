from gcp import GCP
import uuid
import time
from multiprocessing import Process, Queue
from configparser import ConfigParser
import xmlrpc.client
import json
parser = ConfigParser()
parser.read('config.ini')


def connectToKVStore(logger):
    gcpObj = GCP()
    try:
        logger.info("CHECKING IF KEY VALUE STORE VM INSTANCE EXISTS")
        res = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                    parser.get('gcp', 'zone'),
                                    parser.get('address', 'keyValueName'))
        return res
    except:
        logger.info("CREATING THE KEY VALUE STORE VM INSTANCE")
        IPaddr = gcpObj.create_instance(parser.get('gcp', 'project_id'),
                                        parser.get('gcp', 'zone'),
                                        parser.get('address', 'keyValueName'),
                                        parser.get('gcp', 'keyvalue-startup'))
        return IPaddr


def spawnSingleWorker(uniqueId, worker, workerQueue, logger):
    gcpObj = GCP()
    logger.info("CREATING WORKER VM INSTANCE: %s", worker)
    try:
        extIP = gcpObj.get_IP_address(
            parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
            parser.get('address', 'workerBaseName') + "-" + uniqueId + "-" +
            str(worker))
        workerQueue.put(extIP)
    except:
        extIP = gcpObj.create_instance(
            parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
            parser.get('address', 'workerBaseName') + "-" + uniqueId + "-" +
            str(worker), parser.get('gcp', 'worker-startup'))
        workerQueue.put(extIP)


def spawnWorkers(numOfWorkers, uniqueId, logger):
    logger.info("CREATING %s WORKER VM INSTANCES", numOfWorkers)

    nodeAddress = []
    nodeName = []
    tasks = []
    workerQueue = []
    for worker in range(numOfWorkers):
        workerQueue.append(Queue())
        p = Process(target=spawnSingleWorker,
                    args=(uniqueId, worker, workerQueue[worker], logger))
        p.start()
        tasks.append(p)
    for task in tasks:
        task.join()

    for i in range(len(workerQueue)):
        nodeAddress.append(workerQueue[i].get())
        nodeName.append(
            parser.get('address', 'workerBaseName') + "-" + uniqueId + "-" +
            str(i))
    return nodeAddress, nodeName


def waitForWorker(worker, ip, logger):
    logger.info("WAITING FOR WORKER %s TO START THE RPC SERVER.", worker)
    while True:
        try:
            workerObj = xmlrpc.client.ServerProxy(
                'http://' + worker[ip] + ':' + parser.get('address', 'rpc'),
                allow_none=True)
            if (workerObj.isWorkerConnected() == True):
                return
        except:
            continue


#init cluster api
def init_cluster_process(numberOfMappers, numberOfReducers, logger):
    uniqueId = str(uuid.uuid1())
    dataMap = {}

    kvIp = connectToKVStore(logger)

    dataMap["workerAddress"] = []
    dataMap["workerName"] = []
    dataMap["n_mappers"] = numberOfMappers
    dataMap["n_reducers"] = numberOfReducers

    nodeAddresses, nodeNames = spawnWorkers(numberOfMappers, uniqueId, logger)
    dataMap["workerAddress"] = nodeAddresses
    dataMap["workerName"] = nodeNames

    tasks = []
    for IP in range(len(dataMap["workerAddress"])):
        p = Process(target=waitForWorker,
                    args=(dataMap["workerAddress"], IP, logger))
        p.start()
        tasks.append(p)
    for task in tasks:
        task.join()

    logger.info("STORING INITIAL CLIENT DATA AND UUID IN KEYVALUE STORE")
    interactWithKv("init " + uniqueId + "\n" + json.dumps(dataMap) + "\n")
    return uniqueId


def interactWithKv(responseMessage):
    gcpObj = GCP()
    while True:
        try:
            kvIp = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                         parser.get('gcp', 'zone'),
                                         parser.get('address', 'keyValueName'))
            dataStoreObj = xmlrpc.client.ServerProxy(
                'http://' + kvIp + ':' + parser.get('address', 'rpc'),
                allow_none=True)
            if (dataStoreObj.isKvStoreConnected() == True):
                res = dataStoreObj.DataStore(responseMessage)
                return res
        except:
            continue