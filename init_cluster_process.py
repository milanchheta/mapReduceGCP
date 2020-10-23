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
        logger.info("Checking if key value store already exists....")
        res = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                    parser.get('gcp', 'zone'),
                                    parser.get('address', 'keyValueName'))
        logger.info("key value store already exists....")
        return res
    except:
        logger.info("Creating key value store....")
        IPaddr = gcpObj.create_instance(parser.get('gcp', 'project_id'),
                                        parser.get('gcp', 'zone'),
                                        parser.get('address', 'keyValueName'),
                                        parser.get('gcp', 'keyvalue-startup'))
        logger.info("Key value store created....")
        return IPaddr


def spawnSingleWorker(uniqueId, worker, workerQueue, logger):

    gcpObj = GCP()
    try:
        logger.info("Checking if worker-%s already exists....", worker)
        extIP = gcpObj.get_IP_address(
            parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
            parser.get('address', 'workerBaseName') + "-" + uniqueId + "-" +
            str(worker))
        logger.info("Worker-%s already exists....", worker)
        workerQueue.put(extIP)
    except:
        logger.info("Creating worker-%s ....", worker)
        extIP = gcpObj.create_instance(
            parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
            parser.get('address', 'workerBaseName') + "-" + uniqueId + "-" +
            str(worker), parser.get('gcp', 'worker-startup'))
        logger.info("Created worker-%s ....", worker)
        workerQueue.put(extIP)


def spawnWorkers(numOfWorkers, uniqueId, logger):
    nodeAddress = []
    nodeName = []
    tasks = []
    logger.info("Creating %s workers....", numOfWorkers)
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

    logger.info("created workers....")
    return nodeAddress, nodeName


def waitForWorker(worker, ip, logger):
    while True:
        try:
            workerObj = xmlrpc.client.ServerProxy(
                'http://' + worker[ip] + ':' + parser.get('address', 'rpc'),
                allow_none=True)
            if (workerObj.isWorkerConnected() == True):
                break
        except:
            continue
    return


#init cluster api
def init_cluster_process(numberOfMappers, numberOfReducers, logger):

    logger.info("init_cluster() was called....")
    uniqueId = str(uuid.uuid1())
    dataMap = {}

    logger.info("Assigning IP address for kv store")
    kvIp = connectToKVStore(logger)
    dataMap["kvIP"] = kvIp
    logger.info("Assigned KV ip address")

    #CREATE INSTANCES MAPPER EQUALS
    dataMap["workerAddress"] = []
    dataMap["workerName"] = []
    dataMap["n_mappers"] = numberOfMappers
    dataMap["n_reducers"] = numberOfReducers

    nodeAddresses, nodeNames = spawnWorkers(
        max(numberOfMappers, numberOfReducers), uniqueId, logger)
    dataMap["workerAddress"] = nodeAddresses
    dataMap["workerName"] = nodeNames

    logger.info("Waiting for workers nodes to start...")
    tasks = []
    for IP in range(len(dataMap["workerAddress"])):
        p = Process(target=waitForWorker,
                    args=(dataMap["workerAddress"], IP, logger))
        p.start()
        tasks.append(p)
    for task in tasks:
        task.join()
    logger.info("Worker nodes started...")

    logger.info("Storing data in kv store...")
    while True:
        try:
            # initialize by connecting to the kvstore server
            dataStoreObj = xmlrpc.client.ServerProxy(
                'http://' + kvIp + ':' + parser.get('address', 'rpc'),
                allow_none=True)
            if (dataStoreObj.isKvStoreConnected() == True):
                dataStoreObj.DataStore("init " + uniqueId + "\n" +
                                       json.dumps(dataMap) + "\n")
                break
        except:
            logger.info("Kv store not yet started....")
            time.sleep(10)
            continue
    logger.info("Kv store started....")
    return uniqueId
