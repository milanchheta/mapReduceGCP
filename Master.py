# -----------------------------------------------------------
# Master Node
# -----------------------------------------------------------
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import json
from multiprocessing import Process, Queue
import os, stat
from math import ceil
from configparser import ConfigParser
import uuid
from gcp import GCP
import logging
import time
import marshal
dataMap = {}


def destroy_cluster(uniqueId):
    gcpObj = GCP()

    kvIp = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                 parser.get('gcp', 'zone'),
                                 parser.get('address', 'keyValueName'))
    dataStoreObj = xmlrpc.client.ServerProxy('http://' + kvIp + ':' +
                                             parser.get('address', 'rpc'),
                                             allow_none=True)
    file = "Data/" + uniqueId + "/datamap.json"
    responseMessage = 'get-data' + '\n' + file + '\n'
    dataMap = json.loads(dataStoreObj.DataStore(responseMessage))

    for worker in dataMap["workerName"]:
        gcpObj.delete_instance(parser.get('gcp', 'project_id'),
                               parser.get('gcp', 'zone'), worker)


def connectToKVStore():
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


def spawnSingleWorker(uniqueId, worker, workerQueue):

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


def spawnWorkers(numOfWorkers, uniqueId):
    nodeAddress = []
    nodeName = []
    tasks = []
    logger.info("Creating %s workers....", numOfWorkers)
    workerQueue = []
    for worker in range(numOfWorkers):
        workerQueue.append(Queue())
        p = Process(target=spawnSingleWorker,
                    args=(uniqueId, worker, workerQueue[worker]))
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


def waitForWorker(worker, ip):
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
def init_cluster(numberOfMappers, numberOfReducers):

    logger.info("init_cluster() was called....")
    uniqueId = str(uuid.uuid1())
    dataMap = {}

    logger.info("Assigning IP address for kv store")
    kvIp = connectToKVStore()
    dataMap["kvIP"] = kvIp
    logger.info("Assigned KV ip address")

    #CREATE INSTANCES MAPPER EQUALS
    dataMap["workerAddress"] = []
    dataMap["workerName"] = []
    dataMap["n_mappers"] = numberOfMappers
    dataMap["n_reducers"] = numberOfReducers

    nodeAddresses, nodeNames = spawnWorkers(
        max(numberOfMappers, numberOfReducers), uniqueId)
    dataMap["workerAddress"] = nodeAddresses
    dataMap["workerName"] = nodeNames

    logger.info("Waiting for workers nodes to start...")
    tasks = []
    for IP in range(len(dataMap["workerAddress"])):
        p = Process(target=waitForWorker, args=(dataMap["workerAddress"], IP))
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


def run_mapred(uniqueId, inputPath, mapFunction, reducerFunction, outputPath):
    logger.info("run_mapred called....")
    gcpObj = GCP()

    kvIp = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                 parser.get('gcp', 'zone'),
                                 parser.get('address', 'keyValueName'))
    dataStoreObj = xmlrpc.client.ServerProxy('http://' + kvIp + ':' +
                                             parser.get('address', 'rpc'),
                                             allow_none=True)
    file = "Data/" + uniqueId + "/datamap.json"
    responseMessage = 'get-data' + '\n' + file + '\n'
    dataMap = json.loads(dataStoreObj.DataStore(responseMessage))

    dataMap = inputDataProcessing(uniqueId, inputPath, dataMap, dataStoreObj)
    logger.info("Input processing done...")

    # distribute mapper tasks
    logger.info("distibuting tasks among mappers...")
    tasks = []
    for worker in dataMap["mapperInput"]:
        logger.info("distibuting a task among mapper number %s...", worker)

        p = Process(target=callMapperWorkers,
                    args=(uniqueId, worker, dataMap["mapperInput"][worker],
                          mapFunction, kvIp, dataMap["workerAddress"][worker],
                          dataMap["workerName"][worker]))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()
    logger.info("All a mapper done...")

    # #combine mapper output
    intermediateCombiner(uniqueId, dataStoreObj, dataMap)

    # distribute reducer tasks
    callReducerWorkers(uniqueId, reducerFunction, kvIp, dataMap)

    # combine and store reducer outbut
    res = combineAndStoreReducerOutput(uniqueId, outputPath, dataMap,
                                       dataStoreObj)

    return res


def combineAndStoreReducerOutput(uniqueId, outputPath, dataMap, dataStoreObj):

    reducerOutput = []
    for worker in range(dataMap["n_reducers"]):
        file = "Data/" + uniqueId + "/reducerOutput/output" + str(
            worker) + ".json"
        responseMessage = 'get-data' + '\n' + file + '\n'
        jsonData = dataStoreObj.DataStore(responseMessage)
        reducerOutput.append(json.loads(jsonData))
    output = json.dumps(reducerOutput)
    with open(outputPath, 'w') as f:
        f.write(output)
    return output


def callReducerWorkers(uniqueId, reducerFunction, kvIp, dataMap):
    gcpObj = GCP()
    tasks = []
    for worker in range(dataMap["n_reducers"]):
        logger.info("calling a reducer...%s", worker)
        #RETREIVE SAVED MAPPER OBJECT
        if (gcpObj.isInstanceRunning(parser.get('gcp', 'project_id'),
                                     parser.get('gcp', 'zone'),
                                     dataMap["workerName"][worker])):
            pass
        else:
            gcpObj.startInstance(parser.get('gcp', 'project_id'),
                                 parser.get('gcp', 'zone'),
                                 dataMap["workerName"][worker])
        while True:
            try:
                workerObj = xmlrpc.client.ServerProxy(
                    'http://' + dataMap["workerAddress"][worker] + ':' +
                    parser.get('address', 'rpc'),
                    allow_none=True)
                if (workerObj.isWorkerConnected() == True):
                    #CALL THE MAP WORKER
                    file = "Data/" + uniqueId + "/intermediateOutput/output" + str(
                        worker) + ".json"
                    p = Process(target=workerObj.worker,
                                args=(uniqueId, worker, file, reducerFunction,
                                      "reducer", kvIp))
                    p.start()
                    tasks.append(p)
                    break
            except:
                continue
    for task in tasks:
        task.join()
    logger.info("reducer task done..")
    return


def intermediateCombiner(uniqueId, dataStoreObj, dataMap):

    logger.info("Called intermediate combiner...")
    mapperOutput = []
    for worker in dataMap["mapperInput"]:
        for task in range(len(dataMap["mapperInput"][worker])):
            file = "Data/" + uniqueId + "/mapperOutput/output" + str(
                worker) + str(task) + ".json"
            responseMessage = 'get-data' + '\n' + file + '\n'

            #retrieve from keystore
            logger.info("Fetching data for intermediate function...")
            jsonData = dataStoreObj.DataStore(responseMessage)
            mapperOutput.append(json.loads(jsonData))

    logger.info("Fetched all data for intermediate function...")
    logger.info("Preparing reducer data..")

    reducerInput = {}
    for i in range(dataMap["n_reducers"]):
        reducerInput[str(i)] = {}
    # processing data
    for mapperTask in mapperOutput:
        for entry in mapperTask:
            hashSum = 0
            for character in entry[0]:
                hashSum += ord(character)
            hashId = hashSum % dataMap["n_reducers"]
            if entry[0] in reducerInput[str(hashId)]:
                reducerInput[str(hashId)][entry[0]] += [entry]
            else:
                reducerInput[str(hashId)][entry[0]] = [entry]
    logger.info("Storing reducer input data..")

    for reducer in reducerInput:
        path = "Data/" + uniqueId + "/intermediateOutput/output" + str(
            reducer) + ".json"
        data = 'set-data' + ' ' + path + '\n' + json.dumps(
            reducerInput[reducer]) + '\n'

        #store in keystore
        dataStoreObj.DataStore(data)
    logger.info("Stored reducer input data..")


def callMapperWorkers(uniqueId, worker, files, mapFunction, kvIp, workerIp,
                      workerName):
    gcpObj = GCP()
    for i in range(len(files)):
        logger.info("calling a mapper with task...%s", i)
        #RETREIVE SAVED MAPPER OBJECT
        if (gcpObj.isInstanceRunning(parser.get('gcp', 'project_id'),
                                     parser.get('gcp', 'zone'), workerName)):
            pass
        else:
            gcpObj.startInstance(parser.get('gcp', 'project_id'),
                                 parser.get('gcp', 'zone'), workerName)
        while True:
            try:
                workerObj = xmlrpc.client.ServerProxy(
                    'http://' + workerIp + ':' + parser.get('address', 'rpc'),
                    allow_none=True)
                if (workerObj.isWorkerConnected() == True):
                    #CALL THE MAP WORKER
                    p = Process(target=workerObj.worker,
                                args=(uniqueId, worker, files[i], mapFunction,
                                      "mapper", kvIp, i))
                    p.start()
                    p.join()
                    logger.info("waiting for a mapper...")
                    break
            except:
                continue

    logger.info("tasks for a mapper is done...")
    return


def inputDataProcessing(uniqueId, inputPath, dataMap, dataStoreObj):
    logger.info("processing input....")
    dataMap["mapperInput"] = {}
    for i in range(dataMap["n_mappers"]):
        dataMap["mapperInput"][i] = []

    #generate chunks for given input data
    #from directory
    if (os.path.isdir(inputPath)):
        logger.info("processing input directory....")

        allFiles = os.listdir(inputPath)
        i = 0
        j = 0
        for file in allFiles:
            if i == dataMap["n_mappers"]:
                i = 0
            f = open(inputPath + file, 'r')
            data = {}
            data[file] = f.read()
            path = "Data/" + uniqueId + "/chunks/chunk" + str(j) + ".json"
            data = 'set-data' + ' ' + path + '\n' + json.dumps(data) + '\n'

            ##STORE CHUNKS IN KEYVALUE STORE
            logger.info("storing chunks in kv store....")
            dataStoreObj.DataStore(data)
            logger.info("stored chunks in kv store....")

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap["mapperInput"][i].append(path)
            i += 1
            j += 1
    #from file
    else:
        if (os.path.isfile(inputPath)):
            logger.info("processing input file....")
            f = open(inputPath, 'r')
            file = os.path.basename(inputPath)
            content = f.read()
        else:
            logger.info("processing input string....")

            content = inputPath
            file = "InputString"
        content = content.split()
        chunksize = ceil(len(content) / dataMap["n_mappers"])
        chunk = ""
        s = 0
        res = []
        remData = ""

        j = len(content)
        for i in range(0, len(content), chunksize):
            j = i + chunksize
            chunk = " ".join(content[i:i + chunksize])
            res.append(chunk)
        if j < len(content):
            res.append(" ".join(content[i:i + chunksize]))
        i = 0
        j = 0
        for chunk in res:
            if i == dataMap["n_mappers"]:
                i = 0
            data = {}
            data[file] = chunk
            path = "Data/" + uniqueId + "/chunks/chunk" + str(j) + ".json"
            data = 'set-data' + ' ' + path + '\n' + json.dumps(data) + '\n'

            ##STORE CHUNKS IN KEYVALUE STORE
            logger.info("storing chunks in kv store....")
            # #create folder in keyvalue
            dataStoreObj.DataStore(data)
            logger.info("stored chunks in kv store....")

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap["mapperInput"][i].append(path)
            i += 1
            j += 1

    path = "Data/" + uniqueId + "/datamap.json"
    data = 'set-data' + ' ' + path + '\n' + json.dumps(dataMap) + '\n'
    dataStoreObj.DataStore(data)
    return dataMap


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('config.ini')
    log_dir = './'
    os.chmod(log_dir, stat.S_IRWXU)
    logger = logging.getLogger('master-node')
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler('master.log')
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2', )

    port = int(parser.get('address', 'rpc'))
    server = SimpleXMLRPCServer(("", port),
                                requestHandler=RequestHandler,
                                allow_none=True)

    server.register_introspection_functions()
    server.register_function(run_mapred, 'run_mapred')
    server.register_function(init_cluster, 'init_cluster')
    server.register_function(destroy_cluster, 'destroy_cluster')
    # res1 = init_cluster(2, 3)
    # print(res1)
    # res2 = run_mapred(res1, "./Data/test.txt", "InvertedIndexMapper.py",
    #                   "InvertedIndexReducer.py", "outputPath.txt")
    # run the rpc server
    try:
        logger.info('Master running on port %s', str(port))
        server.serve_forever()
    except Exception:
        server.server_close()
        logger.info('Closing the server')
