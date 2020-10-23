from gcp import GCP
import uuid
import time
from multiprocessing import Process, Queue
from configparser import ConfigParser
import xmlrpc.client
import json
import os, stat
from math import ceil
parser = ConfigParser()
parser.read('config.ini')


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


def run_mapred_process(uniqueId, inputPath, mapFunction, reducerFunction,
                       outputPath, logger):

    gcpObj = GCP()
    file = "Data/" + uniqueId + "/datamap.json"
    responseMessage = 'get-data' + '\n' + file + '\n'
    dataMap = json.loads(interactWithKv(responseMessage))

    dataMap = inputDataProcessing(uniqueId, inputPath, dataMap, logger)

    # distribute mapper tasks
    logger.info("CALLING %s MAPPERS WITH TASKS", len(dataMap["mapperInput"]))
    start_time = time.time()
    tasks = []
    for worker in dataMap["mapperInput"]:
        p = Process(target=callMapperWorkers,
                    args=(uniqueId, worker, mapFunction, dataMap, logger))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()
    logger.info("ALL MAPPER TASKS DONE WITH EXECUTION TIME: %s",
                time.time() - start_time)

    if (dataMap["n_reducers"] > dataMap["n_mappers"]):
        nodeAddress = dataMap["workerAddress"]
        nodeName = dataMap["workerName"]
        tasks = []
        workerQueue = []
        for i in range(dataMap["n_reducers"], dataMap["n_mappers"]):
            workerQueue.append(Queue())
            p = Process(target=spawnSingleWorker,
                        args=(uniqueId, i, workerQueue[i], logger))
            p.start()
            nodeName.append(
                parser.get('address', 'workerBaseName') + "-" + uniqueId +
                "-" + str(i))
            tasks.append(p)
        for task in tasks:
            task.join()
            pass
        for i in range(len(workerQueue)):
            nodeAddress.append(workerQueue[i].get())

    elif (dataMap["n_reducers"] < dataMap["n_mappers"]):
        for i in range(dataMap["n_reducers"], dataMap["n_mappers"]):
            gcpObj.delete_instance(parser.get('gcp', 'project_id'),
                                   parser.get('gcp', 'zone'),
                                   dataMap["workerName"][i])
    # #combine mapper output
    intermediateCombiner(uniqueId, dataMap, logger)

    # distribute mapper tasks
    logger.info("CALLING %s REDUCERS WITH TASKS", dataMap["n_reducers"])
    start_time = time.time()

    tasks = []
    for worker in range(dataMap["n_reducers"]):
        p = Process(target=callReducerWorkers,
                    args=(uniqueId, worker, reducerFunction, dataMap, logger))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()
    logger.info("ALL REDUCERS TASKS DONE WITH EXECUTION TIME: %s",
                time.time() - start_time)

    # combine and store reducer outbut
    res = combineAndStoreReducerOutput(uniqueId, outputPath, dataMap, logger)

    return res


def inputDataProcessing(uniqueId, inputPath, dataMap, logger):
    logger.info("PROCESSING INPUT DATA AND DIVIDING INTO CHUNKS")
    start_time = time.time()

    dataMap["mapperInput"] = {}
    for i in range(dataMap["n_mappers"]):
        dataMap["mapperInput"][i] = []

    #generate chunks for given input data
    #from directory
    if (os.path.isdir(inputPath)):
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
            logger.info("STORING CHUNKS IN KEY VALUE STORE")

            interactWithKv(data)

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap["mapperInput"][i].append(path)
            i += 1
            j += 1
    #from file
    else:
        if (os.path.isfile(inputPath)):
            f = open(inputPath, 'r')
            file = os.path.basename(inputPath)
            content = f.read()
        else:

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
            logger.info("STORING CHUNKS IN KEY VALUE STORE")
            # #create folder in keyvalue
            interactWithKv(data)

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap["mapperInput"][i].append(path)
            i += 1
            j += 1

    path = "Data/" + uniqueId + "/datamap.json"
    data = 'set-data' + ' ' + path + '\n' + json.dumps(dataMap) + '\n'
    interactWithKv(data)
    logger.info(
        "PROCESSING INPUT DATA AND DIVIDING INTO CHUNKS DONE WITH EXECUTION TIME: %s",
        time.time() - start_time)

    return dataMap


def callMapperWorkers(uniqueId, worker, mapFunction, dataMap, logger):
    gcpObj = GCP()

    for i in range(len(dataMap["mapperInput"][worker])):
        #RETREIVE SAVED MAPPER OBJECT
        while True:
            logger.info("CALLING MAPPER: {} WITH TASK: {}".format(worker, i))
            try:
                workerIp = gcpObj.get_IP_address(
                    parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                    dataMap["workerName"][worker])

                workerObj = xmlrpc.client.ServerProxy(
                    'http://' + workerIp + ':' + parser.get('address', 'rpc'),
                    allow_none=True)
                if (workerObj.isWorkerConnected() == True):
                    #CALL THE MAP WORKER
                    kvIp = gcpObj.get_IP_address(
                        parser.get('gcp', 'project_id'),
                        parser.get('gcp', 'zone'),
                        parser.get('address', 'keyValueName'))
                    p = Process(target=workerObj.worker,
                                args=(uniqueId, worker,
                                      dataMap["mapperInput"][worker][i],
                                      mapFunction, "mapper", kvIp, i))
                    p.start()
                    p.join()
                    logger.info("waiting for a mapper...")
                    if workerObj.status() == "FINISHED":
                        logger.info(
                            "SUCCESS => MAPPER: {} WITH TASK: {}".format(
                                worker, i))
                        break
            except Exception as e:
                logger.info("ERROR => MAPPER: {} WITH TASK: {}".format(
                    worker, i))
                continue
    return


def intermediateCombiner(uniqueId, dataMap, logger):

    logger.info("INTERMEDIATE FUNCTION CALLED TO COMBINE MAPPER OUTPUTS")
    start_time = time.time()

    mapperOutput = []
    for worker in dataMap["mapperInput"]:
        for task in range(len(dataMap["mapperInput"][worker])):
            file = "Data/" + uniqueId + "/mapperOutput/output" + str(
                worker) + str(task) + ".json"
            responseMessage = 'get-data' + '\n' + file + '\n'

            #retrieve from keystore
            jsonData = interactWithKv(responseMessage)
            mapperOutput.append(json.loads(jsonData))

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
    logger.info("STORING INTERMEDIATE DATA FOR REDUCERS")
    for reducer in reducerInput:
        path = "Data/" + uniqueId + "/intermediateOutput/output" + str(
            reducer) + ".json"
        data = 'set-data' + ' ' + path + '\n' + json.dumps(
            reducerInput[reducer]) + '\n'

        #store in keystore
        interactWithKv(data)
    logger.info(
        "INTERMEDIATE FUNCTION CALLED TO COMBINE MAPPER OUTPUTS DONE WITH EXECUTION TIME: %s",
        time.time() - start_time)

    return


def callReducerWorkers(uniqueId, worker, reducerFunction, dataMap, logger):
    gcpObj = GCP()

    # for worker in range(dataMap["n_reducers"]):

    while True:
        logger.info("CALLING REDUCER: %s", worker)
        try:
            workerIp = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                             parser.get('gcp', 'zone'),
                                             dataMap["workerName"][worker])
            workerObj = xmlrpc.client.ServerProxy('http://' + workerIp + ':' +
                                                  parser.get('address', 'rpc'),
                                                  allow_none=True)
            if (workerObj.isWorkerConnected() == True):
                #CALL THE MAP WORKER
                kvIp = gcpObj.get_IP_address(
                    parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                    parser.get('address', 'keyValueName'))
                file = "Data/" + uniqueId + "/intermediateOutput/output" + str(
                    worker) + ".json"
                p = Process(target=workerObj.worker,
                            args=(uniqueId, worker, file, reducerFunction,
                                  "reducer", kvIp))
                p.start()
                p.join()
                if workerObj.status() == "FINISHED":
                    logger.info("SUCCESS => REDUCER: {}".format(worker))
                    break
        except Exception as e:
            logger.info("ERROR => REDUCER: {}".format(worker))
            continue

    return


def combineAndStoreReducerOutput(uniqueId, outputPath, dataMap, logger):
    logger.info("COMBINER FUNCTION CALLED TO COMBINE REDUCER OUTPUTS")
    start_time = time.time()
    reducerOutput = []
    for worker in range(dataMap["n_reducers"]):
        file = "Data/" + uniqueId + "/reducerOutput/output" + str(
            worker) + ".json"
        responseMessage = 'get-data' + '\n' + file + '\n'
        jsonData = interactWithKv(responseMessage)
        reducerOutput.append(json.loads(jsonData))
    output = json.dumps(reducerOutput)
    logger.info("STORING OUTPUT DATA FILE IN MASTER")
    with open(outputPath, 'w') as f:
        f.write(output)
    logger.info("COMBINER FUNCTION CALLED TO COMBINE REDUCER OUTPUTS %s",
                time.time() - start_time)

    return output
