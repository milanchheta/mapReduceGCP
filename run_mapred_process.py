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


def run_mapred_process(uniqueId, inputPath, mapFunction, reducerFunction,
                       outputPath, logger):
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

    dataMap = inputDataProcessing(uniqueId, inputPath, dataMap, dataStoreObj,
                                  logger)
    logger.info("Input processing done...")

    # distribute mapper tasks
    logger.info("distibuting tasks among mappers...")
    tasks = []
    for worker in dataMap["mapperInput"]:
        logger.info("distibuting a task among mapper number %s...", worker)

        p = Process(target=callMapperWorkers,
                    args=(uniqueId, worker, dataMap["mapperInput"][worker],
                          mapFunction, kvIp, dataMap["workerAddress"][worker],
                          dataMap["workerName"][worker], logger))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()
    logger.info("All a mapper done...")

    # #combine mapper output
    intermediateCombiner(uniqueId, dataStoreObj, dataMap, logger)

    # distribute reducer tasks
    callReducerWorkers(uniqueId, reducerFunction, kvIp, dataMap, logger)

    # combine and store reducer outbut
    res = combineAndStoreReducerOutput(uniqueId, outputPath, dataMap,
                                       dataStoreObj, logger)

    return res


def combineAndStoreReducerOutput(uniqueId, outputPath, dataMap, dataStoreObj,
                                 logger):

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


def callReducerWorkers(uniqueId, reducerFunction, kvIp, dataMap, logger):
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


def intermediateCombiner(uniqueId, dataStoreObj, dataMap, logger):

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
                      workerName, logger):
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


def inputDataProcessing(uniqueId, inputPath, dataMap, dataStoreObj, logger):
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