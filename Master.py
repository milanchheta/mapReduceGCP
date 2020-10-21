# -----------------------------------------------------------
# Master Node
# -----------------------------------------------------------
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import json
from multiprocessing import Process, Queue
import os
from math import ceil
from configparser import ConfigParser
import uuid
from gcp import GCP
import logging
import time

dataMap = {}


def destroy_cluster(uniqueId):
    gcpObj = GCP()
    global dataMap
    for worker in dataMap[uniqueId]["workerName"]:
        gcpObj.delete_instance(parser.get('gcp', 'project_id'),
                               parser.get('gcp', 'zone'), worker)


def connectToKVStore():
    gcpObj = GCP()
    res = gcpObj.get_IP_address(parser.get('gcp', 'project_id'),
                                parser.get('gcp', 'zone'),
                                parser.get('address', 'keyValueName'))
    if res == "error":
        logger.info("Creating key value store instance....")
        while True:
            try:
                logger.info("creating key value store....")

                operation = gcpObj.create_instance(
                    parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                    parser.get('address', 'keyValueName'),
                    parser.get('gcp', 'keyvalue-startup'))

                waitResponse = gcpObj.wait_for_operation(
                    parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                    operation["name"])

                if waitResponse != "error":
                    logger.info("key value store created....")
                    return gcpObj.get_IP_address(
                        parser.get('gcp', 'project_id'),
                        parser.get('gcp', 'zone'),
                        parser.get('address', 'keyValueName'))
                else:
                    logger.info("error creating key value store....")

            except Exception as e:
                raise e
    else:
        logger.info("key value store exists....")
        return res


def spawnWorkers(worker, workerQueue):
    gcpObj = GCP()
    while True:
        try:
            extIP = gcpObj.get_IP_address(
                parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                parser.get('address', 'workerBaseName') + str(worker))
            if extIP == "error":
                logger.info("creating a worker")
                operation = gcpObj.create_instance(
                    parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                    parser.get('address', 'workerBaseName') + str(worker),
                    parser.get('gcp', 'worker-startup'))
                waitResponse = gcpObj.wait_for_operation(
                    parser.get('gcp', 'project_id'), parser.get('gcp', 'zone'),
                    operation["name"])
                if waitResponse != "error":
                    logger.info("created a worker")
                    extIP = gcpObj.get_IP_address(
                        parser.get('gcp', 'project_id'),
                        parser.get('gcp', 'zone'),
                        parser.get('address', 'workerBaseName') + str(worker))
                    workerQueue.put(extIP)
                    break
            else:
                logger.info("worker exists")
                workerQueue.put(extIP)
                break
        except Exception as e:
            logger.info("error creating a worker")
            raise e


#init cluster api
def init_cluster(numberOfMappers, numberOfReducers):
    logger.info("init_cluster called....")
    global dataMap
    uniqueId = str(uuid.uuid1())
    dataMap[uniqueId] = {}

    logger.info("Checking for key value store....")
    kvIp = connectToKVStore()

    logger.info("Assigned KV ip address")

    logger.info("Connecting to kv store...")
    while True:
        try:
            # initialize by connecting to the kvstore server
            dataStoreObj = xmlrpc.client.ServerProxy(
                'http://' + kvIp + ':' + parser.get('address', 'rpc'),
                allow_none=True)
            break
        except:
            logger.info("Error Connecting to kv store...")
            continue
    dataMap[uniqueId]["kvObj"] = dataStoreObj
    dataMap[uniqueId]["kvIP"] = kvIp
    logger.info("Connected to kv store...")

    #CREATE INSTANCES MAPPER EQUALS
    dataMap[uniqueId]["workerAddress"] = []
    dataMap[uniqueId]["workerName"] = []
    dataMap[uniqueId]["workerObj"] = []
    tasks = []
    logger.info("creating workers....")

    for worker in range(max(numberOfMappers, numberOfReducers)):
        workerQueue = Queue()
        p = Process(target=spawnWorkers, args=(worker, workerQueue))
        p.start()
        p.join()
        dataMap[uniqueId]["workerAddress"].append(workerQueue.get())
        dataMap[uniqueId]["workerName"].append(
            parser.get('address', 'workerBaseName') + str(worker))
    logger.info("created workers....")

    logger.info("Connecting to workers....")
    for IP in dataMap[uniqueId]["workerAddress"]:
        while True:
            try:
                logger.info("connecting to a worker....")
                dataMap[uniqueId]["workerObj"].append(
                    (xmlrpc.client.ServerProxy(
                        'http://' +
                        dataMap[uniqueId]["workerAddress"][worker] + ':' +
                        parser.get('address', 'rpc'),
                        allow_none=True)))
                logger.info("connected to a worker....")
                break
            except:
                logger.info("error connecting to a worker....")
                continue
    logger.info("Connected to workers....")

    #initialize data
    dataMap[uniqueId]["mapperInput"] = {}
    dataMap[uniqueId]["n_mappers"] = numberOfMappers
    dataMap[uniqueId]["n_reducers"] = numberOfReducers

    while True:
        try:
            logger.info("initializing kv folders....")
            # #create folder in keyvalue
            dataMap[uniqueId]["kvObj"].DataStore("init " + uniqueId + "\n")
            break
        except:
            time.sleep(15)
            logger.info("error initializing kv folders....")
            continue
    logger.info("kv folders initialized....")
    return uniqueId


def run_mapred(uniqueId, inputPath, mapFunction, reducerFunction, outputPath):
    global dataMap
    logger.info("run_mapred called....")

    inputDataProcessing(uniqueId, inputPath)

    # distribute mapper tasks
    logger.info("distibuting tasks among mappers...")
    tasks = []
    for worker in dataMap[uniqueId]["mapperInput"]:
        logger.info("distibuting a task among mapper number %s...", worker)

        p = Process(target=callMapperWorkers,
                    args=(uniqueId, worker,
                          dataMap[uniqueId]["mapperInput"][worker],
                          mapFunction))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()
    logger.info("All a mapper done...")

    #combine mapper output
    time.sleep(15)

    intermediateCombiner(uniqueId)
    time.sleep(15)

    # ##ADJUST VMS
    # if dataMap[uniqueId]["n_reducers"] < dataMap[uniqueId]["n_mappers"]:
    #     # [n_reducers:]
    #     #remove vms

    #     pass
    # elif dataMap[uniqueId]["n_reducers"] > dataMap[uniqueId]["n_mappers"]:
    #     #add vms
    #     # [n_mappers:n_reducers]

    #     pass

    #distribute reducer tasks
    callReducerWorkers(uniqueId, reducerFunction)

    #combine and store reducer outbut
    res = combineAndStoreReducerOutput(uniqueId, outputPath)

    return res


def combineAndStoreReducerOutput(uniqueId, outputPath):
    global dataMap
    reducerOutput = []
    for worker in range(dataMap[uniqueId]["n_reducers"]):
        file = "Data/" + uniqueId + "/reducerOutput/output" + str(
            worker) + ".json"
        responseMessage = 'get-data' + '\n' + file + '\n'
        jsonData = dataMap[uniqueId]["kvObj"].DataStore(responseMessage)
        reducerOutput.append(json.loads(jsonData))
    output = json.dumps(reducerOutput)
    with open(outputPath, 'w') as f:
        f.write(output)
    return output


def callReducerWorkers(uniqueId, reducerFunction):
    global dataMap
    tasks = []
    for worker in range(dataMap[uniqueId]["n_reducers"]):
        file = "Data/" + uniqueId + "/intermediateOutput/output" + str(
            worker) + ".json"
        # Retrieve worker object
        obj = dataMap[uniqueId]["workerObj"][worker]
        #call reducer worker and change status
        p = Process(target=obj.worker,
                    args=(uniqueId, worker, file, reducerFunction, "reducer",
                          dataMap[uniqueId]["kvIP"]))
        p.start()
        tasks.append(p)
    for task in tasks:
        task.join()
    #change status when done
    return


def intermediateCombiner(uniqueId):
    global dataMap
    print(dataMap)

    logger.info("Called intermediate combiner...")
    mapperOutput = []
    for worker in dataMap[uniqueId]["mapperInput"]:
        for task in range(len(dataMap[uniqueId]["mapperInput"][worker])):
            file = "Data/" + uniqueId + "/mapperOutput/output" + str(
                worker) + str(task) + ".json"
            responseMessage = 'get-data' + '\n' + file + '\n'

            #retrieve from keystore
            logger.info("Fetching data for intermediate function...")
            jsonData = dataMap[uniqueId]["kvObj"].DataStore(responseMessage)
            mapperOutput.append(json.loads(jsonData))

    logger.info("Fetched all data for intermediate function...")
    logger.info("Preparing reducer data..")

    reducerInput = {}
    for i in range(dataMap[uniqueId]["n_reducers"]):
        reducerInput[str(i)] = {}
    # processing data
    for mapperTask in mapperOutput:
        for entry in mapperTask:
            hashSum = 0
            for character in entry[0]:
                hashSum += ord(character)
            hashId = hashSum % dataMap[uniqueId]["n_reducers"]
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
        dataMap[uniqueId]["kvObj"].DataStore(data)
    logger.info("Stored reducer input data..")


def callMapperWorkers(uniqueId, worker, files, mapFunction):
    global dataMap

    for i in range(len(files)):
        logger.info("calling a mapper with task...%s", i)

        #RETREIVE SAVED MAPPER OBJECT
        obj = dataMap[uniqueId]["workerObj"][worker]

        #CALL THE MAP WORKER AND CHANGE STATUS TO BUSY
        p = Process(target=obj.worker,
                    args=(uniqueId, worker, files[i], mapFunction, "mapper",
                          dataMap[uniqueId]["kvIP"], i))
        p.start()
        p.join()
        logger.info("waiting for a mapper...")
    logger.info("tasks for a mapper is done...")

    return


def inputDataProcessing(uniqueId, inputPath):
    logger.info("processing input....")

    global dataMap
    for i in range(dataMap[uniqueId]["n_mappers"]):
        dataMap[uniqueId]["mapperInput"][i] = []
    #generate chunks for given input data
    #from directory
    if (os.path.isdir(inputPath)):
        logger.info("processing input directory....")

        allFiles = os.listdir(inputPath)
        i = 0
        j = 0
        for file in allFiles:
            if i == dataMap[uniqueId]["n_mappers"]:
                i = 0
            f = open(inputPath + file, 'r')
            data = {}
            data[file] = f.read()
            path = "Data/" + uniqueId + "/chunks/chunk" + str(j) + ".json"
            data = 'set-data' + ' ' + path + '\n' + json.dumps(data) + '\n'

            ##STORE CHUNKS IN KEYVALUE STORE
            while True:
                try:
                    logger.info("storing chunks in kv store....")

                    # #create folder in keyvalue
                    dataMap[uniqueId]["kvObj"].DataStore(data)
                    break
                except:
                    logger.info("error storing chunks in kv store....")
                    time.sleep(15)

                    continue
                    logger.info("stored chunks in kv store....")

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap[uniqueId]["mapperInput"][i].append(path)
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
        chunksize = ceil(len(content) / dataMap[uniqueId]["n_mappers"])
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
            if i == dataMap[uniqueId]["n_mappers"]:
                i = 0
            data = {}
            data[file] = chunk
            path = "Data/" + uniqueId + "/chunks/chunk" + str(j) + ".json"
            data = 'set-data' + ' ' + path + '\n' + json.dumps(data) + '\n'

            ##STORE CHUNKS IN KEYVALUE STORE
            while True:
                try:
                    logger.info("storing chunks in kv store....")

                    # #create folder in keyvalue
                    dataMap[uniqueId]["kvObj"].DataStore(data)
                    break
                except:
                    logger.info("error storing chunks in kv store....")
                    time.sleep(15)

                    continue
            logger.info("stored chunks in kv store....")

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap[uniqueId]["mapperInput"][i].append(path)
            i += 1
            j += 1


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('config.ini')

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
    # res2 = run_mapred(res1, "./Data/test.txt", "WordCountMapper",
    #                   "WordCountReducer", "outputPath.txt")
    # destroy_cluster(res1)

    #run the rpc server
    try:
        logger.info('Master running on port %s', str(port))
        server.serve_forever()
    except Exception:
        server.server_close()
        logger.info('Closing the server')
