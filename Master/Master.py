# -----------------------------------------------------------
# Master Node
# -----------------------------------------------------------
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import json
from multiprocessing import Process
import os
from math import ceil
from configparser import ConfigParser
import uuid

dataMap = {}


def destroy_cluster(uniqueId):
    #destory vms dataMap[uniqueId]["workerAddress"]
    #disconnect from kv store dataMap[uniqueId]["kvObj"]
    #clear data from kv store dataMap[uniqueId]["kvObj"]  Data/uniqueId/
    pass


#init cluster api
def init_cluster(numberOfMappers, numberOfReducers):
    global dataMap
    uniqueId = str(uuid.uuid1())
    dataMap[uniqueId] = {}

    # initialize by connecting to the kvstore server
    # dataStoreObj = xmlrpc.client.ServerProxy(
    #     'http://' + parser.get('address', 'keyValueIp') + ':' +
    #     parser.get('address', 'keyValuePort'),
    #     allow_none=True)
    # workerObj = xmlrpc.client.ServerProxy('http://localhost:9002',
    #                                       allow_none=True)

    #CREATE INSTANCES MAPPER EQUALS
    #STORE INSTANCES DATA IN dataMap[uniqueId]["workerAddress"] dataMap[uniqueId]["workerName"]
    dataMap[uniqueId]["workerAddress"] = [0, 1, 2]
    dataMap[uniqueId]["workerName"] = [0, 1, 2]

    #CONNECT TO INSTANCES
    #STORE CLIENT OBJ IN dataMap[uniqueId]["workerObj"]
    # dataMap[uniqueId]["workerObj"] = [workerObj, workerObj, workerObj]
    dataMap[uniqueId]["workerStatus"] = ['idle', 'idle', 'idle']

    #initialize data
    dataMap[uniqueId]["mapperInput"] = {}
    dataMap[uniqueId]["n_mappers"] = numberOfMappers
    dataMap[uniqueId]["n_reducers"] = numberOfReducers
    # dataMap[uniqueId]["kvObj"] = dataStoreObj

    #create folder in keyvalue
    dataMap[uniqueId]["kvObj"].DataStore("init " + uniqueId + "\n")

    # return uniqueId
    return uniqueId


def run_mapred(uniqueId, inputPath, mapFunction, reducerFunction, outputPath):
    inputDataProcessing(uniqueId, inputPath)

    # distribute mapper tasks
    tasks = []
    for worker in dataMap[uniqueId]["mapperInput"]:
        p = Process(target=callMapperWorkers,
                    args=(uniqueId, worker,
                          dataMap[uniqueId]["mapperInput"][worker],
                          mapFunction))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()

    #combine mapper output
    intermediateCombiner(uniqueId)

    ##ADJUST VMS
    if dataMap[uniqueId]["n_reducers"] < dataMap[uniqueId]["n_mappers"]:
        #remove vms
        pass
    elif dataMap[uniqueId]["n_reducers"] < dataMap[uniqueId]["n_mappers"]:
        #add vms
        pass

    #distribute reducer tasks
    callReducerWorkers(uniqueId, reducerFunction)

    #combine and store reducer outbut
    res = combineAndStoreReducerOutput(uniqueId, outputPath)

    return res


def combineAndStoreReducerOutput(uniqueId, outputPath):
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

    tasks = []
    for worker in range(dataMap[uniqueId]["n_reducers"]):
        file = "Data/" + uniqueId + "/intermediateOutput/output" + str(
            worker) + ".json"
        # Retrieve worker object
        obj = dataMap[uniqueId]["workerObj"][worker]
        #call reducer worker and change status
        p = Process(target=obj.worker,
                    args=(uniqueId, worker, file, reducerFunction, "reducer"))
        p.start()
        tasks.append(p)
    for task in tasks:
        task.join()
    #change status when done
    return


def intermediateCombiner(uniqueId):
    mapperOutput = []
    for worker in dataMap[uniqueId]["mapperInput"]:
        for task in range(len(dataMap[uniqueId]["mapperInput"][worker])):
            file = "Data/" + uniqueId + "/mapperOutput/output" + str(
                worker) + str(task) + ".json"
            responseMessage = 'get-data' + '\n' + file + '\n'

            #retrieve from keystore
            jsonData = dataMap[uniqueId]["kvObj"].DataStore(responseMessage)
            mapperOutput.append(json.loads(jsonData))
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

        for reducer in reducerInput:
            path = "Data/" + uniqueId + "/intermediateOutput/output" + str(
                reducer) + ".json"
            data = 'set-data' + ' ' + path + '\n' + json.dumps(
                reducerInput[reducer]) + '\n'

            #store in keystore
            dataMap[uniqueId]["kvObj"].DataStore(data)


def callMapperWorkers(uniqueId, worker, files, mapFunction):
    tasks = []
    for i in range(len(files)):

        #RETREIVE SAVED MAPPER OBJECT
        obj = dataMap[uniqueId]["workerObj"][worker]

        #CALL THE MAP WORKER AND CHANGE STATUS TO BUSY
        p = Process(target=obj.worker,
                    args=(uniqueId, worker, files[i], mapFunction, "mapper",
                          i))
        p.start()
        tasks.append(p)

    for task in tasks:
        task.join()

    #CHANGE STATUS WHEN DONE

    return


def inputDataProcessing(uniqueId, inputPath):

    global dataMap
    for i in range(dataMap[uniqueId]["n_mappers"]):
        dataMap[uniqueId]["mapperInput"][i] = []
    #generate chunks for given input data
    #from directory
    if (os.path.isdir(inputPath)):
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
            dataMap[uniqueId]["kvObj"].DataStore(data)

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap[uniqueId]["mapperInput"][i].append(path)
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
            dataMap[uniqueId]["kvObj"].DataStore(data)

            #SAVE FILENAME IN ARR OF MAPPER INPUT
            dataMap[uniqueId]["mapperInput"][i].append(path)
            i += 1
            j += 1


if __name__ == '__main__':
    parser = ConfigParser()

    # parser.read('config.ini')


    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2', )

    ipAddress = ""
    portNumber = 9000
    server = SimpleXMLRPCServer((ipAddress, portNumber),
                                requestHandler=RequestHandler,
                                allow_none=True)
    server.register_introspection_functions()
    server.register_function(run_mapred, 'run_mapred')
    server.register_function(init_cluster, 'init_cluster')

    #run the rpc server
    try:
        print('Master running')
        server.serve_forever()
    except Exception:
        server.server_close()
        print('Closing the server')
