# -----------------------------------------------------------
# KeyValue store to save data and files
# -----------------------------------------------------------
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from _thread import *
import json
import os, stat
import logging
from configparser import ConfigParser


def isKvStoreConnected():
    return True


#set function of key value store
def set(arguements):
    line1 = arguements[0]
    line2 = arguements[1]
    key = line1.split(' ')[1]
    valueSize = line1.split(' ')[2]
    while thread_lock.locked() == True:
        continue
    thread_lock.acquire()

    value = line2
    res = 'NOT-STORED\r\n'

    if key.isalnum(
    ) and len(key) <= 250 and len(value) <= 250 and ' ' not in key:
        try:
            res = 'STORED\r\n'
            data = open('keyValue.json', )
            jsondata = json.load(data)
            jsondata[key] = {"value": value, "size": valueSize}
            json.dump(jsondata, open("keyValue.json", "w"))
            data.close()
        except:
            res = 'NOT-STORED\r\n'
    thread_lock.release()
    return (res)


#get function of the keyvalue store to get value of specified key
def get(arguements):
    line1 = arguements[0]
    key = line1[1]
    res = ''
    while thread_lock.locked() == True:
        continue
    thread_lock.acquire()
    data = open('keyValue.json', )
    data = json.load(data)
    if key in data:
        value = data[key]["value"] + '\r\n'
        size = str(data[key]["size"]) + '\r\n'
    else:
        value = '\r\n'
        size = '0' + '\r\n'
    res = 'VALUE' + ' ' + key + ' ' + size + value
    thread_lock.release()

    return (res)


##FUNCTION USED TO FETCH STORED DATA BY MAPREDUCE PROCESSES
def getData(arguements):
    logger.info("called getData")
    filepath = arguements[1]
    logger.info("getting data at path: %s", filepath)
    while thread_lock.locked() == True:
        continue
    thread_lock.acquire()
    try:
        d = open('./' + filepath, )
        data = json.load(d)
        data = json.dumps(data)
        logger.info("fetched data")
    except:
        logger.info("Error getting data")

    thread_lock.release()
    logger.info("done getData")
    return data


def setData(arguements):
    logger.info("called setData")
    path = arguements[0].split()[1]
    logger.info("storing data at path: %s", path)
    value = arguements[1]
    while thread_lock.locked() == True:
        continue
    thread_lock.acquire()
    res = 'NOT-STORED\r\n'
    try:
        res = 'NOT-STORED\r\n'
        dataValue = json.loads(value)
        with open("./" + path, 'w') as jsonFile:
            json.dump(dataValue, jsonFile)
            res = 'STORED\r\n'
        logger.info("stored data")
    except:
        logger.info("Error storing data")

        res = 'NOT-STORED\r\n'
    thread_lock.release()
    return res


def initFolders(arguements):
    logger.info("called initFolders")
    id = arguements[0].split()[1]
    datamap = json.loads(arguements[1])
    if (os.path.exists("./Data")):
        pass
    else:
        os.mkdir("./Data")

    path = "./Data/" + str(id)
    if (os.path.exists(path)):
        pass
    else:
        os.mkdir(path)
        with open("./" + path + "/datamap.json", 'w') as jsonFile:
            json.dump(datamap, jsonFile)
        os.mkdir(path + "/chunks")
        os.mkdir(path + "/mapperOutput")
        os.mkdir(path + "/intermediateOutput")
        os.mkdir(path + "/reducerOutput")

    logger.info("done initFolders")
    return True


#main keyvalue store function
def DataStore(message):
    logger.info("called DataStore")
    message = (message.split('\n'))
    res = []
    for line in message:
        res.append(line)
    if len(res[0]) > 1:
        command = res[0].split(' ')[0]
    else:
        command = res[0]
    return functionLookup[command](res)


if __name__ == '__main__':
    log_dir = './'
    os.chmod(log_dir, stat.S_IRWXU)
    logger = logging.getLogger('kvs-node')
    logger.setLevel(logging.DEBUG)

    fh = logging.FileHandler('kvs.log')
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
    server.register_function(DataStore, 'DataStore')
    server.register_function(getData, 'getData')
    server.register_function(set, 'set')
    server.register_function(get, 'get')
    server.register_function(isKvStoreConnected, 'isKvStoreConnected')

    thread_lock = allocate_lock()
    functionLookup = {
        "get": get,
        "set": set,
        "init": initFolders,
        'get-data': getData,
        'set-data': setData
    }

    #run the rpc server
    try:
        logger.info('KeyValueStore running')
        server.serve_forever()
    except Exception:
        logger.info('Error while running the server')
