from gcp import GCP
import uuid
import time
from multiprocessing import Process, Queue
from configparser import ConfigParser
import xmlrpc.client
import json
parser = ConfigParser()
parser.read('config.ini')


def destroy_cluster_process(uniqueId, logegr):
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