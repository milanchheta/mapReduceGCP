from gcp import GCP
import uuid
import time
from multiprocessing import Process, Queue
from configparser import ConfigParser
import xmlrpc.client
import json
from init_cluster_process import interactWithKv
parser = ConfigParser()
parser.read('config.ini')


def destroy_cluster_process(uniqueId):
    interactWithKv("delete-data " + uniqueId + "\n")
