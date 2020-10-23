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
import init_cluster_process
import run_mapred_process
import destroy_cluster_process


def destroy_cluster(uniqueId):
    destroy_cluster_process.destroy_cluster_process(uniqueId.logger)
    return


def run_mapred(uniqueId, inputPath, mapFunction, reducerFunction, outputPath):
    res = run_mapred_process.run_mapred_process(uniqueId, inputPath,
                                                mapFunction, reducerFunction,
                                                outputPath, logger)
    return res


def init_cluster(numberOfMappers, numberOfReducers):
    uniqueId = init_cluster_process.init_cluster_process(
        numberOfMappers, numberOfReducers, logger)
    return uniqueId


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

    try:
        logger.info('Master running on port %s', str(port))
        server.serve_forever()
    except Exception:
        server.server_close()
        logger.info('Closing the server')
