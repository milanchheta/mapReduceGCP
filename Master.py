# -----------------------------------------------------------
# Master Node
# -----------------------------------------------------------
##imports
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from multiprocessing import Process, Queue

import os, stat
from configparser import ConfigParser
import uuid
from gcp import GCP
import logging
import time

import init_cluster_process
import run_mapred_process
import destroy_cluster_process


def init_cluster(numberOfMappers, numberOfReducers):
    logger.info('init_cluster FUNCTION CALLED')
    start_time = time.time()

    uniqueId = init_cluster_process.init_cluster_process(
        numberOfMappers, numberOfReducers, logger)
    logger.info('init_cluster FUNCTION TASK COMPLETED')
    logger.info('TIME TAKEN BY init_cluster: %s', time.time() - start_time)

    return uniqueId


def run_mapred(uniqueId, inputPath, mapFunction, reducerFunction, outputPath):
    logger.info('run_mapred FUNCTION CALLED')
    start_time = time.time()
    res = run_mapred_process.run_mapred_process(uniqueId, inputPath,
                                                mapFunction, reducerFunction,
                                                outputPath, logger)
    logger.info('run_mapred FUNCTION TASK COMPLETED')
    logger.info('TIME TAKEN BY run_mapred: %s', time.time() - start_time)
    return res


def destroy_cluster(uniqueId):
    logger.info('destroy_cluster FUNCTION CALLED')
    start_time = time.time()

    destroy_cluster_process.destroy_cluster_process(uniqueId, logger)
    logger.info('destroy_cluster FUNCTION TASK COMPLETED')
    logger.info('TIME TAKEN BY destroy_cluster: %s', time.time() - start_time)
    return


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
    logger.info('STARTING MASTER NODE')

    try:
        logger.info('MASTER NODE RUNNING ON PORT: %s', str(port))
        server.serve_forever()
    except Exception:
        server.server_close()
        logger.info('CLOSING THE SERVER')
