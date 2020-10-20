import xmlrpc.client
from configparser import ConfigParser
import json
parser = ConfigParser()
parser.read('config.ini')
ipAddress = parser.get('address', 'ip')
portNumber = parser.get('address', 'port')
intm = xmlrpc.client.ServerProxy(('http://' + ipAddress + ':' + portNumber),
                                 allow_none=True)
uniqueId = intm.init_cluster(3, 3)
# res2 = intm.run_mapred(uniqueId, "./Data/test.txt", "InvertedIndexMapper",
#                        "InvertedIndexReducer", "outputPath.txt")
print(uniqueId)