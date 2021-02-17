from kazoo.client import KazooClient
import random

zkhosts = 'c5oc51:2181,c5oc52:2181,c5oc53:2181'
zk = KazooClient(hosts=zkhosts, read_only=True)
zk.start()
hanodes = zk.get_children('/hiveserver2')
if (len(hanodes) == 0):
    print("no hiveserver2 alive. exit here.")
rannode = zk.get('/hiveserver2/' + hanodes[random.randint(0, len(hanodes) - 1)])
nodeinfo = rannode[0].decode().split(';')
nodedict = {k: v for k, v in (x.split('=') for x in nodeinfo)}
host = nodedict.get('hive.server2.thrift.bind.host')
port = nodedict.get('hive.server2.thrift.port')
print(host + ':' + port)
zk.stop()
