import time
import sys
import os
import logging
import shutil
import threading
import json
import random
import string
import hashlib

sys.path.append('fabnet_core')
sys.path.append('tests/manual')

from fabnet.core import constants
constants.KEEP_ALIVE_MAX_WAIT_TIME = 5
constants.CHECK_NEIGHBOURS_TIMEOUT = 1
constants.FRI_CLIENT_TIMEOUT = 2

from fabnet.utils.safe_json_file import SafeJsonFile
from test_monitor import NOTIFICATIONS_DB
from fabnet.core.key_storage import init_keystore

from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.fri_base import RamBasedBinaryData
from fabnet.core.constants import RC_OK, RC_ERROR, NT_SUPERIOR, NT_UPPER, ET_INFO, ET_ALERT, RC_PERMISSION_DENIED
from fabnet.core.config import Config
from fabnet.utils.db_conn import PostgresqlDBConnection as DBConnection
from fabnet.core.node import Node
from fabnet.core.operator import OperatorClient
from fabnet.operations.manage_neighbours import ManageNeighbour
from fabnet.operations.discovery_operation import DiscoveryOperation
from fabnet.operations.topology_cognition import TopologyCognition

from fabnet.utils.logger import core_logger as logger
from fabnet_dht.constants import *
from fabnet_dht.data_block import DataBlockHeader
from fabnet_dht import constants
from fabnet_dht.hash_ranges_table import HashRangesTable
from fabnet_dht.dht_operator import DHTOperator
from fabnet_dht import dht_operator
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange


#logger.setLevel(logging.DEBUG)
NODE1_KS = 'tests/ks/node00.p12'
NODE2_KS = 'tests/ks/node01.p12'
PASSWD = 'node'
USER1_KS = 'tests/ks/user1.p12'
USER2_KS = 'tests/ks/user2.p12'
USER_PWD = 'user'

MAX_KEY = constants.MAX_KEY

os.environ['FABNET_PLUGINS_CONF'] = 'tests/plugins.yaml'

class TestServerThread(threading.Thread):
    def __init__(self, port, home_dir, neighbour=None, is_monitor=False, \
                                config={}, ks_path='', clear_home=True):
        threading.Thread.__init__(self)
        if clear_home and os.path.exists(home_dir):
            shutil.rmtree(home_dir)
        if not os.path.exists(home_dir):
            os.mkdir(home_dir)

        self.port = port
        self.home_dir = home_dir
        self.stopped = True
        self.operator = None
        self.neighbour = neighbour
        self.config = config
        self.is_monitor = is_monitor
        self.ks_path = ks_path
        self.ks = None if not ks_path else init_keystore(ks_path, PASSWD)
        self.__lock = threading.Lock()

    def run(self):
        os.system('cp ./tests/ks/certs.ca %s'%self.home_dir)
        address = '127.0.0.1:%s'%self.port
        if self.is_monitor:
            node_type = 'TestMonitor'
        else:
            node_type = 'DHT'

        config = {'INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT': 0.1,
                 'MONITOR_DHT_RANGES_TIMEOUT': 1,
                 'CHECK_HASH_TABLE_TIMEOUT': 1,
                 'WAIT_FILE_MD_TIMEDELTA': 0.1,
                 'WAIT_DHT_TABLE_UPDATE': 0.2}
        config.update(self.config)

        node = Node('127.0.0.1', self.port, self.home_dir, 'node_%s'%self.port,
                    ks_path=self.ks_path, ks_passwd=PASSWD, node_type=node_type, config=config)
        node.set_auth_key(None)
        node.start(self.neighbour)

        self.__lock.acquire()
        try:
            self.operator = OperatorClient('node_%s'%self.port)
            self.stopped = False
        finally:
            self.__lock.release()

        while not self.stopped:
            time.sleep(0.1)

        node.stop()

    def stop(self):
        self.stopped = True
        self.join()

    def wait_oper_status(self, status, errmsg=''):
        for i in xrange(20):
            if self.get_status() == status:
                return
            time.sleep(.25)
        raise Exception('[%s].wait_oper_status(%s) timeouted! %s'%(self.port, status, errmsg))

    def get_range_dir(self):
        return os.path.join(self.home_dir, 'dht_range/%s'%FSMappedDHTRange.DBCT_MASTER)

    def get_replicas_dir(self):
        return os.path.join(self.home_dir, 'dht_range/%s'%FSMappedDHTRange.DBCT_REPLICA)

    def get_tmp_dir(self):
        return os.path.join(self.home_dir, 'dht_range/%s'%FSMappedDHTRange.DBCT_TEMP)

    def get_stat(self):
        packet_obj = FabnetPacketRequest(method='NodeStatistic')

        client = FriClient(self.ks)
        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj)
        if ret_packet.ret_code != 0:
            raise Exception('cant get node statistic. details: %s'%ret_packet.ret_message)
        return ret_packet.ret_parameters

    def get_status(self):
        self.__lock.acquire()
        try:
            if self.operator is None:
                return None
            return self.operator.get_status()
        finally:
            self.__lock.release()

    def put_data_block(self, data, key, dbct=FSMappedDHTRange.DBCT_MASTER, user_id_hash='', \
                careful_save=False, init_block=False, stored_unixtime=None):

        params = {'key': key, 'dbct': dbct, 'user_id_hash': user_id_hash, 'init_block': init_block,\
                'carefully_save': careful_save, 'stored_unixtime': stored_unixtime}

        req = FabnetPacketRequest(method='PutDataBlock',\
                            binary_data=data, parameters=params)

        client = FriClient(self.ks)
        resp = client.call_sync('127.0.0.1:%s'%self.port, req)
        return resp


    def put(self, data, wait_writes=3, init_block=True, client_ks=None, key=None, replica_count=2):
        if not client_ks:
            client_ks = self.ks
        client = FriClient(client_ks)

        params = {'wait_writes_count': wait_writes, 'replica_count': replica_count, \
                'init_block': init_block, 'key': key}
        data = RamBasedBinaryData(data, 20)
        packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data, sync=True)
        #print '========SENDING DATA BLOCK %s (%s chunks)'%(packet_obj, data.chunks_count())

        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj)
        #print '========SENDED DATA BLOCK'
        return ret_packet

    def get_keys_info(self, key, replica_count=2):
        client = FriClient(self.ks)
        packet = FabnetPacketRequest(method='GetKeysInfo', \
                parameters={'key': key, 'replica_count': replica_count}, sync=True)

        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet)
        if ret_packet.ret_code != 0:
            raise Exception('GetKeysInfo error: %s'%ret_packet.ret_message)
        keys_info = ret_packet.ret_parameters['keys_info']
        return keys_info

    def get_data_block(self, key, dbct=FSMappedDHTRange.DBCT_MASTER, client_ks=None, user_id_hash=''):
        params = {'key': key, 'dbct': dbct, 'user_id_hash': user_id_hash}
        req = FabnetPacketRequest(method='GetDataBlock', parameters=params)

        client = FriClient(client_ks)
        resp = client.call_sync('127.0.0.1:%s'%self.port, req)
        return resp

    def delete(self, key, client_ks=None, replica_count=2):
        client = FriClient(client_ks)
        params = {'key': key, 'replica_count': replica_count}
        packet_obj = FabnetPacketRequest(method='ClientDeleteData', parameters=params, sync=True)

        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj)
        return ret_packet

    def check_data_block(self, key, dbct=FSMappedDHTRange.DBCT_MASTER, checksum=None):
        client = FriClient(self.ks)
        params = {'key': key, 'dbct': dbct, 'checksum': checksum}
        packet_obj = FabnetPacketRequest(method='CheckDataBlock', parameters=params)
        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj) 
        return ret_packet






    def get(self, key, client_ks=None):
        client = FriClient(client_ks)

        params = {'key': key, 'replica_count': 2}
        packet_obj = FabnetPacketRequest(method='ClientGetData', parameters=params, sync=True)

        ret_packet = client.call_sync('127.0.0.1:%s'%self.port, packet_obj)
        return ret_packet


