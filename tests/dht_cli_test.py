import unittest
import time
import os
import logging
import threading
import json
import random
import base64
import socket
import sys
from datetime import datetime, timedelta
path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(path, '..'))
sys.path.append(os.path.join(path, '../fabnet_core'))
sys.path.append(os.path.join(path, '../fabnet_mgmt'))
sys.path.append(os.path.join(path, '../fabnet_mgmt/tests'))

from cli_test import *
from fabnet.core.constants import ET_INFO, ET_ALERT
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet_dht.constants import DS_NORMALWORK 
from fabnet.core.key_storage import init_keystore

os.environ['FABNET_PLUGINS_CONF'] = 'tests/plugins.yaml'

class TestDHTMgmtCLI(TestMgmtCLI):
    NODES = [('mgmt', 'test_node00'), ('dht', 'test_node01'), ('dht', 'test_node02'), ('dht', 'test_node03')]

    def test08_nodesmgmt_conf_startstop(self):
        cli = pexpect.spawn('telnet 127.0.0.1 8022', timeout=2)
        cli.logfile_read = sys.stdout
        try:
            cli.expect('Username:')
            cli.sendline('nodes-admin')
            cli.expect('Password:')
            cli.sendline('test')
            cli.expect(PROMT)

            TestMgmtCLI.CLI = cli

            self._cmd('start-nodes', 'Usage: START-NODE')
            self._cmd('help start-nodes', 'startnodes')
            self._cmd('start-nodes unkn-node', 'Error! [50] Node "unkn-node" does not found!')
            self._cmd('start-nodes test_node01', ['Starting', 'Done'])
            self._cmd('start-nodes test_node[00-04]', ['Node "test_node04" does not found!'])

            def test_call(nodeaddr, method, params=None):
                if method != 'NodeStatistic':
                    raise Exception('Unexpected method %s'%method)
                return FabnetPacketResponse(ret_code=0, ret_parameters={'DHTInfo':{'status': DS_NORMALWORK}})
            BaseMgmtCLIHandler.mgmtManagementAPI.fri_call_node = test_call

            self._cmd('start-nodes test_node[00-03]', ['Starting', 'Done', 'Waiting'], ['Error'])
        finally:
            cli.sendline('exit')
            cli.expect(pexpect.EOF)
            cli.close(force=True)
            TestMgmtCLI.CLI = None

    def test08_dht_stat(self):
        cli = pexpect.spawn('telnet 127.0.0.1 8022', timeout=2)
        cli.logfile_read = sys.stdout
        try:
            cli.expect('Username:')
            cli.sendline('nodes-admin')
            cli.expect('Password:')
            cli.sendline('test')
            cli.expect(PROMT)

            TestMgmtCLI.CLI = cli
            test_stat = { "DHTInfo" : 
                 { "status" : "normwork", "range_start" : "4000000000000000000000000000000000000000", \
                     "range_end" : "7fffffffffffffffffffffffffffffffffffffff", "free_size_percents" : 40.846089025029386, \
                     "range_size" : 0, "replicas_size" : 0, "free_size" : 102889975808L }}
            MgmtDatabaseManager.MGMT_DB_NAME = 'test_fabnet_mgmt_db'
            dbm = MgmtDatabaseManager('localhost')
            dbm.update_node_stat('externa_addr_test_node:2221', test_stat)

            test_stat = { "DHTInfo" : 
                 { "status" : "preinit", "range_start" : "0000000000000000000000000000000000000000", \
                     "range_end" : "3fffffffffffffffffffffffffffffffffffffff", "free_size_percents" : 10.5089025029386, \
                     "range_size" : 2341230, "replicas_size" : 2110, "free_size" : 10288808L }}

            dbm.update_node_stat('externa_addr_test_node:2222', test_stat)

            test_stat = { "DHTInfo" : 
                 { "status" : "init", "range_start" : "8000000000000000000000000000000000000000", \
                     "range_end" : "ffffffffffffffffffffffffffffffffffffffff", "free_size_percents" : 80.5089025029386, \
                     "range_size" : 834113111160, "replicas_size" : 21123421310, "free_size" : 1008L }}

            dbm.update_node_stat('externa_addr_test_node:2223', test_stat)


            self._cmd('help dht-stat', 'dhtstat')
            self._cmd('dht-stat', ['STATUS', 'SIZE', '80.51'])
        finally:
            cli.sendline('exit')
            cli.expect(pexpect.EOF)
            cli.close(force=True)
            TestMgmtCLI.CLI = None

    def test08_dht_repair(self):
        cli = pexpect.spawn('telnet 127.0.0.1 8022', timeout=2)
        cli.logfile_read = sys.stdout
        try:
            cli.expect('Username:')
            cli.sendline('nodes-admin')
            cli.expect('Password:')
            cli.sendline('test')
            cli.expect(PROMT)

            TestMgmtCLI.CLI = cli

            self._cmd('help repair-dht-data', 'repair-data')
            self._cmd('repair-dht-data', 'Usage:')
            self._cmd('repair-dht-data --full', ['No one online node found'])
            self._cmd('repair-dht-data test_node[00-03]', ['checking data blocks at test_node02', 'Error!'])

            self._cmd('help show-repair-info', 'shrepair')
            self._cmd('show-repair-info', ['NODE', 'FAILED REPAIR'],  ['test_node03'])

            MgmtDatabaseManager.MGMT_DB_NAME = 'test_fabnet_mgmt_db'
            dbm = MgmtDatabaseManager('localhost')
            
            s = 'processed_local_blocks=%s, invalid_local_blocks=%s, \
                    repaired_foreign_blocks=%s, failed_repair_foreign_blocks=%s'
            dbm.notification('externa_addr_test_node:2222', ET_INFO, 'RepairDataBlocks', s%(2342,23,1,0), datetime.now()-timedelta(21))
            dbm.notification('externa_addr_test_node:2222', ET_INFO, 'RepairDataBlocks', s%(631222,432,12,3), datetime.now())
            dbm.notification('externa_addr_test_node:2221', ET_INFO, 'RepairDataBlocks', s%(1212, 1231231,0,123131), datetime.now()-timedelta(24))
            dbm.notification('externa_addr_test_node:2223', ET_ALERT, 'RepairDataBlocks', 'Some error bla bla bla', datetime.now())

            self._cmd('show-repair-info', ['NODE', 'FAILED REPAIR', 'test_node03', '631222', '432', '12', '3', '123131', 'Some error'], ['2342'])
        finally:
            cli.sendline('exit')
            cli.expect(pexpect.EOF)
            cli.close(force=True)
            TestMgmtCLI.CLI = None

    def plugins_test(self, dummy=None):
        pass



if __name__ == '__main__':
    #unittest.main('dht_cli_test.TestDHTMgmtCLI')
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDHTMgmtCLI))
    runner = unittest.TextTestRunner()
    runner.run(suite)

