
import time

from fabnet_mgmt.engine.decorators import MgmtApiMethod
from fabnet_mgmt.engine.constants import ROLE_RO, ROLE_NM, \
                            DBK_NODEADDR, DBK_NODETYPE

from fabnet_mgmt.engine import nodes_mgmt
from fabnet_dht.constants import DS_NORMALWORK 

START_DHT_FLAG = False

@MgmtApiMethod(ROLE_NM)
def start_nodes(engine, session_id, nodes_list=[], log=None, wait_routine=None, reboot=False):
    global START_DHT_FLAG
    START_DHT_FLAG = False

    def wait_routine(index, node_obj):
        if node_obj[DBK_NODETYPE].upper() != 'DHT':
            return

        global START_DHT_FLAG
        if not START_DHT_FLAG:
            START_DHT_FLAG = True
            return

        if log:
            log.write('Waiting for DHT initialization ...\n')

        for i in xrange(30):
            ret_packet = engine.fri_call_node(node_obj[DBK_NODEADDR], 'NodeStatistic')

            if ret_packet.ret_code:
                time.sleep(.5)
                continue
            if ret_packet.ret_parameters['DHTInfo']['status'] != DS_NORMALWORK:
                time.sleep(.5)
                continue
            break
        else:
            if log:
                log.write('Node does not initialized at DHT member ...\n')

    return nodes_mgmt.start_nodes(session_id, nodes_list, log, wait_routine, reboot)


