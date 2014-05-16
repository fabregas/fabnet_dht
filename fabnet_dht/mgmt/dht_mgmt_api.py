
import time

from fabnet_mgmt.engine.decorators import MgmtApiMethod
from fabnet_mgmt.engine.constants import ROLE_RO, ROLE_NM, DBK_NODEADDR

from fabnet_mgmt.engine import fabnet_mgmt
from fabnet_dht.constants import DS_NORMALWORK 

@MgmtApiMethod(ROLE_NM)
def start_nodes(engine, session_id, nodes_list=[], log=None, wait_routine=None):
    def wait_routine(index, node_obj):
        if index == 0:
            return
        client = FriClient()
        for i in xrange(100):
            ret_packet = engine.fri_call_node(node_obj[DBK_NODEADDR], 'NodeStatistic')
            if ret_packet.ret_code:
                time.sleep(.5)
                continue
            if ret_packet.ret_parameters['DHTInfo']['status'] != DS_NORMALWORK:
                time.sleep(.5)
                continue
            break

    return fabnet_mgmt.start_nodes(engine, session_id, nodes_list, log, wait_routine)

                

