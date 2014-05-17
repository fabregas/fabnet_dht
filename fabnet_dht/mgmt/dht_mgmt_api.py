
import time
from fabnet_mgmt.engine.decorators import MgmtApiMethod
from fabnet_mgmt.engine.constants import ROLE_RO, ROLE_NM, \
            DBK_ID, DBK_NODETYPE, DBK_STATUS, DBK_NODEADDR, STATUS_UP, \
            DBK_NOTIFY_TYPE, DBK_NOTIFY_MSG, DBK_NOTIFY_MSG, DBK_NOTIFY_DT

from fabnet_mgmt.engine import nodes_mgmt
from fabnet.core.constants import ET_INFO
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



@MgmtApiMethod(ROLE_NM)
def repair_data(engine, session_id, nodes_list=[], log=None):
    if not nodes_list:
        nodes = engine.db_mgr().get_fabnet_nodes({DBK_STATUS: STATUS_UP})
        nodes = nodes.limit(1)
        if nodes.count() == 0:
            if log:
                log.write('No one online node found\n')
            return

        rcode, rmsg = engine.fri_call_net(nodes[0][DBK_NODEADDR], 'RepairDataBlocks')
        if rcode:
            msg = 'Unable to call RepairDataBlocks operation over fabnet. Details: %s\n'%rmsg
            raise Exception(msg)
        return

    nodes_objs = nodes_mgmt.get_nodes_objs(engine, nodes_list)
    for node_obj in nodes_objs:
        if log:
            log.write(' -> checking data blocks at %s ...\n'%node_obj[DBK_ID])
        ret_packet = engine.fri_call_node(node_obj[DBK_NODEADDR], 'RepairDataBlocks')
        if ret_packet.ret_code and log:
            log.write('  Error! %s\n'%ret_packet.ret_message)


@MgmtApiMethod(ROLE_RO)
def get_repair_info(engine, session_id):
    nodes_objs = nodes_mgmt.get_nodes_objs(engine, [])
    ret_list = []
    for node_obj in nodes_objs:
        if node_obj[DBK_NODETYPE].upper() != 'DHT':
            continue

        recs = engine.db_mgr().get_notifications(node=node_obj[DBK_NODEADDR], \
                                    n_topic='RepairDataBlocks', limit=1)
        if recs.count() == 0:
            continue

        rec = recs[0]
        node_name = node_obj[DBK_ID]
        ret_msg = ''
        local_bc = local_inv = rep_bc = rep_fail_bc = '---'
        if rec[DBK_NOTIFY_TYPE] == ET_INFO:
            parts = rec[DBK_NOTIFY_MSG].split(',')
            for part in parts:
                s_name, s_val = part.strip().split('=')
                s_name = s_name.strip()
                s_val = s_val.strip()
                if s_name == 'processed_local_blocks':
                    local_bc = s_val
                elif s_name == 'invalid_local_blocks':
                    local_inv = s_val
                elif s_name == 'repaired_foreign_blocks':
                    rep_bc = s_val
                elif s_name == 'failed_repair_foreign_blocks':
                    rep_fail_bc = s_val
        else:
            ret_msg = rec[DBK_NOTIFY_MSG]

        nd = {'node_name': node_name, 'ret_msg': ret_msg, 'dt': rec[DBK_NOTIFY_DT], \
                'local_bc': local_bc, 'local_invalid_bc': local_inv, \
                'repaired_bc': rep_bc, 'fail_repaired_bc': rep_fail_bc}
        ret_list.append(nd)
    return ret_list

