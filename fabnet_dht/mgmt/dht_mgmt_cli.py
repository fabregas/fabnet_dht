
from fabnet_mgmt.cli.decorators import cli_command
from fabnet_mgmt.cli.utils import parse_nodes
from fabnet_dht.constants import *

DHT_STATUSES_MAP = {DS_PREINIT: 'PERINIT', DS_INITIALIZE: 'INIT', \
        DS_NORMALWORK: 'OK', DS_DESTROYING: 'DESTROY'}

KB = 1000
MB = 1000*KB
GB = 1000*MB
TB = 1000*GB

def norm_size(size):
    if size == '---':
        return size
    size = long(size)
    if size < KB:
        return '%i B'%size
    elif size < MB:
        return '%.2f KB'%((size+0.0)/KB)
    elif size < GB:
        return '%.2f MB'%((size+0.0)/MB)
    elif size < TB:
        return '%.2f GB'%((size+0.0)/GB)
    else:
        return '%.2f TB'%((size+0.0)/TB)


@cli_command(50, 'dht-stat', 'get_nodes_stat', 'dhtstat')
def command_dht_stat(cli, params):
    '''
    Show DHT statistic
    Fields description:
        NODE: fabnet node name
        STATUS: status in DHT network
        DHT RANGE: DHT keys range for node
        M SIZE: master data size
        R SIZE: replica data size
        FREE SIZE: free space on node
        FREE SIZE %: fre space on node in percents
    '''
    stats = cli.mgmtManagementAPI.get_nodes_stat(cli.session_id)

    cli.writeresponse('-'*100)
    cli.writeresponse('%-20s %s %s %s %s %s %s'%('   DHT RANGE', 'NODE'.center(15), \
                        'STATUS'.center(12), 'M SIZE'.center(12), 'R SIZE'.center(12), \
                        'FREE SIZE'.center(12), 'FREE SIZE %'.center(12) ))
    cli.writeresponse('-'*100)

    def key_func(node):
        rstart = stats[node].get('DHTInfo', {}).get('range_start', 'f'*41)
        return int(rstart, 16)

    for node in sorted(stats.keys(), key=key_func):
        n_stat = stats[node]
        dht_i = n_stat.get('DHTInfo', {})
        if not dht_i:
            continue

        status = dht_i.get('status', 'unknown')
        status = DHT_STATUSES_MAP.get(status, status)

        dht_range = "%s* - %s*"%(dht_i.get('range_start', '*'*5)[:6], \
                                dht_i.get('range_end', '*'*5)[:6])

        range_size = norm_size(dht_i.get('range_size', '---'))
        replicas_size = norm_size(dht_i.get('replicas_size', '---'))
        free_size = norm_size(dht_i.get('free_size', '---'))
        free_size_percents = dht_i.get('free_size_percents', '---')
        if free_size_percents != '---':
            free_size_percents = '%.2f %%'%free_size_percents

        cli.writeresponse('%-20s %s %s %s %s %s %s'%(dht_range, node.center(15), \
                    status.center(12), range_size.center(12), replicas_size.center(12), \
                    free_size.center(12), free_size_percents.center(12) ))



@cli_command(51, 'repair-dht-data', 'repair_data', 'repair-data', 'repairdata')
def command_repair_data(cli, params):
    '''[<node(s)>]
    Check data blocks and repair corrupted (or lost) data
    If no nodes specified - check and repair all data on all DHT nodes asynchronously
    Arguments in the <node(s)> list may include normal nodes names, a range of names in hostlist format.
    '''
    if params:
        nodes_list = parse_nodes(params[0])
    else:
        nodes_list = []
    cli.mgmtManagementAPI.repair_data(cli.session_id, nodes_list, log=cli)


@cli_command(52, 'show-repair-info', 'get_repair_info', 'showrepairinfo', 'shrepair')
def command_show_repair_info(cli, params):
    '''
    Show repair information
    This command shows information about last repair process on DHT nodes
    Fields description:
        NODE: fabnet node name
        DATETIME: last repair finish date and time
        LOCAL CNT: count of checked local data blocks
        INVALID CNT: count of invalid local data blocks
        REPAITED: count of repaired invalid data blocks
        FAILED REPAIR: count of fails while repairing data blocks
    '''
    rep_info = cli.mgmtManagementAPI.get_repair_info(cli.session_id)
    cli.writeresponse('-'*100)
    cli.writeresponse('%-15s %s %s %s %s %s'%('NODE', 'DATETIME'.center(15),\
                        'LOCAL CNT'.center(15), 'INVALID CNT'.center(15), \
                        'REPAIRED'.center(15), 'FAILED REPAIR'.center(15)))
    cli.writeresponse('-'*100)

    err_list = []
    for rec in sorted(rep_info, key=lambda i: i['node_name']):
        dt = rec['dt'].strftime('%d.%m.%y %H:%M')
        cli.writeresponse('%-15s %s %s %s %s %s'%(rec['node_name'], dt.center(15),\
                        str(rec['local_bc']).center(15), str(rec['local_invalid_bc']).center(15), \
                        str(rec['repaired_bc']).center(15), str(rec['fail_repaired_bc']).center(15)))
        if rec['ret_msg']:
            err_list.append((rec['node_name'], rec['ret_msg']))
    if err_list:
        cli.writeresponse('\nERROR MESSAGES:')
        for n_name, err_msg in err_list:
            cli.writeresponse('[%s]: %s'%(n_name, err_msg))

