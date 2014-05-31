#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.client_delete

@author Konstantin Andrusenko
@date June 16, 2013
"""
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock

from fabnet_dht.key_utils import KeyUtils

class ClientDeleteOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME = 'ClientDeleteData'

    def init_locals(self):
        self.node_name = self.operator.get_node_name()

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.str_get('key')
        replica_count = packet.int_get('replica_count')
        KeyUtils.validate(key)

        keys = KeyUtils.generate_new_keys(self.node_name, replica_count, prime_key=key)
        errors = []
        if packet.role == NODE_ROLE:
            user_id_hash = packet.str_get('user_id_hash', '')
        else:
            user_id_hash = hashlib.sha1(str(packet.user_id)).hexdigest()

        for i, key in enumerate(keys):
            cur_dbct = FSMappedDHTRange.DBCT_MASTER if i == 0 else FSMappedDHTRange.DBCT_REPLICA
            h_range = self.operator.find_range(key)
            if not h_range:
                errors.append('No hash range found for key=%s!'%key)
                continue

            _, _, node_address = h_range
            params = {'key': key, 'dbct': cur_dbct, 'carefully_delete': True, \
                        'user_id_hash': user_id_hash}

            resp = self._init_operation(node_address, 'DeleteDataBlock', params, sync=True)
            if resp.ret_code != RC_OK:
                errors.append('DeleteDataBlock failed at %s: %s'%(node_address, resp.ret_message))

        if errors:
            ret_code = RC_ERROR
        else:
            ret_code = RC_OK

        return FabnetPacketResponse(ret_code=ret_code, ret_message='\n'.join(errors))


