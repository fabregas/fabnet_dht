#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.get_data_keys

@author Konstantin Andrusenko
@date May 25, 2014
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange

class GetKeysInfoOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME='GetKeysInfo'

    def init_locals(self):
        self.node_name = self.operator.get_node_name()

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.parameters.get('key', None)
        replica_count = packet.parameters.get('replica_count', None)
        if key is not None:
            KeyUtils.validate(key)

            if replica_count is None:
                return FabnetPacketResponse(ret_code=RC_ERROR,
                        ret_message='Replica count should be passed to GetKeysInfo operation')

            keys = KeyUtils.get_all_keys(key, replica_count)
        else:
            key = KeyUtils.generate_key(self.node_name)
            keys = [key]

        msg = ''
        ret_keys = []
        for i, key in enumerate(keys):
            cur_dbct = FSMappedDHTRange.DBCT_MASTER if i == 0 else FSMappedDHTRange.DBCT_REPLICA
            long_key = KeyUtils.validate(key)
            range_obj = self.operator.find_range(long_key)
            if not range_obj:
                msg += '[GetKeysInfoOperation] Internal error: No hash range found for key=%s! \n'%key
            else:
                _, _, node_address = range_obj
                ret_keys.append((key, cur_dbct, node_address))

        return FabnetPacketResponse(ret_parameters={'keys_info': ret_keys}, ret_message=msg)


