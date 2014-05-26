#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.data_access.update_user_profile

@author Konstantin Andrusenko
@date May 22, 2014
"""
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse, BinaryDataPointer
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.constants import MIN_REPLICA_COUNT
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlockHeader
from fabnet_dht.fs_mapped_ranges import TmpFile

def key_to_int(key):
    try:
        key = str(key)
        if len(key) != 40:
            raise ValueError()
        return long(key, 16)
    except Exception:
        raise Exception('Invalid key "%s"'%key)

class UpdateUserProfileOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'UpdateUserProfile'

    def init_locals(self):
        self.node_name = self.operator.get_node_name()

    def process(self, packet):
        """
        @param packet - object of FabnetPacketRequest class
            packet.parameters description:
                * user_id - user ID
                * storage_size - size in bytes of available for user
                * bin_flags - 32-bit integer that can contain custom user's flags
                * md_replica_count - count of metadata replica (MIN_REPLICA_COUNT id None)
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        #user_id = packet.parameters.get('user_id', None)
        #if not user_id:
        #    return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='user_id does not found!')
        user_id = packet.str_get('user_id')

        user_id_hash = hashlib.sha1(user_id).hexdigest()

        replica_count = packet.int_param('md_replica_count', MIN_REPLICA_COUNT)
        storage_size = packet.int_param('storage_size')
        bin_flags = packet.int_param('bin_flags', 0)
        only_local_save = packet.bool_param('only_local_save', False)

        keys = KeyUtils.get_all_keys(user_id_hash, replica_count)
        saved_cnt = 0
        for i, key in enumerate(keys):
            is_replica = i > 0
            h_range = self.operator.find_range(key)
            if not h_range:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No hash range found for key %s!'%key)
            
            if self.self_address == node_address:
                self.update_user_profile(key, user_id, replica_count, storage_size, bin_flags)
                saved_cnt += 1
                continue

            if only_local_save:
                continue

            _, _, node_address = h_range
            params = copy.copy(packet.parameters)
            params['is_replica'] = is_replica
            params['only_local_save'] = True
            resp = self._init_operation(node_address, 'UpdateUserProfile', params, sync=True)
            if resp.ret_code != RC_OK:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Update user profile error at %s: %s'% \
                                        (node_address, resp.ret_message))

            saved_cnt += 1

        if only_local_save and saved_cnt == 0:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No local key for user_id=%s found!'%user_id)

        return FabnetPacketResponse()

    def update_user_profile(self, key, user_id, replica_count, storage_size, bin_flags):
        pass

