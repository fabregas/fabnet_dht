#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.data_access.update_user_profile

@author Konstantin Andrusenko
@date May 22, 2014
"""
import os
import copy
import hashlib

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse, BinaryDataPointer
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE

from fabnet_dht.constants import MIN_REPLICA_COUNT
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange

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

    def process(self, packet):
        """
        @param packet - object of FabnetPacketRequest class
            packet.parameters description:
                * user_id_hash - sha1 hash of user ID
                * storage_size - size in bytes of available for user
                * bin_flags - 32-bit integer that can contain custom user's flags
                * md_replica_count - count of metadata replica (MIN_REPLICA_COUNT id None)
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        user_id_hash = packet.str_get('user_id_hash')

        replica_count = packet.int_get('md_replica_count', MIN_REPLICA_COUNT)
        storage_size = packet.int_get('storage_size')
        bin_flags = packet.int_get('bin_flags', 0)
        save_key = packet.str_get('save_key', '')

        keys = KeyUtils.get_all_keys(user_id_hash, replica_count)
        saved_cnt = 0
        for i, key in enumerate(keys):
            if save_key and key != save_key:
                continue

            cur_dbct = FSMappedDHTRange.DBCT_MD_MASTER if i == 0 else FSMappedDHTRange.DBCT_MD_REPLICA
            h_range = self.operator.find_range(key)
            if not h_range:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No hash range found for key %s!'%key)
            
            _, _, node_address = h_range
            if self.self_address == node_address:
                self.update_user_profile(keys[0], key, cur_dbct, user_id_hash, replica_count, storage_size, bin_flags)
                saved_cnt += 1
                continue

            params = copy.copy(packet.parameters)
            params['save_key'] = key
            resp = self._init_operation(node_address, 'UpdateUserProfile', params, sync=True)
            if resp.ret_code != RC_OK:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Update user profile error at %s: %s'% \
                                        (node_address, resp.ret_message))

            saved_cnt += 1

        if save_key and saved_cnt == 0:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No local key for user_id=%s found!'%user_id)

        return FabnetPacketResponse()

    def update_user_profile(self, master_key, key, dbct, user_id_hash, replica_count, storage_size, bin_flags):
        db_path = self.operator.get_db_path(key, dbct)
        user_info = self.operator.user_metadata_call(db_path, 'get_user_info')
        user_info.user_id_hash = user_id_hash
        user_info.storage_size += storage_size
        user_info.flags = bin_flags
        self.operator.user_metadata_call(db_path, 'update_user_info', user_info)

        db_header = DataBlockHeader(master_key, replica_count, '', user_id_hash)
        with DataBlock(os.path.join(db_path, 'dht_info')) as tmp_db:
            tmp_db.write(db_header.pack(), seek=0)
            tmp_db.close()

