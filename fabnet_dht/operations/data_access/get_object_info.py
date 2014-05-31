#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.get_object_info

@author Konstantin Andrusenko
@date May 31, 2014
"""
import os
import copy
import hashlib
import shutil
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange
from fabnet_dht.constants import MIN_REPLICA_COUNT
from fabnet_dht.key_utils import KeyUtils

class GetObjectInfoOperation(OperationBase):
    ROLES = [CLIENT_ROLE, NODE_ROLE]
    NAME = 'GetObjectInfo'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        o_path = packet.str_get('obj_path')
        req_user_info = packet.bool_get('req_user_info', False)
        replica_count = packet.int_get('md_replica_count', MIN_REPLICA_COUNT)
        get_key = packet.str_get('get_key', '')

        if packet.role == NODE_ROLE:
            user_id_hash = packet.str_get('user_id_hash', '')
        else:
            user_id_hash = hashlib.sha1(str(packet.user_id)).hexdigest()
        keys = KeyUtils.get_all_keys(user_id_hash, replica_count)
        errors = []
        for i, key in enumerate(keys):
            if get_key and get_key != key:
                continue

            cur_dbct = FSMappedDHTRange.DBCT_MD_MASTER if i == 0 else FSMappedDHTRange.DBCT_MD_REPLICA
            h_range = self.operator.find_range(key)
            if not h_range:
                errors.append('No hash range found for key %s!'%key)
                continue
 
            _, _, node_address = h_range
            if self.self_address == node_address:
                try:
                    p_info = self.get_path_info(key, cur_dbct, o_path, req_user_info)
                    return FabnetPacketResponse(ret_parameters=p_info)
                except Exception, err:
                    errors.append(str(err))
                    continue

            params = copy.copy(packet.parameters)
            params['get_key'] = key
            params['user_id_hash'] = user_id_hash
            resp = self._init_operation(node_address, 'GetObjectInfo', params, sync=True)
            if resp.ret_code != RC_OK:
                errors.append('Get path info error at %s: %s'% (node_address, resp.ret_message))
            else:
                return resp

        return FabnetPacketResponse(ret_code=RC_ERROR, ret_message = '\n'.join(errors))

    def get_path_info(self, key, dbct, o_path, req_user_info):
        resp = {}
        db_path = self.operator.get_db_path(key, dbct)
        if req_user_info:
            user_info = self.operator.user_metadata_call(db_path, 'get_user_info')
            user_info_d = {}
            user_info_d['storage_size'] = user_info.storage_size
            user_info_d['used_size'] = user_info.used_size
            user_info_d['flags'] = user_info.flags
            resp['user_info'] = user_info_d

        path_info = self.operator.user_metadata_call(db_path, 'get_path_info', o_path)
        resp['path_info'] = path_info.to_dict()
        
        if path_info.path_type == 'file':
            data_blocks = self.operator.user_metadata_call(db_path, 'get_data_blocks', o_path) 
            resp['data_blocks'] = []
            for db in data_blocks:
                resp['data_blocks'].append(db.to_dict())

        return resp

