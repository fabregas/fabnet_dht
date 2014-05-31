#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.put_object_part

@author Konstantin Andrusenko
@date May 31, 2014
"""
import os
import hashlib
import shutil
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse, FabnetPacketRequest
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.constants import MIN_REPLICA_COUNT
from fabnet_dht.key_utils import KeyUtils

class PutObjectPartOperation(OperationBase):
    ROLES = [CLIENT_ROLE]
    NAME = 'PutObjectPart'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        obj_path = packet.str_get('obj_path')
        seek = packet.int_get('seek', 0)
        replica_count = packet.int_get('replica_count', MIN_REPLICA_COUNT)

        put_data_block = self.get_operation_object('ClientPutData')
        ret = put_data_block.process(packet)
        if ret.ret_code != RC_OK:
            return ret

        user_id_hash = hashlib.sha1(str(packet.user_id)).hexdigest()
        try:
            h_range = self.operator.find_range(user_id_hash)
            if not h_range:
                raise Exception('No hash range found for key=%s!'%user_id_hash)

            add_list = [(obj_path, [(ret.ret_parameters['key'], replica_count, seek, ret.ret_parameters['size'])])]
            params = {'user_id_hash': user_id_hash, 'add_list': add_list, 'rm_list': []}
            _, _, node_address = h_range

            resp = self._init_operation(node_address, 'UpdateMetadata', params, sync=True)
            if resp.ret_code != RC_OK:
                raise Exception('UpdateMetadata failed at %s: %s'%(node_address, resp.ret_message))
        except Exception, err:
            ret = self.delete_dbs(ret.ret_parameters['key'], replica_count, user_id_hash)
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err)+ret)

        return ret


    def delete_dbs(self, master_key, replica_count, user_id_hash):
        try:
            delete_data = self.get_operation_object('ClientDeleteData')
            d_packet = FabnetPacketRequest(method='ClientDeleteData', \
                    parameters={'key': master_key, 'replica_count': replica_count, \
                                'user_id_hash': user_id_hash})
            d_packet.role = NODE_ROLE
            ret = delete_data.process(d_packet)

            if ret.ret_code != RC_OK:
                raise Exception(ret.ret_message)
        except Exception, e:
            err = '\nDelete saved DBs error: %s'%e
            return err
        return ''
