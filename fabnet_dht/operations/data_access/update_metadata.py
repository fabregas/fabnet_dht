#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.data_access.update_metadata

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

from fabnet_dht.constants import MIN_REPLICA_COUNT, RC_MD_NOFREESPACE, RC_MD_NOTINIT
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange
from fabnet_dht.user_metadata import MDDataBlockInfo, MDNoFreeSpace, MDNotInit


class UpdateMetadataOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'UpdateMetadata'

    def process(self, packet):
        """
        @param packet - object of FabnetPacketRequest class
            packet.parameters description:
                * user_id_hash - sha1 hash of user ID
                * add_list - list of (f_path, [(db_key, rcnt, size),...])
                * rm_list - list of f_path
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        user_id_hash = packet.str_get('user_id_hash')
        key = packet.str_get('key', '')
        KeyUtils.validate(user_id_hash)

        add_list = packet.parameters.get('add_list', [])
        rm_list = packet.parameters.get('rm_list', [])

        try:
            return self.try_update(user_id_hash, key, add_list, rm_list, packet)
        except MDNoFreeSpace, err:
            return FabnetPacketResponse(ret_code=RC_MD_NOFREESPACE, ret_message=str(err))
        except MDNotInit, err:
            if not key:
                logger.info('User metadata %s does not initialized! Trying to restore from repicas...'%user_id_hash)
                for _ in self.try_restore_from_replicas(user_id_hash):
                    try:
                        return self.try_update(user_id_hash, key, add_list, rm_list, packet, reinit_md=True)
                    except MDNotInit:
                        pass

            return FabnetPacketResponse(ret_code=RC_MD_NOTINIT, ret_message=str(err))
        except Exception, err:
            import traceback
            logger.write = logger.debug
            traceback.print_exc(file=logger)
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message=str(err))

    def try_update(self, user_id_hash, key, add_list, rm_list, packet, reinit_md=False):
        if not key:
            keys = KeyUtils.get_all_keys(user_id_hash, MIN_REPLICA_COUNT)
            h_range = self.operator.find_range(user_id_hash)
            _, _, node_address = h_range
            if not h_range:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No hash range found for key %s!'%user_id_hash)
            if self.self_address != node_address:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Not my range!')
            
            db_path = self.operator.get_db_path(user_id_hash, FSMappedDHTRange.DBCT_MD_MASTER)
        else:
            KeyUtils.validate(key)
            db_path = self.operator.get_db_path(key, FSMappedDHTRange.DBCT_MD_REPLICA)

        if reinit_md:
            self.operator.reinit_metadata(db_path)
    
        for rm_f_path in rm_list:
            self.operator.user_metadata_call(db_path, 'remove_path', rm_f_path)

        for f_path, dbs in add_list:
            dbs_ol = []
            for db in dbs:
                dbs_ol.append(MDDataBlockInfo(db[0], db[1], db[2]))
            self.operator.user_metadata_call(db_path, 'update_path', f_path, dbs_ol)

        if not key:
            for key in keys[1:]:
                h_range = self.operator.find_range(key)
                if not h_range:
                    return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No hash range found for key %s!'%user_id_hash)
                _, _, node_address = h_range
                params = copy.copy(packet.parameters)
                params['key'] = key
                self._init_operation(node_address, 'UpdateMetadata', params, sync=False)

        return FabnetPacketResponse()

    def try_restore_from_replicas(self, user_id_hash):
        keys = KeyUtils.get_all_keys(user_id_hash, MIN_REPLICA_COUNT)
        for key in keys[1:]:
            h_range = self.operator.find_range(key)
            if not h_range:
                continue
            _, _, node_address = h_range
            params = {'key': key, 'user_id_hash': user_id_hash, 'dbct': FSMappedDHTRange.DBCT_MD_REPLICA}
            ret = self._init_operation(node_address, 'RestoreMetadata', params, sync=True)
            if ret.ret_code != RC_OK:
                logger.warning('User metadata %s not restored from %s: %s'%(user_id_hash, node_address, ret.ret_message))
                continue
            logger.info('User metadata %s restored from %s ...'%(user_id_hash, node_address))
            yield node_address
