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
import tempfile

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse, BinaryDataPointer
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE

from fabnet_dht.constants import MIN_REPLICA_COUNT, RC_MD_NOFREESPACE, RC_MD_NOTINIT
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange


class RestoreMetadataOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'RestoreMetadata'

    def process(self, packet):
        """
        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        user_id_hash = packet.str_get('user_id_hash')
        key = packet.str_get('key')
        dbct = packet.str_get('dbct', FSMappedDHTRange.DBCT_MD_REPLICA)

        db_path = self.operator.get_db_path(key, dbct)
        self.operator.reinit_metadata(db_path)
        user_info = self.operator.user_metadata_call(db_path, 'get_user_info')
        if not user_info.storage_size:
            return FabnetPacketResponse(ret_code=RC_MD_NOTINIT, ret_message='MD is not initialized')

        tmp = tempfile.NamedTemporaryFile(suffix='.zip')
        try:
            os.system('rm -f %s && cd %s && zip -r %s *'%(tmp.name, db_path, tmp.name))
            path = tmp.name
            params = {'key': user_id_hash, 'dbct': FSMappedDHTRange.DBCT_MD_MASTER, 'init_block': False}
            h_range = self.operator.find_range(user_id_hash)
            if not h_range:
                return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No hash range found for key %s!'%key)    
            _, _, node_address = h_range
            tmp_db = ThreadSafeDataBlock(path)
            resp = self._init_operation(node_address, 'PutDataBlock', params, sync=True, binary_data=tmp_db.chunks())
            return resp
        finally:
            tmp.close()

