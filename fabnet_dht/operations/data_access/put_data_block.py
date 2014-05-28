#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.put_data_block

@author Konstantin Andrusenko
@date September 26, 2012
"""
import os
import hashlib
import tempfile

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED, \
                                    NODE_ROLE, CLIENT_ROLE
from fabnet_dht.constants import RC_OLD_DATA, RC_NO_FREE_SPACE, RC_ALREADY_EXISTS
from fabnet_dht.data_block import DataBlockHeader
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange, FSHashRangesOldDataDetected, \
                    FSHashRangesNoFreeSpace, FSHashRangesPermissionDenied

class PutDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = "PutDataBlock"

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.parameters.get('key', None)
        dbct = packet.str_get('dbct')

        init_block = packet.bool_get('init_block', False) #if init_block and db already exists -> error
        carefully_save = packet.bool_get('carefully_save', False)
        user_id_hash = packet.str_get('user_id_hash', '')
        stored_unixtime = packet.parameters.get('stored_unixtime', None)

        if not packet.binary_data:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Binary data does not found!')

        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='key does not found!')

        key = KeyUtils.to_hex(key)
        data = packet.binary_data
        tmp = None
        try:
            db_path = self.operator.get_db_path(key, dbct)

            #with ThreadSafeDataBlock(db_path) as db:
            with DataBlock(db_path) as db:
                if db.exists():
                    if init_block:
                        return FabnetPacketResponse(ret_code=RC_ALREADY_EXISTS, ret_message='Already exists!')

                    if dbct in (FSMappedDHTRange.DBCT_MASTER, FSMappedDHTRange.DBCT_REPLICA) and carefully_save:
                        db.block()
                        db.get_header().match(user_id_hash=user_id_hash, stored_dt=stored_unixtime)

                if dbct in (FSMappedDHTRange.DBCT_MD_MASTER, FSMappedDHTRange.DBCT_MD_REPLICA):
                    tmp = tempfile.NamedTemporaryFile(suffix='.zip')
                    with DataBlock(tmp.name) as tmp_db:
                        tmp_db.write(data, iterate=True)
                    os.system('rm -rf %s && mkdir -p %s && cd %s && unzip %s'%(db_path, db_path, db_path, tmp.name))
                else:
                    db.write(data, iterate=True)
        except FSHashRangesOldDataDetected, err:
            return FabnetPacketResponse(ret_code=RC_OLD_DATA, ret_message=str(err))
        except FSHashRangesNoFreeSpace, err:
            return FabnetPacketResponse(ret_code=RC_NO_FREE_SPACE, ret_message=str(err))
        except FSHashRangesPermissionDenied, err:
            return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, ret_message=str(err))
        finally:
            if tmp: tmp.close()

        return FabnetPacketResponse()


    def callback(self, packet, sender=None):
        """In this method should be implemented logic of processing
        response packet from requested node

        @param packet - object of FabnetPacketResponse class
        @param sender - address of sender node.
        If sender == None then current node is operation initiator
        @return object of FabnetPacketResponse
                that should be resended to current node requestor
                or None for disabling packet resending
        """
        pass
