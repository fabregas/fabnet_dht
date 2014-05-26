#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.get_data_block

@author Konstantin Andrusenko
@date October 3, 2012
"""
import hashlib
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.constants import RC_NO_DATA
from fabnet.core.fri_base import FileBasedChunks
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange, FSHashRangesNoData
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSHashRangesPermissionDenied, FSHashRangesNoData

class GetDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME = 'GetDataBlock'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.str_get('key')
        dbct = packet.str_get('dbct', FSMappedDHTRange.DBCT_MASTER)

        if packet.role == CLIENT_ROLE:
            user_id_hash = hashlib.sha1(str(packet.user_id)).hexdigest()
        else:
            user_id_hash = packet.str_get('user_id_hash', '')

        db = None
        try:
            db_path = self.operator.get_db_path(key, dbct)
            db = DataBlock(db_path)
            if not db.exists():
                raise FSHashRangesNoData('No data found!')

            raw = db.get_next_chunk(DataBlockHeader.HEADER_LEN)
            if user_id_hash:
                header = DataBlockHeader.unpack(raw)
                header.match(user_id_hash=user_id_hash)

            return FabnetPacketResponse(binary_data=db, ret_parameters={'checksum': header.checksum})
        except Exception, err:
            if db:
                db.close()
            if err.__class__ == FSHashRangesPermissionDenied:
                return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, ret_message=str(err))
            elif err.__class__ == FSHashRangesNoData:
                return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message='No data found')
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Unexpected error: %s'%err)


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
