#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.delete_data_block

@author Konstantin Andrusenko
@date June 17, 2013
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED
from fabnet.core.constants import NODE_ROLE

from fabnet_dht.constants import RC_NO_DATA
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange, FSHashRangesNoData, FSHashRangesPermissionDenied
from fabnet_dht.data_block import DataBlock, ThreadSafeDataBlock

class DeleteDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'DeleteDataBlock'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.str_get('key')
        dbct = packet.str_get('dbct', FSMappedDHTRange.DBCT_MASTER)
        user_id_hash = packet.str_get('user_id_hash', '') 
        carefully_delete = packet.bool_get('carefully_delete', True)

        try:
            db_path = self.operator.get_db_path(key, dbct)
            with DataBlock(db_path) as db:
                if not db.exists():
                    raise FSHashRangesNoData('No data found!')

                if carefully_delete:
                    db.get_header().match(user_id_hash=user_id_hash)

                db.remove() #??? may be move to trash?
        except FSHashRangesNoData, err:
            return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message=str(err))
        except FSHashRangesPermissionDenied, err:
            return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, \
                    ret_message='permission denied: %s'%err)

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
