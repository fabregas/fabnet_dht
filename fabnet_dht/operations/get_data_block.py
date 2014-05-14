#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.get_data_block

@author Konstantin Andrusenko
@date October 3, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, RC_PERMISSION_DENIED
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.constants import RC_NO_DATA
from fabnet_dht.data_block import DataBlockHeader
from fabnet_dht.fs_mapped_ranges import FileBasedChunks, FSHashRangesNoData

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
        key = packet.parameters.get('key', None)
        is_replica = packet.parameters.get('is_replica', False)
        r_user_id = packet.parameters.get('user_id', packet.session_id)
        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Key is not found in request packet!')

        try:
            path = self.operator.get_data_block_path(key, is_replica)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message=str(err))

        data = FileBasedChunks(path)
        header = data.read(DataBlockHeader.HEADER_LEN)
        _, _, checksum, user_id, _ = DataBlockHeader.unpack(header)
        if user_id and packet.role != NODE_ROLE:
            if r_user_id != user_id:
                return FabnetPacketResponse(ret_code=RC_PERMISSION_DENIED, ret_message='permission denied')

        return FabnetPacketResponse(binary_data=data, ret_parameters={'checksum': checksum})


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
