#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.check_data_block

@author Konstantin Andrusenko
@date November 10, 2012
"""
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.core.constants import NODE_ROLE
from fabnet.utils.logger import oper_logger as logger

from fabnet_dht.constants import RC_NO_DATA, RC_INVALID_DATA
from fabnet_dht.data_block import DataBlockHeader
from fabnet_dht.fs_mapped_ranges import FileBasedChunks

class CheckDataBlockOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'CheckDataBlock'

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        key = packet.parameters.get('key', None)
        checksum = packet.parameters.get('checksum', None)
        is_replica = packet.parameters.get('is_replica', False)
        if key is None:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Key is not found in request packet!')

        try:
            path = self.operator.get_data_block_path(key, is_replica)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message=str(err))

        data = FileBasedChunks(path)
        try:
            DataBlockHeader.check_raw_data(data, checksum)
        except Exception, err:
            return FabnetPacketResponse(ret_code=RC_INVALID_DATA, ret_message=err)
        finally:
            data.close()

        return FabnetPacketResponse()




