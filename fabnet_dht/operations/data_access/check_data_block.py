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
from fabnet.core.fri_base import FileBasedChunks
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange, FSHashRangesNoData
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock

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
        key = packet.str_get('key')
        checksum = packet.str_get('checksum', '')
        dbct = packet.str_get('dbct', FSMappedDHTRange.DBCT_MASTER)

        db_path = self.operator.get_db_path(key, dbct)
        with DataBlock(db_path) as db:
            if not db.exists():
                return FabnetPacketResponse(ret_code=RC_NO_DATA, ret_message='No data found!')

            try:
                DataBlockHeader.check_raw_data(db, checksum)
            except Exception, err:
                return FabnetPacketResponse(ret_code=RC_INVALID_DATA, ret_message='check data block error: %s'%err)

        return FabnetPacketResponse()




