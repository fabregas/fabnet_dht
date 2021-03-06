#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.mgmt.update_hash_range_table

@author Konstantin Andrusenko
@date September 26, 2012
"""

from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse
from fabnet.core.constants import RC_OK, RC_ERROR, NODE_ROLE
from fabnet.utils.logger import oper_logger as logger

from fabnet_dht.hash_ranges_table import HashRange
from fabnet_dht.constants import DS_NORMALWORK, DS_INITIALIZE, DS_DESTROYING 

class UpdateHashRangeTableOperation(OperationBase):
    ROLES = [NODE_ROLE]
    NAME = 'UpdateHashRangeTable'

    def before_resend(self, packet):
        """In this method should be implemented packet transformation
        for resend it to neighbours

        @params packet - object of FabnetPacketRequest class
        @return object of FabnetPacketRequest class
                or None for disabling packet resend to neigbours
        """
        return packet


    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        if self.operator.get_status() == DS_DESTROYING:
            return

        append_lst = packet.parameters.get('append', [])
        rm_lst = packet.parameters.get('remove', [])

        rm_obj_list = [HashRange(r[0], r[1], r[2]) for r in rm_lst]
        ap_obj_list = [HashRange(a[0], a[1], a[2]) for a in append_lst]
        self._lock()
        try:
            self.operator.apply_ranges_table_changes(rm_obj_list, ap_obj_list)

            logger.debug('RM RANGE: %s'%', '.join([r.to_str() for r in rm_obj_list]))
            logger.debug('APP RANGE: %s'%', '.join([a.to_str() for a in ap_obj_list]))
        except Exception, err:
            logger.debug('UpdateHashRangeTable error: %s STATUS=%s'%(err, self.operator.get_status()))
        finally:
            self._unlock()



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
