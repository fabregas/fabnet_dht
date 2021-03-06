#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.operations.client_put

@author Konstantin Andrusenko
@date October 3, 2012
"""
import os
import hashlib
import shutil
import uuid
from fabnet.core.operation_base import  OperationBase
from fabnet.core.fri_base import FabnetPacketResponse, BinaryDataPointer, FabnetPacketRequest
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import NODE_ROLE, CLIENT_ROLE

from fabnet_dht.constants import MIN_REPLICA_COUNT, RC_ALREADY_EXISTS
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlockHeader, DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange

class ClientPutOperation(OperationBase):
    ROLES = [NODE_ROLE, CLIENT_ROLE]
    NAME = 'ClientPutData'

    def init_locals(self):
        self.node_name = self.operator.get_node_name()

    def process(self, packet):
        """In this method should be implemented logic of processing
        reuqest packet from sender node

        @param packet - object of FabnetPacketRequest class
        @return object of FabnetPacketResponse
                or None for disabling packet response to sender
        """
        if not packet.binary_data:
            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='No binary data found!')
        init_block = packet.bool_get('init_block', True) #if init_block and db already exists -> error

        key = packet.parameters.get('key', None)
        if key is not None:
            KeyUtils.validate(key)

        replica_count = packet.int_get('replica_count', MIN_REPLICA_COUNT)
        wait_writes_count = packet.int_get('wait_writes_count', 1)

        if wait_writes_count > (replica_count+1):
            return FabnetPacketResponse(ret_code=RC_ERROR, \
                    ret_message='Cant waiting more replicas than saving!')
        if replica_count < MIN_REPLICA_COUNT:
            return FabnetPacketResponse(ret_code=RC_ERROR, \
                    ret_message='Minimum replica count is equal to %s!'%MIN_REPLICA_COUNT)

        succ_count = 0
        errors = []
        local_save = []
        tmp_db = None
        master_db = None
        keys = KeyUtils.generate_new_keys(self.node_name, replica_count, prime_key=key)
        try:
            user_id_hash = hashlib.sha1(str(packet.user_id)).hexdigest()

            tmp_db_path = self.operator.get_db_path(keys[0]+str(uuid.uuid4()), FSMappedDHTRange.DBCT_TEMP)
            tmp_db = DataBlock(tmp_db_path)

            master_db_path = self.operator.get_db_path(keys[0], FSMappedDHTRange.DBCT_MASTER)
            master_db = DataBlock(master_db_path)
            if master_db.exists():
                if init_block:
                    return FabnetPacketResponse(ret_code=RC_ALREADY_EXISTS, ret_message='[1] Already exists!')
                master_db.get_header().match(user_id_hash=user_id_hash)
            master_db.block()

            tmp_db.write(DataBlockHeader.EMPTY_HEADER)
            checksum = tmp_db.write(packet.binary_data, iterate=True)
            db_header = DataBlockHeader(keys[0], replica_count, checksum, user_id_hash)
            tmp_db.write(db_header.pack(), seek=0)
            size = os.path.getsize(tmp_db_path) - DataBlockHeader.HEADER_LEN

            for i, key in enumerate(keys):
                cur_dbct = FSMappedDHTRange.DBCT_MASTER if i == 0 else FSMappedDHTRange.DBCT_REPLICA
                h_range = self.operator.find_range(key)
                if not h_range:
                    errors.append('No hash range found for key=%s!'%key)
                    continue

                _, _, node_address = h_range
                params = {'key': key, 'dbct': cur_dbct, 'user_id_hash': user_id_hash, \
                                        'init_block': init_block, 'carefully_save': True}

                if self.self_address == node_address:
                    local_save.append((key, cur_dbct))
                    succ_count += 1
                elif succ_count >= wait_writes_count:
                    binary_data_pointer = BinaryDataPointer(tmp_db.hardlink(), remove_on_close=True)
                    self._init_operation(node_address, 'PutDataBlock', params, binary_data=binary_data_pointer)
                else:
                    resp = self._init_operation(node_address, 'PutDataBlock', params, \
                                                sync=True, binary_data=tmp_db.chunks())

                    if resp.ret_code == RC_ALREADY_EXISTS:
                        return FabnetPacketResponse(ret_code=RC_ALREADY_EXISTS, ret_message='[2] Already exists!')
                    elif resp.ret_code != RC_OK:
                        errors.append('From %s: %s'%(node_address, resp.ret_message))
                    else:
                        succ_count += 1

            #local save
            for i, (key, dbct) in enumerate(local_save):
                try:
                    db_path = self.operator.get_db_path(key, dbct)
                    with DataBlock(db_path) as db:
                        if key != keys[0] and db.exists():
                            if init_block:
                                raise Exception('Key %s with CT=%s is already exists'%(key, dbct))
                            db.get_header().match(user_id_hash=user_id_hash)

                    if i == 0:
                        os.rename(tmp_db_path, db_path)
                    else:
                        shutil.copyfile(tmp_db_path, db_path)

                    tmp_db_path = db_path
                except Exception, err:
                    succ_count -= 1
                    msg = 'Saving data block to local range error: %s'%err
                    errors.append(msg)
        
            if wait_writes_count > succ_count:
                raise Exception('\n'.join(errors))

            return FabnetPacketResponse(ret_parameters={'key': keys[0], 'checksum': checksum, 'size': size})
        except Exception, err:
            if init_block:
                try:
                    delete_data = self.get_operation_object('ClientDeleteData')
                    d_packet = FabnetPacketRequest(method='ClientDeleteData', \
                            parameters={'key': keys[0], 'replica_count': replica_count, \
                                        'user_id_hash': user_id_hash})
                    d_packet.role = NODE_ROLE
                    ret = delete_data.process(d_packet)

                    if ret.ret_code != RC_OK:
                        raise Exception(ret.ret_message)
                except Exception, e:
                    err = str(err)
                    err += '\nDelete saved DBs error: %s'%e
            else:

                #FIXME: restore data blocks
                pass

            return FabnetPacketResponse(ret_code=RC_ERROR, ret_message='Write error [key=%s]: %s'%(keys[0], err))
        finally:
            if tmp_db:
                tmp_db.close()
                tmp_db.remove()
            if master_db:
                master_db.close()



