#!/usr/bin/python
"""
Copyright (C) 2013 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.repair_process

@author Konstantin Andrusenko
@date January 09, 2013
"""
import os
import hashlib
import tempfile

from fabnet.utils.logger import oper_logger as logger
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse

from fabnet_dht.constants import RC_NO_DATA, RC_INVALID_DATA, RC_OLD_DATA, MIN_REPLICA_COUNT
from fabnet_dht.key_utils import KeyUtils
from fabnet_dht.data_block import DataBlock, ThreadSafeDataBlock
from fabnet_dht.fs_mapped_ranges import FSMappedDHTRange

class RepairProcess:
    def __init__(self, operator):
        self.operator = operator
        self.__invalid_local_blocks = 0
        self.__repaired_foreign_blocks = 0
        self.__failed_repair_foreign_blocks = 0
        self.__processed_local_blocks = 0
        self.__local_moved = []

    def __init_stat(self, params):
        self.__processes_data_blocks = 0
        self.__invalid_local_blocks = 0
        self.__repaired_foreign_blocks = 0
        self.__failed_repair_foreign_blocks = 0
        self.__processed_local_blocks = 0
        self.__local_moved = []

        self.__check_range_start = params.get('check_range_start', None)
        self.__check_range_end = params.get('check_range_end', None)
        if self.__check_range_start:
            self.__check_range_start = long(self.__check_range_start, 16)
        if self.__check_range_end:
            self.__check_range_end = long(self.__check_range_end, 16)

    def __get_stat(self):
        return 'processed_local_blocks=%s, invalid_local_blocks=%s, ' \
                'repaired_foreign_blocks=%s, failed_repair_foreign_blocks=%s'% \
                (self.__processed_local_blocks, self.__invalid_local_blocks,
                self.__repaired_foreign_blocks, self.__failed_repair_foreign_blocks)

    def _in_check_range(self, key):
        if not self.__check_range_start and not self.__check_range_end:
            return True

        if self.__check_range_start <= long(key, 16) <= self.__check_range_end:
            return True

        return False

    def repair_process(self, params):
        self.__init_stat(params)
        dht_range = self.operator.get_dht_range()

        logger.info('[RepairDataBlocks] Processing DHT range...')
        for key, dbct, path in dht_range.iterator([FSMappedDHTRange.DBCT_MASTER, FSMappedDHTRange.DBCT_REPLICA]):
            if (key, dbct) in self.__local_moved:
                continue
            self.__process_data_block(key, path, dbct)
        logger.info('[RepairDataBlocks] DHT range is processed!')

        logger.info('[RepairDataBlocks] Processing users metadata range...')
        for key, dbct, path in dht_range.iterator(FSMappedDHTRange.DBCT_MD_MASTER):
            logger.info('PROCESS %s %s'%(dbct, path))
            if (key, dbct) in self.__local_moved:
                continue
            self.__process_md_block(key, path)
        logger.info('[RepairDataBlocks] Users metadata range is processed!')

        return self.__get_stat()

    def __process_data_block(self, key, path, dbct):
        self.__processed_local_blocks += 1
        with ThreadSafeDataBlock(path) as db:
            try:
                header = db.get_header()
                data_keys = KeyUtils.get_all_keys(header.master_key, header.replica_count)

                if dbct == FSMappedDHTRange.DBCT_MASTER and key != header.master_key:
                    raise Exception('Master key is invalid: %s != %s'%(key, header.master_key))
                elif dbct == FSMappedDHTRange.DBCT_REPLICA:
                    if key not in data_keys:
                        raise Exception('Replica key is invalid: %s'%key)
            except Exception, err:
                self.__invalid_local_blocks += 1
                logger.error('[RepairDataBlocks] %s'%err)
                return

            if dbct == FSMappedDHTRange.DBCT_REPLICA and self._in_check_range(data_keys[0]):
                self.__check_data_block(key, db, dbct,  data_keys[0], \
                        header, FSMappedDHTRange.DBCT_MASTER)

            for repl_key in data_keys[1:]:
                if repl_key == key:
                    continue

                if self._in_check_range(repl_key):
                    self.__check_data_block(key, db, dbct, repl_key, \
                            header, FSMappedDHTRange.DBCT_REPLICA)

    def __process_md_block(self, check_key, path):
        self.__processed_local_blocks += 1
        data_keys = KeyUtils.get_all_keys(check_key, MIN_REPLICA_COUNT)

        for repl_key in data_keys[1:]:
            if not self._in_check_range(repl_key):
                continue

            long_key = self.__validate_key(repl_key)
            range_obj = self.operator.ranges_table.find(long_key)
            checksum = self.operator.user_metadata_call(path, 'get_checksum')
            params = {'key': repl_key, 'checksum': checksum, 'dbct': FSMappedDHTRange.DBCT_MD_REPLICA}
            req = FabnetPacketRequest(method='CheckDataBlock', sender=self.operator.self_address, sync=True, parameters=params)
            resp = self.operator.call_node(range_obj.node_address, req)
            if resp.ret_code == RC_OK:
                return
            if resp.ret_code not in (RC_NO_DATA, RC_INVALID_DATA):
                self.__failed_repair_foreign_blocks += 1
                logger.error('CheckDataBlock failed at %s. Details: %s'%(range_obj.node_address, resp.ret_message))
                return

            logger.info('Invalid metadata for user=%s at %s ([%s]%s). Sending valid block...'%\
                (check_key, range_obj.node_address, resp.ret_code, resp.ret_message))

            tmp = tempfile.NamedTemporaryFile(suffix='.zip')
            os.system('rm -f %s && cd %s && zip -r %s *'%(tmp.name, path, tmp.name))
            params = {'key': repl_key, 'dbct': FSMappedDHTRange.DBCT_MD_REPLICA, 'user_id_hash': check_key}
            req = FabnetPacketRequest(method='PutDataBlock', sender=self.operator.self_address, sync=True, \
                                        parameters=params, binary_data=ThreadSafeDataBlock(tmp.name))
            resp = self.operator.call_node(range_obj.node_address, req)
            tmp.close()
            if resp.ret_code != RC_OK:
                self.__failed_repair_foreign_blocks += 1
                logger.error('PutDataBlock failed on %s. Details: %s'%(range_obj.node_address, resp.ret_message))
            else:
                self.__repaired_foreign_blocks += 1

    def __validate_key(self, key):
        try:
            if len(key) != 40:
                raise ValueError()
            return long(key, 16)
        except Exception:
            return None

    def __check_data_block(self, local_key, db, dbct, check_key, header, remote_dbct):
        long_key = self.__validate_key(check_key)
        if long_key is None:
            logger.error('[RepairDataBlocks] Invalid data key "%s"'%key)
            self.__invalid_local_blocks += 1

        range_obj = self.operator.ranges_table.find(long_key)
        params = {'key': check_key, 'checksum': header.checksum, 'dbct': remote_dbct}
        req = FabnetPacketRequest(method='CheckDataBlock', sender=self.operator.self_address, sync=True, parameters=params)
        resp = self.operator.call_node(range_obj.node_address, req)

        if resp.ret_code in (RC_NO_DATA, RC_INVALID_DATA):
            logger.info('Invalid DB with key=%s at %s ([%s]%s). Sending valid block...'%\
                    (check_key, range_obj.node_address, resp.ret_code, resp.ret_message))

            if self.operator.self_address == range_obj.node_address:
                self.__local_moved.append((check_key, remote_dbct))
                self.operator.copy_db(local_key, dbct, check_key, remote_dbct)
                resp = FabnetPacketResponse()
            else:
                params = {'key': check_key, 'dbct': remote_dbct, 'carefully_save': True, \
                        'user_id_hash': header.user_id_hash, 'stored_unixtime': header.stored_dt}
                req = FabnetPacketRequest(method='PutDataBlock', sender=self.operator.self_address, sync=True, \
                                            parameters=params, binary_data=db)
                resp = self.operator.call_node(range_obj.node_address, req)

            if resp.ret_code == RC_OLD_DATA:
                self.__invalid_local_blocks += 1
                logger.error('Old data block detected with key=%s'%check_key)
            elif resp.ret_code != RC_OK:
                self.__failed_repair_foreign_blocks += 1
                logger.error('PutDataBlock failed on %s. Details: %s'%(range_obj.node_address, resp.ret_message))
            else:
                self.__repaired_foreign_blocks += 1

        elif resp.ret_code != RC_OK:
            self.__failed_repair_foreign_blocks += 1
            logger.error('CheckDataBlock failed on %s. Details: %s'%(range_obj.node_address, resp.ret_message))

