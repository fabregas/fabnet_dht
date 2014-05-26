#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.data_block

@author Konstantin Andrusenko
@date October 06, 2012
"""
import os
import fcntl
import time
import struct
import hashlib
import threading
from datetime import datetime

from fabnet.core.constants import DEFAULT_CHUNK_SIZE
from fabnet.core.fri_base import FileBasedChunks
from fabnet_dht.exceptions import *

class DataBlockHeader:
    DATA_BLOCK_LABEL = 'FDB01'
    STRUCT_FMT = '<5sd20sb20s20s'
    HEADER_LEN = struct.calcsize(STRUCT_FMT)
    EMPTY_HEADER = '\x00'*HEADER_LEN

    def __init__(self, master_key, replica_count, checksum, user_id_hash, stored_dt=None):
        self.master_key = master_key
        self.replica_count = replica_count
        self.checksum = checksum
        self.user_id_hash = user_id_hash
        self.stored_dt = stored_dt

    def match(self, master_key=None, replica_count=None, checksum=None, \
                                                user_id_hash=None, stored_dt=None):
        if master_key and self.master_key != master_key:
            raise FSHashRangesInvalidDataBlock('Master DB key %s != %s'%(master_key, self.master_key))

        if replica_count and self.replica_count != replica_count:
            raise FSHashRangesInvalidDataBlock('Replica count %s != %s'%(replica_count, self.replica_count))
        
        if checksum and self.checksum != checksum:
            raise FSHashRangesInvalidDataBlock('Checksum %s != %s'%(checksum, self.checksum))

        if user_id_hash:
            if user_id_hash != self.user_id_hash:
                raise FSHashRangesPermissionDenied('Alien data block')

        if stored_dt and stored_dt < self.stored_dt:
            raise FSHashRangesOldDataDetected('Data block is already saved with newer datetime')


    def pack(self):
        unixtime = time.mktime(datetime.utcnow().timetuple())
        try:
            header = struct.pack(self.STRUCT_FMT, self.DATA_BLOCK_LABEL, unixtime, \
                    self.master_key.decode('hex'), self.replica_count, \
                    self.checksum.decode('hex'), self.user_id_hash.decode('hex'))
        except Exception, err:
            raise Exception('Data block header packing failed! Details: %s'%err)

        return header

    @classmethod
    def unpack(cls, data):
        header = data[:cls.HEADER_LEN]
        try:
            db_label, put_unixtime, primary_key, replica_count, \
                    checksum, user_id_hash = struct.unpack(cls.STRUCT_FMT, header)
        except Exception, err:
            raise FSHashRangesInvalidDataBlock('Data block header is invalid! Details: %s'%err)

        if db_label != cls.DATA_BLOCK_LABEL:
            raise FSHashRangesInvalidDataBlock('Corrupted data block! No block label found')


        return DataBlockHeader(primary_key.encode('hex'), replica_count, \
                        checksum.encode('hex'), user_id_hash.encode('hex'), put_unixtime)


    @classmethod
    def check_raw_data(cls, binary_data, exp_checksum=None):
        header = binary_data.get_next_chunk(cls.HEADER_LEN)

        data_block = cls.unpack(header)

        if exp_checksum and exp_checksum != data_block.checksum:
            raise FSHashRangesInvalidDataBlock('Data checksum is not equal to expected')

        h_func = hashlib.sha1('')
        while True:
            chunk = binary_data.get_next_chunk()
            if chunk is None:
                break
            h_func.update(chunk)

        if data_block.checksum != h_func.hexdigest():
            raise FSHashRangesInvalidDataBlock('Data block has bad checksum')






class DataBlock:
    __TD_LOCKS = {}
    __TD_LOCK = threading.Lock()
    NEED_THRD_LOCK = False

    @classmethod
    def __try_thread_lock(cls, path, shared=False):
        cls.__TD_LOCK.acquire()
        try:
            lock = cls.__TD_LOCKS.get(path, None)
            cur_thrd_id = threading.current_thread().ident
            if lock is None:
                cls.__TD_LOCKS[path] = (cur_thrd_id, shared, 1)
                return True
            thrd_id, lock, cnt = lock
            if shared:
                if thrd_id == cur_thrd_id:
                    return True
                if lock == shared:
                    cls.__TD_LOCKS[path] = (cur_thrd_id, shared, cnt+1)
                    return True
            else:
                if thrd_id == cur_thrd_id:
                    cls.__TD_LOCKS[path] = (thrd_id, shared, 1)
                    return True
            return False
        finally:
            cls.__TD_LOCK.release()
            
    @classmethod
    def __thread_lock(cls, path, shared=False):
        while not cls.__try_thread_lock(path, shared):
            time.sleep(0.01)

    @classmethod
    def __thread_unlock(cls, path):
        cls.__TD_LOCK.acquire()
        try:
            lock = cls.__TD_LOCKS.get(path, None)
            if lock is None:
                return
            cur_thrd_id = threading.current_thread().ident
            thrd_id, lock, cnt = lock
            if lock == True: #shared
                cnt -= 1
                if cnt == 0:
                    del cls.__TD_LOCKS[path]
                else:
                    cls.__TD_LOCKS[path] = (thrd_id, lock, cnt)
            else:
                if cur_thrd_id != thrd_id:
                    return
                del cls.__TD_LOCKS[path]
        finally:
            cls.__TD_LOCK.release()

    def __init__(self, path):
        self.__path = path
        self.__fd = None
        self.__blocked = False
        self.__link_idx = 0
        self.__cur_seek = 0

    def __open(self):
        self.__fd = os.open(self.__path, os.O_RDWR | os.O_CREAT)
        fcntl.lockf(self.__fd, fcntl.LOCK_SH)
        if self.NEED_THRD_LOCK:
            self.__thread_lock(self.__path, shared=True)

    def exists(self):
        '''return True if data block file is already exists'''
        return os.path.exists(self.__path)

    def read(self, bytes_cnt=0, seek=0, iterate=False):
        '''read bytes_cnt bytes of data from data block file
        if bytes_cnt <= 0 - all data will be read
        if seek > 0 - start read from seek position, else - from start of file
        if iterate is True then return iterator over file that
        will continuosly get portions of data at most bytes_cnt size until EOF
        '''
        if not self.__fd:
            self.__open()

        if seek:
            os.lseek(self.__fd, seek, os.SEEK_SET)
        else:
            os.lseek(self.__fd, 0, os.SEEK_SET) #move to start

        def iterator_func(bytes_cnt):
            if bytes_cnt <= 0:
                bytes_cnt = 1024

            while True:
                data = os.read(self.__fd, bytes_cnt)
                if not data:
                    return
                yield data

        if iterate:
            return iterator_func(bytes_cnt)
        else:
            if bytes_cnt > 0:
                return os.read(self.__fd, bytes_cnt)
            else:
                ret_data = ''
                for data in iterator_func(bytes_cnt):
                    ret_data += data
                return ret_data

    def write(self, buf, seek=-1, iterate=False, truncate=False):
        '''write buf string to data block file
        If seek >= 0 write process will be started from seek position,
        else - write to end of data block
        If iterate == True, buf will be used as iterator that continously get data
        '''
        wr_checksum = hashlib.sha1('')

        blocked = self.block()
        try:
            if truncate:
                os.ftruncate(self.__fd, 0)
            if seek >= 0:
                os.lseek(self.__fd, seek, os.SEEK_SET)
            else:
                os.lseek(self.__fd, 0, os.SEEK_END) #move to end

            if iterate:
                for data in buf:
                    os.write(self.__fd, data)
                    wr_checksum.update(data)
            else:
                os.write(self.__fd, buf)
                wr_checksum.update(buf)

            os.fsync(self.__fd)
        finally:
            if blocked:
                self.unblock()
        return wr_checksum.hexdigest()

    def get_header(self):
        raw = self.read(DataBlockHeader.HEADER_LEN)
        return DataBlockHeader.unpack(raw)

    def __enter__(self):
        return self

    def __exit__(self, type, value, trace):
        self.close()

    def block(self):
        '''open data block file (if not opened), lock file with exclusive lock
        and keep file opened'''
        if not self.__fd:
            self.__open()

        if self.__blocked:
            return False
        if self.NEED_THRD_LOCK:
            self.__thread_lock(self.__path, shared=False)
        fcntl.lockf(self.__fd, fcntl.LOCK_EX)
        return True

    def unblock(self):
        '''unlock and file descriptor'''
        if self.NEED_THRD_LOCK:
            self.__thread_unlock(self.__path)

        if self.__blocked:
            fcntl.lockf(self.__fd, fcntl.LOCK_UN)
            self.__blocked = False

    def chunks(self):
        return FileBasedChunks(self.__path)

    def hardlink(self):
        link = '%s.%s' % (self.__path, self.__link_idx)
        os.link(self.__path, link)
        self.__link_idx += 1
        return link

    def remove(self):
        if os.path.exists(self.__path):
            os.unlink(self.__path)

    def __del__(self):
        self.close()

    def close(self):
        '''close file descriptor is opened'''
        if self.__fd:
            os.close(self.__fd)
            self.__fd = None

    def get_next_chunk(self, l=None):
        if l is None:
            l = DEFAULT_CHUNK_SIZE
        data = self.read(l, self.__cur_seek)
        if not data:
            return None
        self.__cur_seek += len(data)
        return data

    def chunks_count(self):
        f_size = os.path.getsize(self.__path) - self.__cur_seek
        cnt = f_size / DEFAULT_CHUNK_SIZE
        if f_size % DEFAULT_CHUNK_SIZE != 0:
            cnt += 1
        return cnt

    def data(self):
        data = self.read(seek=self.__cur_seek)
        self.close()
        return data

class ThreadSafeDataBlock(DataBlock):
    NEED_THRD_LOCK = True

