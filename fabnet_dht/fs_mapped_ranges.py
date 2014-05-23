#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.fs_mapped_ranges

@author Konstantin Andrusenko
@date May 23, 2014
"""
import os
import fcntl
import shutil
import threading
import copy
import json
import time
import errno

from fabnet.utils.logger import oper_logger as logger
from fabnet_dht.constants import MIN_KEY, MAX_KEY

class FSHashRangesException(Exception):
    pass

class FSHashRangesNotFound(FSHashRangesException):
    pass

class FSHashRangesNoData(FSHashRangesException):
    pass

class FSHashRangesOldDataDetected(FSHashRangesException):
    pass

class FSHashRangesPermissionDenied(FSHashRangesException):
    pass

class FSHashRangesNoFreeSpace(FSHashRangesException):
    pass

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

    def write(self, buf, seek=0, iterate=False, truncate=False):
        '''write buf string to data block file
        If seek > 0 write process will be started from seek position,
        else - write to end of data block
        If iterate == True, buf will be used as iterator that continously get data
        '''
        self._block()
        try:
            if truncate:
                os.ftruncate(self.__fd, seek)
            if seek:
                os.lseek(self.__fd, seek, os.SEEK_SET)
            else:
                os.lseek(self.__fd, 0, os.SEEK_END) #move to end

            if iterate:
                for data in buf:
                    os.write(self.__fd, data)
            else:
                os.write(self.__fd, buf)

            os.fsync(self.__fd)
        finally:
            self._unblock()

    def __enter__(self):
        return self

    def __exit__(self, type, value, trace):
        self.close()

    def _block(self):
        '''open data block file (if not opened), lock file with exclusive lock
        and keep file opened'''
        if not self.__fd:
            self.__open()

        if self.__blocked:
            return
        if self.NEED_THRD_LOCK:
            self.__thread_lock(self.__path, shared=False)
        fcntl.lockf(self.__fd, fcntl.LOCK_EX)

    def _unblock(self):
        '''unlock and file descriptor'''
        if self.__blocked:
            if self.NEED_THRD_LOCK:
                self.__thread_unlock(self.__path)
            fcntl.lockf(self.__fd, fcntl.LOCK_UN)
            self.__blocked = False

    def close(self):
        '''close file descriptor is opened'''
        if self.NEED_THRD_LOCK:
            self.__thread_unlock(self.__path)
        if self.__fd:
            os.close(self.__fd)
            self.__fd = None


class ThreadSafedDataBlock(DataBlock):
    NEED_THRD_LOCK = True

class FSMappedDHTRange:
    #data blocks content type
    DBCT_MASTER = 'mdb'
    DBCT_REPLICA = 'rdb'
    DBCT_MD_MASTER = 'mmd'
    DBCT_MD_REPLICA = 'rmd'
    DBCT_TEMP = 'tmp'

    __DBCT_LIST = (DBCT_MASTER, DBCT_REPLICA, DBCT_MD_MASTER, DBCT_MD_REPLICA, DBCT_TEMP)
    __RANGE_INFO_FN = 'range_info'

    @classmethod 
    def discovery_range(cls, range_path):
        '''try to find range_info file and read previous saved range scope'''
        range_info_path = os.path.join(range_path, cls.__RANGE_INFO_FN)
        if not os.path.exists(range_info_path):
            return FSMappedDHTRange(MIN_KEY, MAX_KEY, range_path)

        try:
            raw_data = open(range_info_path).read()
            data = json.loads(raw_data)
            range_start = data.get('range_start', MIN_KEY)
            range_end = data.get('range_end', MAX_KEY)
        except Exception, err:
            logger.error('Invalid range_info file: %s'%err)

        return FSMappedDHTRange(range_start, range_end, range_path)

    def __init__(self, start, end, range_path):
        if not os.path.exists(range_path):
            raise FSHashRangesException('Path %s does not found!'%range_path) 

        self.__start = self._long_key(start)
        self.__end = self._long_key(end)
        self.__range_path = range_path
        self.__range_info = DataBlock(os.path.join(range_path, 'range_info'))

        self.__dirs_map = {}
        for dbct in self.__DBCT_LIST:
            dir_path = os.path.join(range_path, dbct)
            if not os.path.exists(dir_path):
                try:
                    os.mkdir(dir_path)
                except OSError, err:
                    raise FSHashRangesException('Unable to create directory %s: (%s)'%(dir_path, err))
            self.__dirs_map[dbct] = dir_path + '/'
            
        self.__mgmt_lock = threading.Lock()
        self.__child_ranges = []

        self.__no_free_space_flag = threading.Event()
        self.__free_for_unlock = 0


    def _long_key(self, key):
        if type(key) == int:
            key = long(key)
        elif type(key) != long:
            key = long(key, 16)

        return key

    def _str_key(self, key):
        if type(key) in (int, long):
            return '%040x'%key
        return key

    def _in_range(self, key):
        return self.__start <= key <= self.__end

    def save_range(self):
        '''write range scope to range_info file.
        current range scope (if found) should be saved too as old_range
        '''
        try:
            old_range_start, old_range_end = self.__get_saved_range('range_start', 'range_end')
            if old_range_start == self.__start and old_range_end == self.__end:
                return

            range_info = json.dumps({'range_start': self.__start,
                        'range_end': self.__end,
                        'old_range_start': old_range_start,
                        'old_range_end': old_range_end})

            self.__range_info.write(range_info, truncate=True)
        finally:
            self.__range_info.close()

    def get_last_range(self):
        return self.__get_saved_range('old_range_start', 'old_range_end')

    def __get_saved_range(self, start_f_name, end_f_name):
        raw_data = self.__range_info.read()
        self.__range_info.close()
        old_data = {}
        if raw_data:
            try:
                old_data = json.loads(raw_data)
            except Exception, err:
                logger.warning('__update_range_info: range_info file corrupted!')
        range_start = old_data.get(start_f_name, MIN_KEY)
        range_end = old_data.get(end_f_name, MAX_KEY)
        return long(range_start), long(range_end)


    def get_start(self):
        return self.__start

    def get_end(self):
        return self.__end

    def length(self):
        return 1 + self.__end - self.__start

    def is_max_range(self):
        return self.__start == MIN_KEY and self.__end == MAX_KEY

    def split_range(self, start_key, end_key):
        start_key = self._long_key(start_key)
        end_key = self._long_key(end_key)
        if start_key == self.__start:
            split_key = end_key
            ret_range_i = 0
            first_subrange_end = split_key
            second_subrange_start = split_key + 1
        elif end_key == self.__end:
            split_key = start_key
            ret_range_i = 1
            first_subrange_end = split_key - 1
            second_subrange_start = split_key
        else:
            raise FSHashRangesException('Bad subrange [%040x-%040x] for range [%040x-%040x]'%\
                                            (start_key, end_key, self.__start, self.__end))

        if not self._in_range(split_key):
            FSHashRangesNotFound('No key %040x found in range'%split_key)

        first_rg = FSMappedDHTRange(self.__start, first_subrange_end, self.__range_path)
        second_rg = FSMappedDHTRange(second_subrange_start, self.__end, self.__range_path)
        ranges = [first_rg, second_rg]

        self.__mgmt_lock.acquire()
        try:
            if self.__child_ranges:
                raise FSHashRangesException('Range is already splited!')
            self.__child_ranges = (ranges[ret_range_i], ranges[int(not ret_range_i)])
        finally:
            self.__mgmt_lock.release()

        return self.get_subranges()

    def join_subranges(self):
        self.__mgmt_lock.acquire()
        try:
            self.__child_ranges = []
        finally:
            self.__mgmt_lock.release()

    def get_subranges(self):
        self.__mgmt_lock.acquire()
        try:
            if self.__child_ranges:
                return copy.copy(self.__child_ranges)
            return None
        finally:
            self.__mgmt_lock.release()

    def extend(self, start_key, end_key):
        start_key = self._long_key(start_key)
        end_key = self._long_key(end_key)
        if start_key >= end_key:
            raise FSHashRangesException('Bad subrange [%040x-%040x] of [%040x-%040x]'%\
                                        (start_key, end_key, self.__start, self.__end))

        start = self.__start
        end = self.__end
        if self.__start == end_key+1:
            start = start_key
        elif self.__end == start_key-1:
            end = end_key
        else:
            raise FSHashRangesException('Bad range for extend [%040x-%040x] of [%040x-%040x]'%\
                                        (start_key, end_key, self.__start, self.__end))

        h_range = FSMappedDHTRange(start, end, self.__range_path)
        h_range.save_range()
        return h_range

    def iterator(self, db_content_type, foreign_only=False, all_data=False):
        '''iterator over data blocks
        - db_content_type should be one of DBCT_* constants
        - if foreign_only is False then data blocks in range [range_start, range_end]
          will be iterated. Else - only data blocks NOT in range [range_start, range_end]
        - if all_data is True - iterate all data blocks with requested content type
          foreign_only parameter will be ignored in this case

        yield (<data block key>, <data block full path>)
        '''
        try:
            f_path = self.__dirs_map.get(db_content_type, None)
            if f_path is None:
                raise FSHashRangesException('Unknown data block content type "%s"'%db_content_type)

            files = os.listdir(f_path) #FIXME: this is really sucks for huge count of files
            for db_key in files:
                if not all_data:
                    try:
                        in_range = self.__start <= long(db_key, 16) <= self.__end
                        if foreign_only and in_range:
                            continue
                        if (not foreign_only) and (not in_range):
                            continue
                    except ValueError:
                        logger.warning('invalid data block name "%s"'%db_key)
                        continue

                yield db_key, f_path + db_key
        except Exception, err:
            raise FSHashRangesException('Iterator over data blocks failed with error: %s'%err)

    def __get_file_size(self, file_path):
        try:
            stat = os.stat(file_path)
        except OSError:
            #no file found
            return 0
        rest = stat.st_size % stat.st_blksize
        if rest:
            rest = stat.st_blksize - rest
        return stat.st_size + rest

    def get_free_size(self):
        stat = os.statvfs(self.__range_path)
        free_space = stat.f_frsize * stat.f_bavail
        return free_space

    def get_free_size_percents(self):
        stat = os.statvfs(self.__range_path)
        return (stat.f_bavail * 100.) / stat.f_blocks

    def get_estimated_data_percents(self, add_size=0):
        stat = os.statvfs(self.__range_path)
        total = stat.f_blocks * stat.f_frsize 
        free = stat.f_bfree * stat.f_frsize
        used = total - free
        estimated_data_size_perc = (used + add_size) * 100. / total
        return estimated_data_size_perc

    def get_data_size(self, db_content_type=None, only_in_range=True):
        '''calculate data blocks size with content type db_content_type
        If db_content_type is None - return size of all stored data blocks
        '''
        if db_content_type is None:
            ct_list = self.__DBCT_LIST
        else:
            ct_list = [db_content_type]

        size = 0
        for dbct in ct_list:
            for key, path in self.iterator(dbct, all_data=(not only_in_range)):
                size += self.__get_file_size(path)
        return size

    def get_db_path(self, key, db_content_type, for_write=True):
        '''get absolute path to data block by key and content type'''
        if for_write and self.__no_free_space_flag.is_set():
            if self.get_free_size_percents() > self.__free_for_unlock:
                self.__no_free_space_flag.clear()
                logger.info('Range is unlocked for write...')
            else:
                raise FSHashRangesNoFreeSpace('No free space for saving data block')

        f_path = self.__dirs_map.get(db_content_type, None)
        if f_path is None:
            raise FSHashRangesException('Unknown data block content type "%s"'%db_content_type)

        return f_path + key

    def block_for_write(self, free_for_unlock):
        if self.__no_free_space_flag.is_set():
            return
        self.__free_for_unlock = free_for_unlock
        self.__no_free_space_flag.set()

    def remove_db(self, key, db_content_type):
        '''remove data block'''
        f_path = self.__dirs_map.get(db_content_type, None)
        if f_path is None:
            raise FSHashRangesException('Unknown data block content type "%s"'%db_content_type)

        db_path = f_path + key
        try:
            os.remove(db_path)
        except OSError, err:
            if err.errno == errno.ENOENT:
                #no such file
                return
            raise err 

