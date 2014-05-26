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
import threading
import copy
import json
import errno

from fabnet.utils.logger import oper_logger as logger
from fabnet_dht.constants import MIN_KEY, MAX_KEY
from fabnet_dht.data_block import DataBlock, ThreadSafeDataBlock
from fabnet_dht.exceptions import *


class FSMappedDHTRange:
    #data blocks content type
    DBCT_MASTER = 'mdb'
    DBCT_REPLICA = 'rdb'
    DBCT_MD_MASTER = 'mmd'
    DBCT_MD_REPLICA = 'rmd'
    DBCT_TEMP = 'tmp'

    __DBCT_LIST_RANGES = [DBCT_MASTER, DBCT_REPLICA, DBCT_MD_MASTER, DBCT_MD_REPLICA]
    __DBCT_LIST = __DBCT_LIST_RANGES + [DBCT_TEMP]
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
        start, end = self.__get_saved_range('old_range_start', 'old_range_end')
        return FSMappedDHTRange(start, end, self.__range_path)

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

    def iterator(self, db_content_type=None, foreign_only=False, all_data=False):
        '''iterator over data blocks
        - db_content_type should be one of DBCT_* constants or list of DBCT_* constants
          if db_content_type is None - iterate over all content types
        - if foreign_only is False then data blocks in range [range_start, range_end]
          will be iterated. Else - only data blocks NOT in range [range_start, range_end]
        - if all_data is True - iterate all data blocks with requested content type
          foreign_only parameter will be ignored in this case

        yield (<data block key>, <data block full path>)
        '''
        try:
            if db_content_type is None:
                db_ct_list = []
                for ct, path in self.__dirs_map.items():
                    if ct in self.__DBCT_LIST_RANGES:
                        db_ct_list.append((ct, path))
            elif type(db_content_type) in (list, tuple):
                db_ct_list = []
                for ct in db_content_type:
                    if ct not in self.__DBCT_LIST_RANGES:
                        raise FSHashRangesException('Unsupported data block content type "%s"'%ct)
                    db_ct_list.append((ct, self.__dirs_map[ct]))
            else:
                f_path = self.__dirs_map.get(db_content_type, None)
                if f_path is None:
                    raise FSHashRangesException('Unknown data block content type "%s"'%db_content_type)
                db_ct_list = [(db_content_type, f_path)]

            for dbct, f_path in db_ct_list:
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

                    yield db_key, dbct, f_path + db_key
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
        free = stat.f_bfree * stat.f_frsize
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
        size = 0
        for key, _, path in self.iterator(db_content_type, all_data=(not only_in_range)):
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

