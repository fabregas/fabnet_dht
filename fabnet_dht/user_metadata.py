#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.user_metadata

@author Konstantin Andrusenko
@date May 26, 2014
"""
import os
import zlib
import copy
import struct
import threading
import hashlib

from fabnet_dht.data_block import ThreadSafeDataBlock
from fabnet_dht import leveldb

MAX_INDEX = pow(2, 16)
MAX_LEVEL = pow(2, 16)

class MDException(Exception):
    pass

class MDNotFound(MDException):
    pass

class MDAlreadyExists(MDException):
    pass

class MDNoFreeSpace(MDException):
    pass

class MDNotInit(MDException):
    pass


class UserInfo:
    STRUCT_FMT = '<20sQQH'
    def __init__(self, user_id_hash, storage_size, used_size, flags=0):
        self.user_id_hash = user_id_hash
        self.storage_size = storage_size
        self.used_size = used_size
        self.flags = flags

    def __repr__(self):
        return 'user_id_hash=%s, storage_size=%s, used_size=%s, flags=%s' \
                % (self.user_id_hash, self.storage_size, self.used_size, self.flags)

    def pack(self):
        return struct.pack(self.STRUCT_FMT, self.user_id_hash.decode('hex'), \
                        self.storage_size, self.used_size, self.flags)

    @classmethod
    def unpack(cls, raw):
        user_id_hash, st_size, fr_size, flags = struct.unpack(cls.STRUCT_FMT, raw)
        return UserInfo(user_id_hash.encode('hex'), st_size, fr_size, flags)
                
class MDKey:
    STRUCT_FMT = 'QLHH'
    def __init__(self, parent_id, path_hash, level, index=0):
        assert(level < MAX_LEVEL)
        assert(index < MAX_INDEX)
        
        self.parent_id = parent_id
        self.path_hash = path_hash & 0xffffffff
        self.level = level
        self.index = index

    def __repr__(self):
        return '[%016x][%08x][%02x][%02x]'%(self.parent_id, self.path_hash, self.level, self.index)

    def pack(self):
        return struct.pack(self.STRUCT_FMT, self.parent_id, self.path_hash, self.level, self.index)

    def make_parent(self):
        return (self.path_hash << 32) | (self.level << 16) | self.index

    def make_parent_range(self):
        parent = self.make_parent()
        start = struct.pack('QQ', parent, 1)
        end = struct.pack('QQ', parent, 0xffffffffffffffff)
        return start, end


    @classmethod
    def unpack(cls, raw):
        parent_id, path_hash, level, index = struct.unpack(cls.STRUCT_FMT, raw)
        return MDKey(parent_id, path_hash, level, index)

class MDItemValue:
    IT_FILE = 1
    IT_DIR = 2
    IT_INFO = 3

    def __init__(self, item_type, name, content=''):
        assert(len(name) < 255)
        self.item_type = item_type
        self.name = name
        self.content = content

    def pack(self):
        return '%s%s%s%s'%(chr(self.item_type), chr(len(self.name)), self.name, self.content)

    @classmethod
    def unpack(cls, raw):
        item_type = ord(raw[0])
        name_size = ord(raw[1])
        name = raw[2:2+name_size]
        content = raw[2+name_size:]
        return MDItemValue(item_type, name, content)


class MDDataBlockInfo:
    STRUCT_FMT = '<20sBL'
    REC_LEN = struct.calcsize(STRUCT_FMT)

    def __init__(self, db_key, replica_count, size):
        self.db_key = db_key
        self.replica_count = replica_count
        self.size = size

    def __repr__(self):
        return '[%s][%s][%s]'%(self.db_key, self.replica_count, self.size)

    def pack(self):
        return struct.pack(self.STRUCT_FMT, self.db_key.decode('hex'), \
                        self.replica_count, self.size)

    @classmethod
    def unpack(cls, raw):
        db_key, replica_count, size = struct.unpack(cls.STRUCT_FMT, raw)
        return MDDataBlockInfo(db_key.encode('hex'), replica_count, size)


class MDFileContent:
    def __init__(self, data_blocks):
        self.data_blocks = data_blocks

    def pack(self):
        return ''.join([db.pack() for db in self.data_blocks])

    @classmethod
    def unpack(cls, raw):
        i = 0
        dbs = []
        while True:
            raw_db = raw[i:i+MDDataBlockInfo.REC_LEN]
            if len(raw_db) < MDDataBlockInfo.REC_LEN:
                break
            dbs.append(MDDataBlockInfo.unpack(raw_db))
            i += MDDataBlockInfo.REC_LEN
        return MDFileContent(dbs)

class PathInfo:
    PT_DIR = 'dir'
    PT_FILE = 'file'

    def __init__(self, name, path_type, size, children=None):
        self.name = name
        self.path_type = path_type
        if children is None:
            children = []
        self.children = children
        self.size = size

    def add_child(self, child):
        self.children.append(child)

    def __repr__(self):
        ret = 'path: %s, path_type: %s, size: %s'%(self.name, self.path_type, self.size)
        if self.children:
            ret += '\nchildren:\n'
            for child in self.children:
                ret += '\tname: %s, size: %s\n'%(child.name, child.size)
        return ret

class UserMetadata:
    ROOT_KEY = MDKey(0, 0, 0)
    UI_KEY = ROOT_KEY.pack()

    def __init__(self, md_file):
        if not os.path.exists(md_file):
            os.mkdir(md_file)
        self.__db_lock = ThreadSafeDataBlock(os.path.join(md_file, 'dht.lock'))
        self.__db_lock.block()
        self.__db = leveldb.DB(md_file, create_if_missing=True, default_sync=True)

    def block(self):
        self.__db_lock.block()

    def unblock(self):
        self.__db_lock.unblock()

    def close(self):
        self.__db.close()
        self.__db_lock.unblock()
        self.__db_lock.close()

    def __get_item(self, parent, item_name, level, index=0):
        key = MDKey(parent.make_parent(), zlib.crc32(item_name), level, index)
        item = self.__db.get(key.pack(), None)
        if item is None:
            return None
        if MDItemValue.unpack(item).name != os.path.basename(item_name):
            return self.__get_item(parent, item_name, level, index+1)
        return key, item

    def __mk_item(self, parent, item_name, level, value, index=0):
        assert(index < MAX_INDEX)
        new_key = MDKey(parent.make_parent(), zlib.crc32(item_name), level, index)
        new_key_s = new_key.pack()
        item = self.__db.get(new_key_s, None)
        if item is None:
            self.__db[new_key_s] = value.pack()
            return new_key
        else:
            val = MDItemValue.unpack(item)
            if val.name == os.path.basename(item_name):
                raise MDAlreadyExists('Path %s is already exists'%item_name)
            return self.__mk_item(parent, item_name, value, level, index+1)

    def __find(self, path):
        parts = path.split('/')

        cur_key = self.ROOT_KEY
        cur_path = ''
        cur_level = 0
        cur_val = '\x02\x00'
        for part in parts:
            if not part: continue
            cur_level += 1
            cur_path += '/' + part
            item = self.__get_item(cur_key, cur_path, cur_level)
            if item is None:
                return None
            cur_key, cur_val = item

        return cur_key, cur_level, cur_val
            
    def __mkdir(self, path):
        if path.endswith('/'): path = path[:-1]
        dir_name, item_name = os.path.split(path)
        item = self.__find(dir_name)
        if item is None:
            parent_key, level = self.__mkdir(dir_name)
        else:
            parent_key, level, _ = item

        value = MDItemValue(MDItemValue.IT_DIR, item_name)
        level = level + 1
        return self.__mk_item(parent_key, path, level, value), level

    def _iter_keys(self):
        root = MDKey(0,0,0)
        st_key, _ = root.make_parent_range()
        root = MDKey(0xffffffffffffffff, 0xffffffff, 0xffff-1, 0xffff-1)
        _, end_key = root.make_parent_range()
        for key, value in self.__db.range(start_key=st_key, end_key=end_key):
            yield MDKey.unpack(key), MDItemValue.unpack(value)

    def _inc_used_size(self, size):
        user_info = self.get_user_info()
        user_info.used_size += size
        self.update_user_info(user_info)

    def _dec_used_size(self, size):
        user_info = self.get_user_info()
        user_info.used_size -= size
        if user_info.used_size < 0:
            user_info.used_size = 0
        self.update_user_info(user_info)

    def update_user_info(self, user_info):
        self.__db[self.UI_KEY] = user_info.pack()

    def add_user_storage_size(self, size):
        user_info = self.get_user_info()
        user_info.storage_size += size
        self.update_user_info(user_info)

    def get_user_info(self):
        raw = self.__db.get(self.UI_KEY, None)
        if raw is None:
            return UserInfo('', 0, 0, 0)
        return UserInfo.unpack(raw)

    def get_checksum(self):
        return hashlib.sha1(str(self.get_user_info())).hexdigest()

    def iterdir(self, path):
        if type(path) == unicode:
            path = path.encode('utf8')
        item = self.__find(path)
        if item is None:
            raise MDNotFound('Path %s does not found!'%path)
        dir_key, level, dir_val = item
        val = MDItemValue.unpack(dir_val)
        if val.item_type != MDItemValue.IT_DIR:
            raise MDException('Path %s is not dir!'%path)

        st_key, end_key = dir_key.make_parent_range()
        for key, value in self.__db.range(start_key=st_key, end_key=end_key):
            #yield MDItemValue.unpack(value).name
            yield MDKey.unpack(key), MDItemValue.unpack(value)

    def listdir(self, path):
        if type(path) == unicode:
            path = path.encode('utf8')
        return list([v.name for _,v in self.iterdir(path)])
    
    def make_path(self, path):
        if type(path) == unicode:
            path = path.encode('utf8')
        self.__mkdir(path)

    def update_path(self, path, data_blocks):
        if type(path) == unicode:
            path = path.encode('utf8')
        size = 0
        for db in data_blocks:
            size += db.size * (db.replica_count + 1)
        user_info = self.get_user_info()
        if user_info.storage_size == 0:
            raise MDNotInit('Does not initialized')
        elif user_info.storage_size < (user_info.used_size + size):
            raise MDNoFreeSpace('No free user space!')


        if path.endswith('/'): path = path[:-1]
        item = self.__find(path)
        if item is None:
            dir_name, item_name = os.path.split(path)
            item = self.__find(dir_name)
            if item is None:
                raise MDNotFound('Path %s does not found!'%dir_name)

            parent_key, level, _ = item
            content = MDFileContent(data_blocks)
            val = MDItemValue(MDItemValue.IT_FILE, item_name, content.pack())
            key = self.__mk_item(parent_key, path, level+1, val)
        else:
            size = 0
            key, _, val = item
            val = MDItemValue.unpack(val)
            content = MDFileContent.unpack(val.content)
            for_app = []
            for new_db in data_blocks:
                for db in content.data_blocks:
                    if new_db.db_key != db.db_key:
                        continue
                    if new_db.size != db.size:
                        size += (new_db.size - db.size) * (db.replica_count + 1)
                        db.size = new_db.size
                    break
                else:
                    size += new_db.size * (new_db.replica_count + 1)
                    for_app.append(new_db)

            user_info.used_size
            content.data_blocks += for_app
            val.content = content.pack()
            self.__db[key.pack()] = val.pack()

        user_info.used_size += size
        self.update_user_info(user_info)

    def get_path_info(self, path):
        '''
        return instance of PathInfo class
        '''
        if type(path) == unicode:
            path = path.encode('utf8')
        item = self.__find(path)
        if item is None:
            raise MDNotFound('Path %s does not found!'%path)

        key, _, val = item
        val = MDItemValue.unpack(val)
        path_info = PathInfo(path, None, 0)
        if val.item_type == MDItemValue.IT_DIR:
            path_info.path_type = PathInfo.PT_DIR
            for key, value in self.iterdir(path):
                f_size = 0
                if value.item_type == MDItemValue.IT_DIR:
                    f_tp = PathInfo.PT_DIR
                else:
                    f_tp = PathInfo.PT_FILE
                    content = MDFileContent.unpack(value.content)
                    for db in content.data_blocks:
                        f_size += db.size
                path_info.add_child(PathInfo(value.name, f_tp, f_size))
                path_info.size += f_size
        else:
            path_info.path_type = PathInfo.PT_FILE
            content = MDFileContent.unpack(val.content)
            for db in content.data_blocks:
                path_info.size += db.size

        return path_info

    def get_data_blocks(self, path):
        if type(path) == unicode:
            path = path.encode('utf8')
        item = self.__find(path)
        if item is None:
            raise MDNotFound('Path %s does not found!'%path)

        key, _, val = item
        val = MDItemValue.unpack(val)
        if val.item_type != MDItemValue.IT_FILE:
            raise MDException('Path %s is not file!'%path)

        content = MDFileContent.unpack(val.content)
        return copy.copy(content.data_blocks)

    def remove_path(self, path):
        if type(path) == unicode:
            path = path.encode('utf8')
        item = self.__find(path)
        if item is None:
            raise MDNotFound('Path %s does not found!'%path)
        key, _, val = item

        if key == self.ROOT_KEY:
            raise MDException('Can not remove root!')

        val = MDItemValue.unpack(val)
        if val.item_type == MDItemValue.IT_DIR:
            for _ in self.iterdir(path):
                raise MDException('Directory %s is not empty!'%path)
        else:
            size = 0
            content = MDFileContent.unpack(val.content)
            for db in content.data_blocks:
                size += db.size * (db.replica_count + 1)
            self._dec_used_size(size)

        del self.__db[key.pack()]


class MetadataCache:
    def __init__(self):
        self.__lock = threading.RLock()
        self.__cached = {}

    def call(self, path, method, *params, **kv_params):
        self.__lock.acquire()
        try:
            md_obj = self.__cached.get(path, None)
            if md_obj is None:
                md_obj = UserMetadata(path)
                self.__cached[path] = md_obj
        finally:
            self.__lock.release()

        md_obj.block()
        try:
            method = getattr(md_obj, method) 
            return method(*params, **kv_params)
        finally:
            md_obj.unblock()

    def destroy(self):
        self.__lock.acquire()
        try:
            for obj in self.__cached.values():
                obj.block()
                obj.close()
            self.__cached = {}
        finally:
            self.__lock.release()

    def close_md(self, path):
        self.__lock.acquire()
        try:
            if not path in self.__cached:
                return
            obj = self.__cached[path]
            obj.block()
            obj.close()
            del self.__cached[path]
        finally:
            self.__lock.release()
