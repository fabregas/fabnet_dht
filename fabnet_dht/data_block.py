#!/usr/bin/python
"""
Copyright (C) 2012 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.data_block

@author Konstantin Andrusenko
@date October 06, 2012
"""
from datetime import datetime
import time
import struct
import hashlib

DATA_BLOCK_LABEL = 'FDB01'
STRUCT_FMT = '<5sd20sb20s20s'


class DataBlockHeader:
    HEADER_LEN = struct.calcsize(STRUCT_FMT)

    @classmethod
    def pack(cls, key, replica_count, checksum, user_id=None):
        if not user_id:
            user_id = ''
        user_id = str(user_id)
        user_id_hash = hashlib.sha1(user_id).digest()
        
        unixtime = time.mktime(datetime.utcnow().timetuple())
        try:
            header = struct.pack(STRUCT_FMT, DATA_BLOCK_LABEL, unixtime, key.decode('hex'), \
                            replica_count, checksum.decode('hex'), user_id_hash)
        except Exception, err:
            raise Exception('Data block header packing failed! Details: %s'%err)

        return header

    @classmethod
    def match(cls, user_id, user_hash):
        if not user_id:
            user_id = ''
        user_id_hash = hashlib.sha1(str(user_id)).digest()
        return user_id_hash == user_hash

    @classmethod
    def unpack(cls, data):
        header = data[:cls.HEADER_LEN]
        try:
            db_label, put_unixtime, primary_key, replica_count, checksum, user_id_hash = struct.unpack(STRUCT_FMT, header)
        except Exception, err:
            raise Exception('Data block header is invalid! Details: %s'%err)

        if db_label != DATA_BLOCK_LABEL:
            raise Exception('Corrupted data block! No block label found')

        return primary_key.encode('hex'), replica_count, checksum.encode('hex'), user_id_hash, put_unixtime

    @classmethod
    def check_raw_data(cls, binary_data, exp_checksum=None):
        header = binary_data.read(cls.HEADER_LEN)

        _, _, checksum, _, _ = cls.unpack(header)

        if exp_checksum and exp_checksum != checksum:
            raise Exception('Data checksum is not equal to expected')

        h_func = hashlib.sha1('')
        while True:
            chunk = binary_data.get_next_chunk()
            if chunk is None:
                break
            h_func.update(chunk)

        if checksum != h_func.hexdigest():
            raise Exception('Data block has bad checksum')


