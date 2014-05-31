#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package fabnet_dht.user_metadata

@author Konstantin Andrusenko
@date May 31, 2014
"""

import hashlib
from M2Crypto import X509

from fabnet.core.fri_base import FabnetPacketRequest, FabnetPacketResponse
from fabnet.core.fri_client import FriClient
from fabnet.core.constants import RC_OK, RC_ERROR
from fabnet_dht.constants import RC_NO_DATA, MIN_REPLICA_COUNT 


class NimbusError(Exception):
    pass


class Nimbus:
    def __init__(self, key_storage, endpoint):
        cert = X509.load_cert_string(key_storage.cert())
        self.__user_id_hash = hashlib.sha1(cert.get_subject().CN).hexdigest()
        self.__client = FriClient(key_storage)
        self.__endpoint = endpoint

    def __get_keys_info(self, key, replica_count):
        packet = FabnetPacketRequest(method='GetKeysInfo', \
                parameters={'key': key, 'replica_count': replica_count})

        ret_packet = self.__client.call_sync(self.__endpoint, packet)
        if ret_packet.ret_code != RC_OK:
            raise NimbusError('GetKeysInfo error: %s'%ret_packet.ret_message)
        keys_info = ret_packet.ret_parameters['keys_info']

        return keys_info

    def put_data_block(self, data_block, key=None, replica_count=MIN_REPLICA_COUNT, \
                                    init_block=True, wait_writes=MIN_REPLICA_COUNT+1):
        keys_info = self.__get_keys_info(key, replica_count)
        key, _, nodeaddr = keys_info[0]

        params = {'wait_writes_count': wait_writes, 'replica_count': replica_count, \
                'init_block': init_block, 'key': key}
        packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data_block)

        ret_packet = self.__client.call_sync(nodeaddr, packet_obj)
        if ret_packet.ret_code != RC_OK:
            raise NimbusError('ClientPutData error: %s'%ret_packet.ret_message)
        return ret_packet.ret_parameters['key']

    def get_data_block(self, key, replica_count=MIN_REPLICA_COUNT):
        keys_info = self.__get_keys_info(key, replica_count)

        for key, dbct, nodeaddr in keys_info:
            params = {'key': key, 'dbct': dbct, 'user_id_hash': self.__user_id_hash}
            req = FabnetPacketRequest(method='GetDataBlock', parameters=params)

            resp = self.__client.call_sync(nodeaddr, req)
            if resp.ret_code == RC_OK:
                return resp.binary_data
            if resp.ret_code == RC_NO_DATA:
                continue
            raise NimbusError('GetDataBlock error: %s'%resp.ret_message)
        raise NimbusError('No data found!')

    def delete_data_block(self, key, replica_count=MIN_REPLICA_COUNT):
        params = {'key': key, 'replica_count': replica_count}
        packet_obj = FabnetPacketRequest(method='ClientDeleteData', parameters=params)
        resp = self.__client.call_sync(self.__endpoint, packet_obj)
        if resp.ret_code != RC_OK:
            raise NimbusError('ClientDeleteData error: %s' % resp.ret_message)

