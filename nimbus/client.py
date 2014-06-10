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
from fabnet_dht.constants import RC_NO_DATA, MIN_REPLICA_COUNT, RC_ALREADY_EXISTS 


class NimbusError(Exception):
    pass


class Nimbus:
    def __init__(self, key_storage, endpoint):
        if key_storage:
            cert = X509.load_cert_string(key_storage.cert())
            user_id = cert.get_subject().CN
        else:
            user_id = 'None'
        self.__user_id_hash = hashlib.sha1(user_id).hexdigest()
        self.__client = FriClient(key_storage)
        self.__endpoint = endpoint

    def __get_keys_info(self, key, replica_count):
        packet = FabnetPacketRequest(method='GetKeysInfo', \
                parameters={'key': key, 'replica_count': replica_count})

        ret_packet = self.__client.call_sync(self.__endpoint, packet)
        if ret_packet.ret_code != RC_OK:
            raise NimbusError('GetKeysInfo error: %s'%ret_packet.ret_message)
        keys_info = ret_packet.ret_parameters.get('keys_info', None)
        if not keys_info:
            raise Exception('GetKeysInfo error: %s [%s]'%(ret_packet.ret_message, ret_packet.ret_parameters))

        return keys_info

    def put_data_block(self, data_block, u_key=None, replica_count=MIN_REPLICA_COUNT, \
                                    init_block=True, wait_writes=MIN_REPLICA_COUNT+1):
        keys_info = self.__get_keys_info(u_key, replica_count)
        key, _, nodeaddr = keys_info[0]

        params = {'wait_writes_count': wait_writes, 'replica_count': replica_count, \
                'init_block': init_block, 'key': key}
        packet_obj = FabnetPacketRequest(method='ClientPutData', parameters=params, binary_data=data_block)

        ret_packet = self.__client.call_sync(nodeaddr, packet_obj)

        if (ret_packet.ret_code == RC_ALREADY_EXISTS) and (u_key is None):
            return self.put_data_block(data_block, u_key, replica_count, init_block, wait_writes)

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

