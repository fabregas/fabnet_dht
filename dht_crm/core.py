#!/usr/bin/python
"""
Copyright (C) 2014 Konstantin Andrusenko
    See the documentation for further information on copyrights,
    or contact the author. All Rights Reserved.

@package dht_crm.core

@author Konstantin Andrusenko
@date June 22, 2014
"""
import httplib
import urllib
import socket
import json
import random
import string
import hashlib

from M2Crypto import EVP

from fabnet.core.constants import CLIENT_CERTIFICATE
from fabnet.core.fri_base import FabnetPacketRequest
from fabnet.core.fri_client import FriClient
from fabnet_ca.cert_req_generator import generate_keys, gen_request


class DHTCRM:
    def __init__(self, ks, ca_addr):
        self.ks = ks
        parts = ca_addr.split('://')
        if len(parts) == 1:
            scheme = 'http'
            addr = ca_addr
        else:
            scheme, addr = parts

        if scheme.lower() == 'http':
            self.ca_conn_class = httplib.HTTPConnection
        else:
            self.ca_conn_class = httplib.HTTPSConnection

        self.scheme = scheme
        self.ca_addr = addr

    def register_user(self, service_term, storage_capacity):
        activation_key = ''.join(random.choice(string.uppercase+string.digits) for i in xrange(15))
        key = EVP.load_key_string(self.ks.private())
        key.reset_context()
        key.sign_init()
        key.sign_update(activation_key)
        sign = key.sign_final()

        conn = self.__ca_call('/add_new_certificate_info', {'sign_cert': self.ks.cert(), 'signed_data': sign, \
                    'activation_key': activation_key, 'cert_term': service_term, 'cert_add_info': json.dumps({'storage_capacity': storage_capacity}), \
                    'cert_role': CLIENT_CERTIFICATE})

        try:
            resp = conn.getresponse()
            if resp.status != 200:
                raise Exception('CA service error! Add new certificate: [%s %s] %s'%\
                        (resp.status, resp.reason, resp.read()))
        finally:
            conn.close()

        return activation_key

    def gen_cert_req(self, user_cn):
        pub, pri = generate_keys(None, length=1024)
        cert_req = gen_request(pri, user_cn, passphrase=None, OU=CLIENT_CERTIFICATE)
        return pri, cert_req

    def activate(self, act_key, cert_req):
        conn = self.__ca_call('/generate_certificate', \
                {'cert_req_pem': cert_req, 'activation_key': act_key, 'unique_cn': True})

        try:
            resp = conn.getresponse()
            if resp.status != 200:
                raise Exception('CA service error! Generate certificate: [%s %s] %s'%\
                        (resp.status, resp.reason, resp.read()))
            cert = resp.read()
        finally:
            conn.close()

        return cert

    def activate_on_dht(self, user_name, dht_endpoint):
        conn = self.__ca_call('/get_certificate_info', {'cn': user_name})
        try:
            resp = conn.getresponse()
            if resp.status != 200:
                raise Exception('CA service error! Get cert info: [%s %s] %s'%\
                        (resp.status, resp.reason, resp.read()))
            data = resp.read()
        finally:
            conn.close()

        data = json.loads(data)
        ###data['cert_term']
        #cert_add_info
        #cert_term
        #status
        fri_client = FriClient(self.ks)
        add_info = json.loads(data['cert_add_info'])

        params = {'user_id_hash': hashlib.sha1(user_name).hexdigest(), 'storage_size': add_info['storage_capacity']}

        packet_obj = FabnetPacketRequest(method='UpdateUserProfile', parameters=params)
        ret_packet = fri_client.call_sync(dht_endpoint, packet_obj) 
        if ret_packet.ret_code != 0:
            raise Exception(ret_packet.ret_message)

    def __ca_call(self, path, params={}, method='POST'):
        try:
            conn = self.ca_conn_class(self.ca_addr)
            params = urllib.urlencode(params)
            conn.request(method, path, params)
        except Exception, err:
            raise Exception('CA service does not respond at %s://%s%s\n%s'%(self.scheme, self.ca_addr, path, err))

        return conn

