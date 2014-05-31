import unittest
from test_utils import *
from M2Crypto import RSA, X509, EVP

class TestDHTInitProcedure(unittest.TestCase):
    NODES = [(1986, '/tmp/dht_1986_home', NODE1_KS), (1987, '/tmp/dht_1987_home', NODE2_KS)]

    def test01_dht_init(self):
        servers = []
        try:
            N = self.NODES
            server = TestServerThread(N[0][0], N[0][1], config={'MAX_USED_SIZE_PERCENTS': 99, 'FLUSH_MD_CACHE_TIMEOUT': 0.2},  ks_path=N[0][2])
            servers.append(server)
            server.start()
            time.sleep(1)

            server = TestServerThread(N[1][0], N[1][1], neighbour='127.0.0.1:%s'%N[0][0], \
                    config={'MAX_USED_SIZE_PERCENTS': 99, 'FLUSH_MD_CACHE_TIMEOUT': 0.2}, ks_path=N[1][2])
            servers.append(server)
            server.start()

            time.sleep(.2)
            server.wait_oper_status(DS_NORMALWORK)

            node86_stat = servers[0].get_stat()
            node87_stat = servers[1].get_stat()

            self.assertEqual(long(node86_stat['DHTInfo']['range_start'], 16), 0L)
            self.assertEqual(long(node86_stat['DHTInfo']['range_end'], 16), MAX_KEY/2)
            self.assertEqual(long(node87_stat['DHTInfo']['range_start'], 16), MAX_KEY/2+1)
            self.assertEqual(long(node87_stat['DHTInfo']['range_end'], 16), MAX_KEY)

            table_dump = server.operator.dump_ranges_table()
            table = HashRangesTable()
            table.load(table_dump)
            self.assertEqual(table.count(), 2)
            hr = table.find(0)
            self.assertEqual(hr.start, 0)
            self.assertEqual(hr.end, MAX_KEY/2)
            hr = table.find(MAX_KEY)
            self.assertEqual(hr.start, MAX_KEY/2+1)
            self.assertEqual(hr.end, MAX_KEY)

            self.UMetadata_test(servers)
            self.PutDataBlock_test(servers)
            self.PutGet_test(servers)
            self.Objects_test(servers)
        finally:
            for server in servers:
                server.stop()

    def UMetadata_test(self, servers, need_restore_test=True):
        add_list = [('/test.out', [('%040x'%23124, 2, 0, 22223), ('%040x'%542322, 2, 22223, 3333)])]
        KEY = MAX_KEY - 333
        ret = servers[1].update_md('%040x'%KEY, add_list, rm_list=[])
        self.assertEqual(ret.ret_code, RC_MD_NOTINIT, ret.ret_message)
        
        ret = servers[1].update_user_md('%040x'%KEY, 100500)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[1].update_md('%040x'%KEY, add_list, rm_list=[])
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        path = os.path.join(servers[1].home_dir, 'dht_range', FSMappedDHTRange.DBCT_MD_MASTER, '%040x'%KEY)
        self.assertTrue(os.path.exists(path), path)
        if need_restore_test:
            os.system('rm -rf %s'%path)
            time.sleep(0.5)

        add_list = [('/test2.out', [('%040x'%5426662, 3, 0, 133)])]
        ret = servers[1].update_md('%040x'%KEY, add_list, rm_list=[])
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        

    def PutDataBlock_test(self, servers):
        r_data = '12323423r3fdjvnoi4fhruwefwwfw'*100
        key = '%040x'%23412
        db_header = DataBlockHeader(key, 1, hashlib.sha1(r_data).hexdigest(), hashlib.sha1('1324').hexdigest())
        data = db_header.pack() + r_data
        ret = servers[0].put_data_block(data, key, FSMappedDHTRange.DBCT_MASTER) 
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[0].put_data_block(data, key, FSMappedDHTRange.DBCT_MASTER, \
                    user_id_hash=hashlib.sha1('13244').hexdigest(), careful_save=True) 
        self.assertEqual(ret.ret_code, RC_PERMISSION_DENIED, ret.ret_message)

        ret = servers[0].put_data_block(data, key, FSMappedDHTRange.DBCT_MASTER, \
                user_id_hash=hashlib.sha1('1324').hexdigest(), careful_save=True, stored_unixtime=23523) 
        self.assertEqual(ret.ret_code, RC_OLD_DATA, ret.ret_message)

        ret = servers[0].put_data_block(data, key, FSMappedDHTRange.DBCT_MASTER, \
                user_id_hash=hashlib.sha1('1324').hexdigest(), careful_save=True) 
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[0].put_data_block(data, key, FSMappedDHTRange.DBCT_MASTER, init_block=True) 
        self.assertEqual(ret.ret_code, RC_ALREADY_EXISTS, ret.ret_message)


    def PutGet_test(self, servers):
        data = 'sdfw34pofi20jpifj3049f23fdjvnoi4fhruwefwwfw'*100
        data2 = '342234242pifj3049f23fdjvnoi4fhruwefwwfw'*80
        client_ks = init_keystore(USER1_KS, USER_PWD)
        client2_ks = init_keystore(USER2_KS, USER_PWD)
        
        ret = servers[1].put(data, wait_writes=3, init_block=True, client_ks=client_ks)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        master_key = ret.ret_parameters['key']

        ret = servers[0].put(data2, wait_writes=1, init_block=False, client_ks=client2_ks, key=master_key)
        self.assertNotEqual(ret.ret_code, 0, ret.ret_message)

        keys_info = servers[1].get_keys_info(None)
        master_key2 = keys_info[0][0]
        ret = servers[0].put(data2, wait_writes=1, init_block=True, client_ks=client2_ks, key=master_key2, replica_count=4)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        key = ret.ret_parameters['key']
        self.assertEqual(key, master_key2)

        #get test
        ret = servers[0].get_data_block('%040x'%3422222, FSMappedDHTRange.DBCT_REPLICA, client_ks)
        self.assertEqual(ret.ret_code, RC_NO_DATA, ret.ret_message)

        keys_info = servers[0].get_keys_info(master_key)
        cnt = 0
        for key, dbct, addr in keys_info:
            server = servers[0] if addr.endswith('86') else servers[1]
            ret = server.get_data_block(key, dbct, client_ks)
            self.assertEqual(ret.ret_code, 0, ret.ret_message)
            self.assertEqual(ret.binary_data.data(), data)
            cnt += 1
        self.assertEqual(cnt, 3)


        time.sleep(.5)
        keys_info = servers[1].get_keys_info(master_key2, 4)
        cnt = 0
        for key, dbct, addr in keys_info:
            server = servers[0] if addr.endswith('86') else servers[1]
            ret = server.get_data_block(key, dbct, client2_ks)
            self.assertEqual(ret.ret_code, 0, ret.ret_message)
            self.assertEqual(ret.binary_data.data(), data2)
            ret = server.get_data_block(key, dbct, client_ks)
            self.assertEqual(ret.ret_code, RC_PERMISSION_DENIED, ret.ret_message)
            cnt += 1
        self.assertEqual(cnt, 5)

        #delete data
        ret = servers[1].delete('234d332dw', client_ks)
        self.assertEqual(ret.ret_code, RC_ERROR, ret.ret_message)

        ret = servers[1].delete('%040x'%2343214, client_ks)
        self.assertEqual(ret.ret_code, RC_ERROR, ret.ret_message)

        ret = servers[1].delete(master_key, client2_ks)
        self.assertEqual(ret.ret_code, RC_ERROR, ret.ret_message)

        ret = servers[1].delete(master_key, client_ks)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        keys_info = servers[0].get_keys_info(master_key)
        for key, dbct, addr in keys_info:
            server = servers[0] if addr.endswith('86') else servers[1]
            ret = server.get_data_block(key, dbct, client_ks)
            self.assertEqual(ret.ret_code, RC_NO_DATA, ret.ret_message)

        #check data block
        ret = servers[0].check_data_block('%040x'%324235352, FSMappedDHTRange.DBCT_MASTER, checksum=None)
        self.assertEqual(ret.ret_code, RC_NO_DATA, ret.ret_message)

        keys_info = servers[1].get_keys_info(master_key2, 4)
        cnt = 0
        for key, dbct, addr in keys_info:
            server = servers[0] if addr.endswith('86') else servers[1]
            ret = server.check_data_block(key, dbct, checksum=None)
            self.assertEqual(ret.ret_code, 0, ret.ret_message)

            ret = server.check_data_block(key, dbct, checksum='2313131')
            self.assertEqual(ret.ret_code, RC_INVALID_DATA, ret.ret_message)

        ret = server.check_data_block(key, dbct)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        path = os.path.join(server.home_dir, 'dht_range/%s/%s'%(dbct, key))
        rb = open(path, 'r+b')
        rb.seek(100)
        rb.write('hm')
        rb.close()
        ret = server.check_data_block(key, dbct)
        self.assertEqual(ret.ret_code, RC_INVALID_DATA, ret.ret_message)

    def _get_cl_cn(self, ks):
        cert = X509.load_cert_string(ks.cert())
        return hashlib.sha1(cert.get_subject().CN).hexdigest()

    def Objects_test(self, servers):
        data = 'sdfw34pofi20jpifj3049f23fdjvnoi4fhruwefwwfw'*100
        data2 = '342234242pifj3049f23fdjvnoi4fhruwefwwfw'*80
        data3 = 'gsdgsdsdfai4fhru'*120
        client_ks = init_keystore(USER1_KS, USER_PWD)
        client2_ks = init_keystore(USER2_KS, USER_PWD)

        keys_info = servers[1].get_keys_info(None)
        mkey = keys_info[0][0]
        ret = servers[1].put_object_part('/test_file.out', data, seek=0, wait_writes=3, key=mkey, client_ks=client_ks)
        self.assertNotEqual(ret.ret_code, 0, ret.ret_message)
        server = servers[0] if keys_info[0][2].endswith('86') else servers[1]
        ret = server.get_data_block(mkey, FSMappedDHTRange.DBCT_MASTER, client_ks)
        self.assertNotEqual(ret.ret_code, 0, ret.ret_message)

        
        ret = servers[1].update_user_md(self._get_cl_cn(client_ks), 100500)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        ret = servers[0].update_user_md(self._get_cl_cn(client2_ks), 1005000)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[1].put_object_part('/test_file.out', data, seek=0, wait_writes=3, init_block=True, client_ks=client_ks)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        master_key = ret.ret_parameters['key']
        size = ret.ret_parameters['size']
        keys_info = servers[1].get_keys_info(master_key, 2)
        server = servers[0] if keys_info[0][2].endswith('86') else servers[1]
        ret = server.get_data_block(master_key, FSMappedDHTRange.DBCT_MASTER, client_ks)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[0].put_object_part('/test_file.out', data2, wait_writes=2, init_block=False, client_ks=client2_ks, replica_count=4)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[1].put_object_part('/test_file.out', data3, seek=size, wait_writes=2, init_block=True, client_ks=client_ks)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)

        ret = servers[0].get_object_info('/tttt', client2_ks)
        self.assertEqual(ret.ret_code, RC_ERROR, ret.ret_message)

        ret = servers[0].get_object_info('/test_file.out', client2_ks)
        self.assertEqual(ret.ret_code, 0, ret.ret_message)
        self.assertEqual(ret.ret_parameters['user_info']['used_size'], len(data2)*5)
        self.assertEqual(ret.ret_parameters['user_info']['storage_size'], 1005000)
        self.assertEqual(ret.ret_parameters['data_blocks'][0]['seek'], 0)
        self.assertEqual(ret.ret_parameters['data_blocks'][0]['replica_count'], 4)
        self.assertEqual(ret.ret_parameters['data_blocks'][0]['size'], len(data2))
        self.assertEqual(ret.ret_parameters['path_info']['type'], 'file')
        self.assertEqual(ret.ret_parameters['path_info']['name'], '/test_file.out')

        ret = servers[0].get_object_info('/test_file.out', client_ks)
        self.assertEqual(ret.ret_parameters['user_info']['used_size'], len(data)*3 + len(data3)*3)
        self.assertEqual(ret.ret_parameters['user_info']['storage_size'], 100500)
        self.assertEqual(ret.ret_parameters['data_blocks'][0]['db_key'], master_key)
        self.assertEqual(ret.ret_parameters['data_blocks'][0]['seek'], 0)
        self.assertEqual(ret.ret_parameters['data_blocks'][0]['size'], len(data))
        self.assertEqual(ret.ret_parameters['data_blocks'][1]['seek'], size)
        self.assertEqual(ret.ret_parameters['data_blocks'][1]['size'], len(data3))
        self.assertEqual(ret.ret_parameters['path_info']['type'], 'file')
        self.assertEqual(ret.ret_parameters['path_info']['name'], '/test_file.out')

        self.assertEqual(ret.ret_code, 0, ret.ret_message)

    def test02_dht_restore_after_network_fail(self):
        servers = []
        try:
            N = self.NODES
            server = TestServerThread(N[0][0], N[0][1], config={'MAX_USED_SIZE_PERCENTS': 99},  ks_path=N[0][2])
            servers.append(server)
            server.start()
            time.sleep(1)

            server = TestServerThread(N[1][0], N[1][1], neighbour='127.0.0.1:%s'%N[0][0], \
                        config={'MAX_USED_SIZE_PERCENTS': 99}, ks_path=N[1][2])
            servers.append(server)
            server.start()

            time.sleep(.2)
            server.wait_oper_status(DS_NORMALWORK)

            node86_stat = servers[0].get_stat()
            node87_stat = servers[1].get_stat()


            packet_obj = FabnetPacketRequest(method='TopologyCognition', sender=None)
            fri_client = FriClient(servers[0].ks)
            fri_client.call('127.0.0.1:1987', packet_obj)
            time.sleep(2)

            os.system('sudo /sbin/iptables -A INPUT -p tcp --destination-port 1986 -j DROP')
            os.system('sudo /sbin/iptables -A OUTPUT -p tcp --destination-port 1986 -j DROP')

            time.sleep(10)
            node87_stat = servers[1].get_stat()
            self.assertEqual(long(node87_stat['DHTInfo']['range_start'], 16), 0L)
            self.assertEqual(long(node87_stat['DHTInfo']['range_end'], 16), MAX_KEY)

            os.system('sudo /sbin/iptables -D INPUT -p tcp --destination-port 1986 -j DROP')
            os.system('sudo /sbin/iptables -D OUTPUT -p tcp --destination-port 1986 -j DROP')

            time.sleep(10)
            node86_stat = servers[0].get_stat()
            node87_stat = servers[1].get_stat()

            self.assertEqual(long(node86_stat['DHTInfo']['range_start'], 16), 0L)
            self.assertEqual(long(node86_stat['DHTInfo']['range_end'], 16), MAX_KEY/2)
            self.assertEqual(long(node87_stat['DHTInfo']['range_start'], 16), MAX_KEY/2+1)
            self.assertEqual(long(node87_stat['DHTInfo']['range_end'], 16), MAX_KEY)
        finally:
            for server in servers:
                server.stop()


    def test02_dht_init_fail(self):
        servers = []
        try:
            N = self.NODES
            server = TestServerThread(N[0][0], N[0][1], config={'MAX_USED_SIZE_PERCENTS': 99},  ks_path=N[0][2])
            servers.append(server)
            server.start()
            time.sleep(1)

            server = TestServerThread(N[1][0], N[1][1], neighbour='127.0.0.1:%s'%N[0][0], \
                    config={'DHT_CYCLE_TRY_COUNT':20, 'ALLOW_USED_SIZE_PERCENTS':0}, ks_path=N[1][2])
            servers.append(server)
            server.start()

            server = servers[0]
            server1 = servers[1]
            time.sleep(1.5)
            self.assertNotEqual(server1.operator.get_status(), DS_NORMALWORK)
            for i in xrange(3):
                try:
                    server.operator.split_range(0, 100500)
                    break
                except Exception, err:
                    time.sleep(.1)
            time.sleep(.2)
            server.operator.join_subranges()
            time.sleep(.2)
            server1.operator.update_config({'ALLOW_USED_SIZE_PERCENTS':70})
            server1.wait_oper_status(DS_NORMALWORK, server1.operator.get_config())

            node86_stat = servers[0].get_stat()
            node87_stat = servers[1].get_stat()

            self.assertEqual(long(node86_stat['DHTInfo']['range_start'], 16), 0L)
            self.assertEqual(long(node86_stat['DHTInfo']['range_end'], 16), MAX_KEY/2)
            self.assertEqual(long(node87_stat['DHTInfo']['range_start'], 16), MAX_KEY/2+1)
            self.assertEqual(long(node87_stat['DHTInfo']['range_end'], 16), MAX_KEY)
        finally:
            for server in servers:
                server.stop()



    def test03_dht_collisions_resolutions(self):
        servers = []
        try:
            home1 =  self._make_fake_hdd('node_1986', 1024, '/dev/loop0')
            home2 =  self._make_fake_hdd('node_1987', 1024, '/dev/loop1')

            N = self.NODES
            server = TestServerThread(N[0][0], home1, config={},  ks_path=N[0][2], clear_home=False)
            servers.append(server)
            server.start()
            time.sleep(1)

            server = TestServerThread(N[1][0], home2, neighbour='127.0.0.1:%s'%N[0][0], \
                    config={}, ks_path=N[1][2], clear_home=False)
            servers.append(server)
            server.start()
            time.sleep(.2)
            server.wait_oper_status(DS_NORMALWORK)
            server = servers[0]
            server1 = servers[1]

            print 'REMOVING 1987 NODE RANGE FROM DHT'
            rm_list = [(MAX_KEY/2+1, MAX_KEY, '127.0.0.1:1987')]
            params = {'append': [], 'remove': rm_list}
            packet_obj = FabnetPacketRequest(method='UpdateHashRangeTable', sender='127.0.0.1:1986', parameters=params)
            server.operator.call_network(packet_obj)

            server.wait_oper_status(DS_NORMALWORK)
            server1.wait_oper_status(DS_NORMALWORK)

            table_dump = server1.operator.dump_ranges_table()
            table = HashRangesTable()
            table.load(table_dump)
            self.assertEqual(table.count(), 2)
            hr = table.find(0)
            self.assertEqual(hr.start, 0)
            self.assertEqual(hr.end, MAX_KEY/2)
            hr = table.find(MAX_KEY)
            self.assertEqual(hr.start, MAX_KEY/2+1)
            self.assertEqual(hr.end, MAX_KEY)


            step = MAX_KEY/2/100
            for i in range(100):
                data = ''.join(random.choice(string.letters) for i in xrange(8*1024))
                server.put_data_block(data, i*step)

            node86_stat = server.get_stat()
            self.assertEqual(node86_stat['DHTInfo']['free_size_percents'] < 20, True, node86_stat['DHTInfo']['free_size_percents'])
            time.sleep(1)

            step = MAX_KEY/2/10
            for i in range(5):
                data = ''.join(random.choice(string.letters) for i in xrange(int(16*1024)))
                server.put_data_block(data, i*step, FSMappedDHTRange.DBCT_REPLICA)

            node86_stat = server.get_stat()
            node87_stat = server1.get_stat()

            self.assertEqual(node86_stat['DHTInfo']['free_size_percents'] < 10, True, node86_stat['DHTInfo']['free_size_percents'] )
            self.assertEqual(node87_stat['DHTInfo']['free_size_percents'] > 90, True)
            print node86_stat['DHTInfo']['free_size_percents'], node87_stat['DHTInfo']['free_size_percents'], '==============='
            time.sleep(4)
            node86_stat = server.get_stat()
            node87_stat = server1.get_stat()
            print node86_stat['DHTInfo']['free_size_percents'], node87_stat['DHTInfo']['free_size_percents'], '==============='
            self.assertEqual(node86_stat['DHTInfo']['free_size_percents'] > 15, True, node86_stat['DHTInfo']['free_size_percents'] )
            self.assertEqual(node87_stat['DHTInfo']['free_size_percents'] < 90, True)
        finally:
            for server in servers:
                server.stop()

            self._destroy_fake_hdd('node_1986', '/dev/loop0')
            self._destroy_fake_hdd('node_1987', '/dev/loop1')

    def test05_repair_data(self):
        servers = []
        monitor = None
        try:
            monitor_home = '/tmp/node_monitor_home'
            N = self.NODES
            server = TestServerThread(N[0][0], N[0][1], config={'MAX_USED_SIZE_PERCENTS': 99},  ks_path=N[0][2])
            servers.append(server)
            server.start()
            time.sleep(1)

            server = TestServerThread(N[1][0], N[1][1], neighbour='127.0.0.1:%s'%N[0][0], \
                    config={}, ks_path=N[1][2])
            servers.append(server)
            server.start()
            time.sleep(.2)

            monitor = TestServerThread(1990, monitor_home, is_monitor=True, neighbour='127.0.0.1:1986', ks_path=N[0][2])

            monitor.start()
            time.sleep(1.5)
            server.wait_oper_status(DS_NORMALWORK)
            server = servers[0]
            server1 = servers[1]

            data = 'Hello, fabregas!'*10
            ret = server.put(data)
            self.assertEqual(ret.ret_code, 0, ret.ret_message)
            data_key = ret.ret_parameters['key']

            data = 'This is replica data!'*10
            ret = server1.put(data)
            self.assertEqual(ret.ret_code, 0, ret.ret_message)
            data_key2 = ret.ret_parameters['key']
            
            self.UMetadata_test(servers, need_restore_test=False)

            time.sleep(.2)
            client = FriClient(servers[0].ks)
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters={})
            rcode, rmsg = client.call('127.0.0.1:1986', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(1.5)

            NDB = os.path.join(monitor_home, NOTIFICATIONS_DB)
            db = SafeJsonFile(NDB)
            data = db.read()
            os.remove(NDB)
            events = data.get('notifications', [])
            events = filter(lambda e: e['event_topic']=='RepairDataBlocks', events)

            stat = 'processed_local_blocks=%i, invalid_local_blocks=0, repaired_foreign_blocks=0, failed_repair_foreign_blocks=0'
            self.assertEqual(len(events), 2, events)
            event86 = event87 = None
            for event in events:
                if event['event_provider'] == '127.0.0.1:1986':
                    event86 = event
                elif event['event_provider'] == '127.0.0.1:1987':
                    event87 = event

            self.assertEqual(event86['event_type'], ET_INFO)
            cnt86 = len(os.listdir(server.get_range_dir())) + len(os.listdir(server.get_replicas_dir()))
            self.assertTrue(stat%cnt86 in event86['event_message'], event86['event_message'])

            self.assertEqual(event87['event_type'], ET_INFO)
            cnt87 = len(os.listdir(server1.get_range_dir())) + len(os.listdir(server1.get_replicas_dir())) + 1
            self.assertTrue(stat%cnt87 in event87['event_message'], event87['event_message'])

            node86_stat = server.get_stat()
            server.stop()
            server.join()
            server = None
            time.sleep(1)

            params = {'check_range_start': node86_stat['DHTInfo']['range_start'], 'check_range_end': node86_stat['DHTInfo']['range_end']}
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters=params)
            rcode, rmsg = client.call('127.0.0.1:1987', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(2)

            db = SafeJsonFile(NDB)
            data = db.read()
            os.remove(NDB)
            events = data.get('notifications', [])
            events = filter(lambda e: e['event_topic']=='RepairDataBlocks', events)

            self.assertEqual(len(events), 1, events)
            event86 = event87 = None
            for event in events:
                if event['event_provider'] == '127.0.0.1:1986':
                    event86 = event
                elif event['event_provider'] == '127.0.0.1:1987':
                    event87 = event
            self.assertEqual(event87['event_type'], ET_INFO)
            stat_rep = 'processed_local_blocks=%i, invalid_local_blocks=0, repaired_foreign_blocks=%i, failed_repair_foreign_blocks=0'
            self.assertTrue(stat_rep%(cnt87, cnt86+1) in event87['event_message'], event87['event_message'])

            open(os.path.join(server1.get_range_dir(), data_key), 'wr').write('wrong data')
            open(os.path.join(server1.get_range_dir(), data_key2), 'ar').write('wrong data')

            server1.operator.user_metadata_call(os.path.join(server1.home_dir, \
                    'dht_range/mmd/fffffffffffffffffffffffffffffffffffffeb2'), 'remove_path', '/test2.out')

            time.sleep(.2)
            packet_obj = FabnetPacketRequest(method='RepairDataBlocks', is_multicast=True, parameters={})
            rcode, rmsg = client.call('127.0.0.1:1987', packet_obj)
            self.assertEqual(rcode, 0, rmsg)
            time.sleep(2)

            db = SafeJsonFile(NDB)
            data = db.read()
            os.remove(NDB)
            events = data.get('notifications', [])
            events = filter(lambda e: e['event_topic']=='RepairDataBlocks', events)

            self.assertEqual(len(events), 1)
            for event in events:
                if event['event_provider'] == '127.0.0.1:1986':
                    event86 = event
                elif event['event_provider'] == '127.0.0.1:1987':
                    event87 = event

            stat_rep = 'processed_local_blocks=%i, invalid_local_blocks=%i, repaired_foreign_blocks=%i, failed_repair_foreign_blocks=0'
            self.assertTrue(stat_rep%(cnt87+cnt86, 1, 4) in event87['event_message'], event87['event_message'])

        finally:
            for server in servers:
                server.stop()
            if monitor:
                monitor.stop()



    def _make_fake_hdd(self, name, size, dev='/dev/loop0'):
        os.system('sudo rm -rf /tmp/mnt_%s'%name)
        os.system('sudo rm -rf /tmp/%s'%name)
        os.system('dd if=/dev/zero of=/tmp/%s bs=1024 count=%s'%(name, size))
        os.system('sudo umount /tmp/mnt_%s'%name)
        os.system('sudo losetup -d %s'%dev)
        os.system('sudo losetup %s /tmp/%s'%(dev, name))
        os.system('sudo mkfs -t ext2 -m 1 -v %s'%dev)
        os.system('sudo mkdir /tmp/mnt_%s'%name)
        os.system('sudo mount -t ext2 %s /tmp/mnt_%s'%(dev, name))
        os.system('sudo chmod 777 /tmp/mnt_%s -R'%name)
        os.system('rm -rf /tmp/mnt_%s/*'%name)
        return '/tmp/mnt_%s'%name

    def _destroy_fake_hdd(self, name, dev='/dev/loop0'):
        ret = os.system('sudo umount /tmp/mnt_%s'%name)
        self.assertEqual(ret, 0, 'destroy_fake_hdd failed')
        ret = os.system('sudo losetup -d %s'%dev)
        self.assertEqual(ret, 0, 'destroy_fake_hdd failed')
        os.system('sudo rm /tmp/%s'%name)
        os.system('sudo rm -rf /tmp/mnt_%s'%name)



if __name__ == '__main__':
    unittest.main()

