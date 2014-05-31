import unittest

from test_utils import *

from nimbus.client import Nimbus, NimbusError
from fabnet.core.key_storage import init_keystore


class TestNimbus(unittest.TestCase):
    SERVERS = []
    def test00_init_backend(self):
        server = TestServerThread(1770, '/tmp/nimbus_test_1770', ks_path=NODE1_KS)
        self.SERVERS.append(server)
        server.start()
        time.sleep(1)

        server = TestServerThread(1771, '/tmp/nimbus_test_1771', neighbour='127.0.0.1:1770', ks_path=NODE2_KS)
        self.SERVERS.append(server)
        server.start()
        time.sleep(.2)
        server.wait_oper_status(DS_NORMALWORK)

        server = TestServerThread(1772, '/tmp/nimbus_test_1772', neighbour='127.0.0.1:1770', ks_path=NODE1_KS)
        self.SERVERS.append(server)
        server.start()
        time.sleep(.2)
        server.wait_oper_status(DS_NORMALWORK)

        server = TestServerThread(1773, '/tmp/nimbus_test_1773', neighbour='127.0.0.1:1771', ks_path=NODE2_KS)
        self.SERVERS.append(server)
        server.start()
        time.sleep(.2)
        server.wait_oper_status(DS_NORMALWORK)

    def test99_destroy(self):
        for server in self.SERVERS:
            server.stop()


    def test01_basic(self):
        client_ks = init_keystore(USER1_KS, USER_PWD)
        nimbus = Nimbus(client_ks, '127.0.0.1:1771')

        data_block = 'test data'*1000*10
        key = nimbus.put_data_block(data_block)

        binary = nimbus.get_data_block(key)
        self.assertEqual(data_block, binary.data())

        nimbus.delete_data_block(key)
        with self.assertRaises(NimbusError):
            nimbus.get_data_block(key)
    

if __name__ == '__main__':
    unittest.main()

