
import os
import sys
import hashlib
import unittest
import threading
import random
from datetime import datetime
sys.path.append('fabnet_core')

from fabnet_dht.user_metadata import *

TEST_MD_PATH = '/tmp/ut_dht_user_metadata'


class TestThread(threading.Thread):
    CNT = 50
    ERRORS = []
    md_cache = MetadataCache()

    def run(self):
        MAP = {}
        for i in xrange(self.CNT):
            dblen = random.randint(10, 10000000) 
            MAP[i] = dblen
            self.md_cache.call(TEST_MD_PATH, 'update_path', '/test/test_file_%s'%i, [MDDataBlockInfo(hashlib.sha1(str(dblen)).hexdigest(), 2, dblen)])

        for i in xrange(self.CNT):

            dblen = MAP[i] 
            dbs = self.md_cache.call(TEST_MD_PATH, 'get_data_blocks', '/test/test_file_%s'%i)
            for db in dbs:
                if db.db_key == hashlib.sha1(str(dblen)).hexdigest() and db.size == dblen:
                    break
            else:
                self.ERRORS.append('/test/test_file_%s'%i)



class TestUserMetadata(unittest.TestCase):
    def test00_func(self):
        if os.path.exists(TEST_MD_PATH):
            os.system('rm -rf %s'%TEST_MD_PATH)
        
        um = UserMetadata(TEST_MD_PATH)
        try:
            user_info = um.get_user_info()
            self.assertEqual(user_info.storage_size, 0)
            self.assertEqual(user_info.used_size, 0)
            self.assertEqual(user_info.flags, 0)

            user_id_hash = hashlib.sha1('fabregas').hexdigest()
            um.update_user_info(UserInfo(user_id_hash, 24214124312123, 0, 522))
            um.close()

            um = UserMetadata(TEST_MD_PATH)
            user_info = um.get_user_info()
            self.assertEqual(user_info.user_id_hash, user_id_hash)
            self.assertEqual(user_info.storage_size, 24214124312123)
            self.assertEqual(user_info.used_size, 0)
            self.assertEqual(user_info.flags, 522)

            with self.assertRaises(MDNotFound):
                um.listdir('/test')
            items = um.listdir('/')
            self.assertEqual(items, [])

            um.make_path('/test')
            items = um.listdir('/')
            self.assertEqual(items, ['test'])

            with self.assertRaises(MDAlreadyExists):
                um.make_path('/test')

            um.update_path('/test/test_file.out', [])
            with self.assertRaises(MDNotFound):
                um.get_path_info('/some/path')

            p_info = um.get_path_info('/test/test_file.out')
            self.assertEqual(p_info.name, '/test/test_file.out')
            self.assertEqual(p_info.size, 0)
            self.assertEqual(p_info.path_type, 'file')
            self.assertEqual(p_info.children, [])
            user_info = um.get_user_info()
            self.assertEqual(user_info.used_size, 0)

            um.update_path('/test/test_file.out', [])
            um.update_path('/test/test_file.out', [MDDataBlockInfo(hashlib.sha1('2rf3ef3ef').hexdigest(), 2, 23422)])
            um.update_path('/test/test_file.out', [MDDataBlockInfo(hashlib.sha1('23ffef').hexdigest(), 2, 1003428)])
            um.update_path('/test/test_file_2.out', [MDDataBlockInfo(hashlib.sha1('sssssssssssef').hexdigest(), 3, 100500)])
            p_info = um.get_path_info('/test/test_file.out')
            self.assertEqual(p_info.size, 1003428+23422)

            p_info = um.get_path_info('/test/')
            self.assertEqual(p_info.size, 1003428+23422+100500)
            self.assertEqual(p_info.path_type, 'dir')
            self.assertEqual(len(p_info.children), 2)
            self.assertEqual(p_info.children[1].name, 'test_file_2.out')
            self.assertEqual(p_info.children[1].size, 100500)

            user_info = um.get_user_info()
            self.assertEqual(user_info.used_size, (1003428+23422)*3+100500*4)

            with self.assertRaises(MDException):
                um.get_data_blocks('/test')
            with self.assertRaises(MDNotFound):
                um.get_data_blocks('/test/ddd')
            dbs = um.get_data_blocks('/test/test_file_2.out')
            self.assertEqual(len(dbs), 1)
            self.assertEqual(dbs[0].db_key, hashlib.sha1('sssssssssssef').hexdigest())

            lst = um.listdir('/test')
            self.assertEqual(lst, ['test_file.out', 'test_file_2.out'])

            for key, value in um.iterdir('/'):
                self.assertEqual(value.name, 'test')

            with self.assertRaises(MDException):
                um.remove_path('/test')

            um.remove_path('/test/test_file.out')
            user_info = um.get_user_info()
            self.assertEqual(user_info.used_size, 100500*4)

            with self.assertRaises(MDNotFound):
                um.remove_path('/test/test_file.out')

            um.remove_path('/test/test_file_2.out')
            user_info = um.get_user_info()
            self.assertEqual(user_info.used_size, 0)
            um.remove_path('/test/')
            with self.assertRaises(MDException):
                um.remove_path('/')
            user_info = um.get_user_info()
        finally:
            um.close()

    def test01_load(self):
        um = UserMetadata(TEST_MD_PATH)
        try:
            um.make_path('/test')
        finally:
            um.close()

        thrds = []
        for i in xrange(5):
            thrds.append(TestThread())

        for thrd in thrds:
            thrd.start()

        for thrd in thrds:
            thrd.join()

        TestThread.md_cache.destroy()
        self.assertEqual(TestThread.ERRORS, [])

    def test03_free_space(self):
        um = UserMetadata(TEST_MD_PATH)
        try:
            user_id_hash = hashlib.sha1('fabregas').hexdigest()
            um.update_user_info(UserInfo(user_id_hash, 1024, 0, 522))

            um.update_path('/test_file.out', [MDDataBlockInfo(hashlib.sha1('2rf3ef3ef').hexdigest(), 2, 333)])
            with self.assertRaises(MDNoFreeSpace):
                um.update_path('/test_file.out', [MDDataBlockInfo(hashlib.sha1('asdffff3ef3ef').hexdigest(), 1, 33)])

            um.add_user_storage_size(1000)
            user_info = um.get_user_info()
            self.assertEqual(user_info.user_id_hash, user_id_hash)
            self.assertEqual(user_info.used_size, 333*3)
            self.assertEqual(user_info.storage_size, 2024)
            um.update_path('/test_file.out', [MDDataBlockInfo(hashlib.sha1('asdffff3ef3ef').hexdigest(), 1, 33)])
        finally:
            um.close()

if __name__ == '__main__':
    unittest.main()

