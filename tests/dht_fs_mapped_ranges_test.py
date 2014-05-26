import unittest
import threading
import time
import os
import logging
import json
import shutil
import sys
from datetime import datetime

sys.path.append('fabnet_core')

from fabnet.utils.logger import logger
from fabnet.core.config import Config
from fabnet_dht.fs_mapped_ranges import *
from fabnet_dht.constants import *
Config.update_config({'WAIT_FILE_MD_TIMEDELTA': 0.1}, 'DHT')

logger.setLevel(logging.DEBUG)

TEST_FS_RANGE_DIR = None
TEST_FS_RANGE_DIR_NAME = 'test_fs_ranges'
START_RANGE_HASH = '%040x'%1000
END_RANGE_HASH = '%040x'%10000000000


def tmpdata(data, f_end=''):
    fname = '/tmp/tmpdata' + f_end
    tmp = TmpFile(fname, data)
    return fname, tmp


class WriteThread(threading.Thread):
    def __init__(self, fs_ranges, cnt, data):
        threading.Thread.__init__(self)
        self.fs_ranges = fs_ranges
        self.cnt = cnt
        self.data = data

    def run(self):
        for i in range(self.cnt):
            for i in range(100):
                path = self.fs_ranges.get_db_path('%040x'%((i+1000)*10000), FSMappedDHTRange.DBCT_MASTER)
                with ThreadSafeDataBlock(path) as db:
                    db.write(self.data)

class ReadThread(threading.Thread):
    def __init__(self, fs_ranges):
        threading.Thread.__init__(self)
        self.fs_ranges = fs_ranges
        self.stop_flag = False
        self.error = False

    def run(self):
        while True:
            for i in range(100):
                try:
                    if self.stop_flag:
                        return
                    path = self.fs_ranges.get_db_path('%040x'%((i+1000)*10000), FSMappedDHTRange.DBCT_MASTER, for_write=False)
                    db = ThreadSafeDataBlock(path)
                    if not db.exists():
                        continue

                    try:
                        data = db.read()
                        if len(data) % 20:
                            self.error = True
                            raise Exception('data size = %s'%data)
                    finally:
                        db.close()
                except Exception, err:
                    print 'GET DATA EXCEPTION: %s'%err


class TestFSMappedRanges(unittest.TestCase):
    def test00_init(self):
        global TEST_FS_RANGE_DIR
        TEST_FS_RANGE_DIR = self._make_fake_hdd(TEST_FS_RANGE_DIR_NAME, 70*1024)

    def test99_destroy(self):
        self._destroy_fake_hdd(TEST_FS_RANGE_DIR_NAME)

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
        ret = os.system('sudo umount /tmp/mnt_%s'%name)
        os.system('sudo rm /tmp/%s'%name)
        os.system('sudo rm -rf /tmp/mnt_%s'%name)

    def test01_discovery_ranges(self):
        fs_range = FSMappedDHTRange.discovery_range(TEST_FS_RANGE_DIR)
        self.assertEqual(fs_range.get_start(), MIN_KEY)
        self.assertEqual(fs_range.get_end(), MAX_KEY)
        self.assertEqual(fs_range.length(), MAX_KEY+1)
        self.assertTrue(fs_range.is_max_range())

        fs_range = FSMappedDHTRange(START_RANGE_HASH, END_RANGE_HASH, TEST_FS_RANGE_DIR)
        fs_range.save_range()
        fs_range.split_range(1000, 100500)
        self.assertTrue(not fs_range.is_max_range())

        discovered_range = FSMappedDHTRange.discovery_range(TEST_FS_RANGE_DIR)
        self.assertEqual(discovered_range.get_start(), long(START_RANGE_HASH, 16))
        self.assertEqual(discovered_range.get_end(), long(END_RANGE_HASH, 16))

    def test02_main(self):
        fs_ranges = FSMappedDHTRange(START_RANGE_HASH, END_RANGE_HASH, TEST_FS_RANGE_DIR)

        self.assertTrue(os.path.exists(os.path.join(TEST_FS_RANGE_DIR, 'range_info')))
        self.assertTrue(os.path.exists(os.path.join(TEST_FS_RANGE_DIR, 'mdb')))

        wd_data = 'WriteThread0000DATA\n'*100
        wd1_data = 'WriteThread1111DATA\n'*100
        wd2_data = 'WriteThread2222DATA\n'*100
        wt = WriteThread(fs_ranges, 100, wd_data)
        wt.start()
        wt1 = WriteThread(fs_ranges, 100, wd1_data)
        wt1.start()
        wt2 = WriteThread(fs_ranges, 100,  wd2_data)
        wt2.start()

        
        with self.assertRaises(FSHashRangesException):
            ret_range, new_range = fs_ranges.split_range('%040x'%0, '%040x'%10500000)
        ret_range, new_range = fs_ranges.split_range('%040x'%1000, '%040x'%10500000)
        ranges = fs_ranges.get_subranges()
        self.assertEqual(ranges, ( ret_range, new_range ))

        rt = ReadThread(fs_ranges)
        rt.start()

        wt.join()
        wt1.join()
        wt2.join()
        rt.stop_flag = True
        rt.join()
        self.assertEqual(rt.error, False)
    
        #check sizes
        size = ret_range.get_data_size()
        self.assertTrue(size > 0, size)
        r_size = size
        print 'ret range size = %s'%size

        size = new_range.get_data_size()
        self.assertTrue(size > 0, size)
        r_size += size
        print 'new range size = %s'%size

        #iterate subranges
        start = long(START_RANGE_HASH, 16)
        end = 10500000
        for key, dbct, path in ret_range.iterator(FSMappedDHTRange.DBCT_MASTER):
            self.assertTrue(start <= long(key, 16) <= end) 

        for key, dbct, path in ret_range.iterator(FSMappedDHTRange.DBCT_MASTER, foreign_only=True):
            self.assertTrue((long(key, 16) > end) or (long(key, 16) < start)) 

        local = foreign = False
        for key, dbct, path in ret_range.iterator(FSMappedDHTRange.DBCT_MASTER, all_data=True):
            if start <= long(key, 16) <= end:
                local = True
            else:
                foreign = True
        self.assertTrue(local)
        self.assertTrue(foreign)

        start = 10500000
        end = long(END_RANGE_HASH, 16)
        for key, dbct, path in new_range.iterator(FSMappedDHTRange.DBCT_MASTER):
            self.assertTrue(start <= long(key, 16) <= end) 

        fs_ranges.join_subranges()
        data = fs_ranges.get_subranges()
        self.assertEqual(data, None)

        size = fs_ranges.get_data_size()
        self.assertTrue(size > 0)
        self.assertEqual(r_size, size)

        #check data blocks
        def check_db(path):
            with DataBlock(path) as db:
                self.assertTrue(db.exists())
                for i, data in enumerate(db.read(len(wd_data), iterate=True)):
                    self.assertEqual(len(data), len(wd_data))
                    self.assertTrue(data in [wd_data, wd1_data, wd2_data], data)
                self.assertEqual(i, 299, db.read())
            
        for key, dbct, path in fs_ranges.iterator(FSMappedDHTRange.DBCT_MASTER):
            check_db(path)

        size = fs_ranges.get_data_size()
        self.assertEqual(size, r_size)

        free_size = fs_ranges.get_free_size()
        self.assertTrue(free_size > 0)
        print 'free size: %s'%free_size

        free_size_perc = fs_ranges.get_free_size_percents()
        self.assertTrue(free_size_perc > 0)
        self.assertTrue(free_size_perc < 100)
        
        est_perc = fs_ranges.get_estimated_data_percents(102400)
        c_est_perc = fs_ranges.get_estimated_data_percents(0)
        self.assertTrue(est_perc > 0)
        self.assertTrue(c_est_perc < est_perc)

        #block for write
        fs_ranges.block_for_write(15)

        with self.assertRaises(FSHashRangesException):
            path = fs_ranges.get_db_path('testfile', FSMappedDHTRange.DBCT_TEMP)

        for key, dbct, path in fs_ranges.iterator(FSMappedDHTRange.DBCT_MASTER):
            print '[free=%.2f%%] removing %s'%(fs_ranges.get_free_size_percents(), key)
            fs_ranges.remove_db(key, FSMappedDHTRange.DBCT_MASTER)
            try:
                path = fs_ranges.get_db_path('%040x'%10700, FSMappedDHTRange.DBCT_REPLICA)
                with DataBlock(path) as db:
                    db.write('test data')
                break
            except FSHashRangesException:
                pass

        tmp_data = False
        for key, dbct, path in fs_ranges.iterator():
            if dbct == FSMappedDHTRange.DBCT_REPLICA and key == '%040x'%10700:
                tmp_data = True
        self.assertTrue(tmp_data)

        free_size_perc = fs_ranges.get_free_size_percents()
        self.assertTrue(free_size_perc >= 15, free_size_perc)

        #extend
        with self.assertRaises(FSHashRangesException):
            fs_ranges.extend('%040x'%0, '%040x'%100)

        with self.assertRaises(FSHashRangesException):
            fs_ranges.extend('%040x'%0, '%040x'%100000)
        
        extended_range = fs_ranges.extend('%040x'%0, '%040x'%(999))
        extended_range.save_range()
        self.assertEqual(extended_range.get_start(), 0)
        self.assertEqual(extended_range.get_end(), long(END_RANGE_HASH, 16))
        last = extended_range.get_last_range()
        self.assertEqual(last.get_start(), long(START_RANGE_HASH, 16))
        self.assertEqual(last.get_end(), long(END_RANGE_HASH, 16))



if __name__ == '__main__':
    unittest.main()

