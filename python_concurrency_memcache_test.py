import os
import sys
import unittest
import time
import logging
import memcache
import test_data

from concurrencytest import ConcurrentTestSuite, fork_for_tests

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0

logger = logging.getLogger('memcacheapp')
cur_dir = os.path.dirname(os.path.realpath(__file__))
log_filename = os.path.join(cur_dir, 'concurrency_memcache_test_output.log')
hdlr = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

class MemcacheTestClient(object):
    """
    """
    def __init__(self):
        self.successCount = 0
        self.failureCount = 0
        self._connect()
        
    def _connect(self):
        """
        Establishing connection to memcache service
        """
        # instantiate
        config = ConfigParser()
    
        # parse existing file
        cur_dir = os.path.dirname(os.path.realpath(__file__))
        filename = os.path.join(cur_dir, 'memcache_config.ini')
        config.read(filename)
    
        # read values from a memcache and iterations section
        ip_addr = config.get('memcache', 'ipaddress')
        ip_port = config.get('memcache', 'port')
        debug_mode = config.getint('memcache', 'debug')
        #iter_count = config.getint('iterations', 'count')

        server_details = ip_addr + ":" + ip_port
        self.conn = memcache.Client([server_details], debug_mode)

    def setData(self, data_dict):
        """
        Writing data to Memcache
        """
        flag = False
        for key, val in data_dict.items():
            if self.conn.cas(str(key), val):
                logger.info("MEMCACHE UPDATE SUCCESS - For Key: %s \n Data: %s\n" % (key, val))
                self.successCount += 1
                flag = True
            else:
                logger.info("MEMCACHE UPDATE FAILURE - For Key: %s \n Data: %s\n" % (key, val))
                self.failureCount += 1
                flag = False

        return flag

    def getData(self, key):
        """
        Retrieving data from Memcache
        """
        return self.conn.gets(key) 


class MemcacheConcurrencyTestCase(unittest.TestCase):
    mem_obj = MemcacheTestClient()

    def test_setEDataInCache(self):
        try:
            self.assertEqual(self.mem_obj.setData(test_data.episode_data), True)
        except AssertionError as e:
            # logger.error("FAIL: Episode Object - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
            pass

    def test_setPDataInCache(self):
        try:
            self.assertEqual(self.mem_obj.setData(test_data.patient_data), True)
        except AssertionError as e:
            # logger.error("FAIL: Patient Object - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
            pass

    def test_temp(self):
        a = b = []
        for i in range(1000):
            a.append(i)
            b.append(i)
        data = dict(zip(a, b))
        try:
            self.assertEqual(self.mem_obj.setData(data), True)
        except AssertionError as e:
            # logger.error("FAIL: Patient Object - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
            pass


def main(args):
    """
    Implementation of main method starts here....
    """
    process = 0
    iter_num = 0
    if len(args) > 1:
        iter_num = int(args[0])
        process = int(args[1])
    else:
        iter_num = int(args[0])   

    # load a TestSuite with 50x TestCases for demo
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for _ in range(iter_num):
        suite.addTests(loader.loadTestsFromTestCase(MemcacheConcurrencyTestCase))
    print('Loaded %d test cases...' % suite.countTestCases())
 
    runner = unittest.TextTestRunner()
    
    # Run parallely with process and parallel hits at once
    if process:
        print('\nRun same tests with %s processes: ' % process)
        concurrent_suite = ConcurrentTestSuite(suite, fork_for_tests(process))
        runner.run(concurrent_suite)
    else:
        # Sequential run (one by one)
        print('\nRun tests sequentially:')
        runner.run(suite)

if __name__ == '__main__':
    """
    Concurrency test for set and get operations
    """
    if len(sys.argv) > 1:
        if sys.argv[1] in ('--help', '-h'):
            print """
            Memcache concurrency script
            
            cmd: python python_concurrency_memcache_test.py number process
            
            Params:
            number    : It will represents the number of iteration for set or get operations.
                        By Default 1.

            process : It will represents number of threads to run paralley. If number not provided
                        it will run sequential test.
                        By Default 0.
            """
        else:
            main(sys.argv[1:])
    else:
        print "\n\nPlease provide number of iterations and process to run\n\n \
        python concurrency_test.py number process \
        "
