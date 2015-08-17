import sys
import unittest
import time
from thread import get_ident
from concurrencytest import ConcurrentTestSuite, fork_for_tests
from ZeMemcacheClient import ZeMemcacheClient
from EpisodeObject import ZeEpisodeObject
from PatientObject import ZePatientObject
import config
import test_data

# import logging

# logger = logging.getLogger('memcacheapp')
# hdlr = logging.FileHandler('memcache_test_output.log')
# formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
# hdlr.setFormatter(formatter)
# logger.addHandler(hdlr) 
# logger.setLevel(logging.INFO)

caches = {}


class MemcacheDetails(object):
    
    def __init__(self):
        self.__cacheid = '%s_%f' % (id(self), time.time())
        self._settings = {
                #'servers': ['192.168.15.94:4094'],
                'servers': ['127.0.0.1:5551'],
                'debug': 0,
                'single_node': 1
                }
    
    def getSettings(self):
        """
        """
        return self._settings.copy()
    
    def ZCacheManager_getCache(self):
        key = (get_ident(), self.__cacheid)
        try:
            return caches[key]
        except KeyError:
            # logger.error("%s - Key not found, creating Connection object again" % (key, ))
            print "New Key Generation"
            cache = ZeMemcacheClient()
            settings = self.getSettings()
            cache.setupConnectionToMemcacheClient(settings)
            caches[key] = cache
            return cache

    def counter(self, func):
        @wraps(func)
        def tmp(*args, **kwargs):
            tmp.count += 1
            return func(*args, **kwargs)
        tmp.count = 0
        return tmp
        
    def setPatientObjectInCache(self, clm_idn, attrs):
        """
        """
        clm_obj = self.ZCacheManager_getCache().getPatientDetails(clm_idn)
        if not clm_obj:
            clm_obj = ZePatientObject(str(clm_idn))
        clm_obj.setAttributes(attrValueMapping=attrs)
        return self.ZCacheManager_getCache().setPatientDetails(clm_idn, clm_obj)

    def setEpisodeObjectInCache(self, enc_idn, attrs):
        """
        """

        enc_obj = self.ZCacheManager_getCache().getEpisodeDetails(enc_idn)
        if not enc_obj:
            enc_obj = ZeEpisodeObject(str(enc_idn))
        enc_obj.setAttributes(attrValueMapping=attrs)
        return self.ZCacheManager_getCache().setEpisodeDetails(enc_idn, enc_obj)
        
    #def getPatientObjectData(self, claimant_idn=0):
        #"""
        #Returns cached content for given claimant_idn as a dictionary

        #@param claimant_idn: claimant_idn (patient id whose data has to be returned
        #@return: Returns cached content for given claimant_idn as a dictionary
        #"""
        #pat_obj = self.ZCacheManager_getCache().getPatientDetails(claimant_idn)
        #if pat_obj:
            #return pat_obj.getAllAttributes()
    
    #def getEpisodeObjectData(self, encounter_idn=0):
        #"""
        #Returns cached content for given encounter_idn as a dictionary

        #@param encounter_idn: encounter_idn (episode id whose data has to be returned
        #@return: Returns cached content for given encounter_idn as a dictionary
        #"""
        #enc_obj = self.ZCacheManager_getCache().getEpisodeDetails(encounter_idn)
        #if enc_obj:
            #return enc_obj.getAllAttributes()
 
 
class MemcacheConcurrencyTestCase(unittest.TestCase):
    mem_obj = MemcacheDetails()
    def test_setEpisodeObjectInCache(self):
        for item in range(5):
            for key, val in test_data.episode_data.items():
                time.sleep(0.5)
                try:
                    self.assertEqual(self.mem_obj.setEpisodeObjectInCache(key, val), True)
                except AssertionError as e:
                    # logger.error("FAIL: Episode Object - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
                    pass
    
            for key, val in test_data.patient_data.items():
                time.sleep(0.5)
                try:
                    self.assertEqual(self.mem_obj.setPatientObjectInCache(key, val), True)
                except AssertionError as e:
                    # logger.error("FAIL: Patient Object - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
                    pass

    def test_check_set_object_cache_after_added_services(self):
        # mem_obj = MemcacheDetails()
        for item in range(5):
            for key, val in test_data.episode_data_after_add_services.items():
                time.sleep(0.5)
                try:
                    self.assertEqual(self.mem_obj.setEpisodeObjectInCache(key, val), True)
                except AssertionError as e:
                    # logger.error("FAIL: Add Service/Stay - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
                    pass

        ##for key, val in test_data.patient_data.items():
            ##self.assertEqual(mem_obj.setPatientObjectInCache(key, val), True)
            ##time.sleep(0.5)

    def test_check_set_object_cache_after_change_status(self):
        # mem_obj = MemcacheDetails()
        for item in range(5):
            for key, val in test_data.episode_data_after_change_status.items():
                time.sleep(0.5)
                try:
                    self.assertEqual(self.mem_obj.setEpisodeObjectInCache(key, val), True)
                except AssertionError as e:
                    # logger.error("FAIL: Change Status - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
                    pass

    def test_check_set_object_cache_after_add_provider(self):
        # mem_obj = MemcacheDetails()
        for item in range(5):
            for key, val in test_data.episode_data_after_change_status.items():
                time.sleep(0.5)
                try:
                    self.assertEqual(self.mem_obj.setEpisodeObjectInCache(key, val), True)
                except AssertionError as e:
                    # logger.error("FAIL: Add Provider - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
                    pass

    def test_check_set_object_cache_after_edit_episode_info(self):
        # mem_obj = MemcacheDetails()
        for item in range(5):
            for key, val in test_data.episode_data_after_edit_episode_info.items():
                time.sleep(0.5)
                try:
                    self.assertEqual(self.mem_obj.setEpisodeObjectInCache(key, val), True)
                except AssertionError as e:
                    # logger.error("FAIL: Edit Episode Info - Memcache SET operation failed \n Key: %s \n Value: %s \n" % (key, val))
                    pass

    #def test_check_set_object_cache_after_add_notes(self):
        ## mem_obj = MemcacheDetails()
        #for key, val in test_data.episode_data_after_add_notes.items():
            #time.sleep(0.5)
            #self.assertEqual(mem_obj.setEpisodeObjectInCache(key, val), True)

        #for key, val in test_data.patient_data.items():
            #self.assertEqual(mem_obj.setPatientObjectInCache(key, val), True)

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
            Keep this file in ZeCache folder in Products. Make sure MEMCACHE service is running.
            
            cmd: ./bin/zopepy concurrency_test.py number process
            
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
