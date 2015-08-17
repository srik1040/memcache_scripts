import os
import logging
import memcache
import test_data

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0

logger = logging.getLogger('memcacheapp')
cur_dir = os.path.dirname(os.path.realpath(__file__))
log_filename = os.path.join(cur_dir, 'memcache_test_output.log')
hdlr = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)


class MemcacheTestClient(object):
    """
    """
    def __init__(self, ipAddress='127.0.0.1', port='11211', debug=0):
        self.successCount = 0
        self.failureCount = 0
        self.server = ipAddress + ":" + port
        self.debug_mode = debug
        #self.conn = memcache.Client([self.server], self.debug_mode)
        self._connect()
        
    def _connect(self):
        """
        Establishing connection to memcache service
        """
        self.conn = memcache.Client([self.server], self.debug_mode)

    def setData(self, data_dict):
        """
        Writing data to Memcache
        """
        for key, val in data_dict.items():
            if self.conn.cas(str(key), val):
                logger.info("MEMCACHE UPDATE SUCCESS - For Key: %s \n Data: %s\n" % (key, val))
                self.successCount += 1
            else:
                logger.info("MEMCACHE UPDATE FAILURE - For Key: %s \n Data: %s\n" % (key, val))
                self.failureCount += 1

    def getData(self, key):
        """
        Retrieving data from Memcache
        """
        return self.conn.gets(key)

def main():
    """
    Implementation of main method starts here....
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
    mode = config.getint('memcache', 'debug')
    iter_count = config.getint('iterations', 'count')

    print "Memcache test started...........\n"
    
    # Instantiating class of MemcacheTestClient
    memObj = MemcacheTestClient(ip_addr, ip_port)
    
    print "Setting episode specific data into cache...........\n"
    # Setting Episode specific data into cache
    for index in range(iter_count):
        memObj.setData(test_data.episode_data)
    print "First Success Count: ", memObj.successCount
    
    print "Setting member specific data into cache...........\n"
    # Setting Member specific data into cache
    for index in range(iter_count):
        memObj.setData(test_data.patient_data)
    print "Second Success Count: ", memObj.successCount

    print "Memcache test completed...........\n\n\n"

    #print "Total Count: %s \n", (memObj.successCount + memObj.failureCount)
    print "Total Success: %s \n", (memObj.successCount)
    print "Total Failure: %s \n", (memObj.failureCount)

if __name__ == '__main__':
    """
    Memcache test with static data
    """
    main()
