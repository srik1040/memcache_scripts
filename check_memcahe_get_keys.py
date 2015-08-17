import re, telnetlib, sys

class MemcachedStats:

    _client = None
    #_key_regex = re.compile(ur'ITEM (EPISODE_NS_.*) \[(.*); (.*)\]')
    _slab_regex = re.compile(ur'STAT items:(.*):number')
    _stat_regex = re.compile(ur"STAT (.*) (.*)\r")

    def __init__(self, host='127.0.0.1', port='5577',cache_type='None'):
        self._host = host
        self._port = port
        if cache_type == 'Episode':
            self._key_regex = re.compile(ur'ITEM (EPISODE_NS_.*) \[(.*); (.*)\]')
        elif cache_type == 'Patient':
            self._key_regex = re.compile(ur'ITEM (PATIENT_NS_.*) \[(.*); (.*)\]')
        elif cache_type == 'User':
            self._key_regex = re.compile(ur'ITEM (USER_.*) \[(.*); (.*)\]')
        elif cache_type == 'None':
            self._key_regex = re.compile(ur'ITEM (.*) \[(.*); (.*)\]')
    @property
    def client(self):
        if self._client is None:
            self._client = telnetlib.Telnet(self._host, self._port)
        return self._client

    def command(self, cmd):
        ' Write a command to telnet and return the response '
        self.client.write("%s\n" % cmd)
        return self.client.read_until('END')

    def key_details(self, sort=True, limit=100):
        ' Return a list of tuples containing keys and details '
        cmd = 'stats cachedump %s %s'
        keys = [key for id in self.slab_ids()
            for key in self._key_regex.findall(self.command(cmd % (id, limit)))]
        if sort:
            return sorted(keys)
        else:
            return keys

    def keys(self, sort=True, limit=100):
        ' Return a list of keys in use '
        return [key[0] for key in self.key_details(sort=sort, limit=limit)]

    def slab_ids(self):
        ' Return a list of slab ids in use '
        return self._slab_regex.findall(self.command('stats items'))

    def stats(self):
        ' Return a dict containing memcached stats '
        return dict(self._stat_regex.findall(self.command('stats')))

def main(argv=None):
    if not argv:
        argv = sys.argv
    host = argv[1] if len(argv) >= 2 else '127.0.0.1'
    port = argv[2] if len(argv) >= 3 else '5577'
    cache_type = argv[3] if len(argv) >= 4 and argv[3] in ['Episode','Patient','User'] else 'None'
    import pprint
    m = MemcachedStats(host, port,cache_type)
    pprint.pprint(m.keys())

if __name__ == '__main__':
    main()
