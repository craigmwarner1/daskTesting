from os import environ as e
import redis

class RedisHandler():

    def  __init__(self, host):
        # host = e['REDIS_HOST'] if 'REDIS_HOST' in e else 'localhost'
        # port = e['REDIS_PORT'] if 'REDIS_PORT' in e else '6379'
        # db = e['REDIS_DBID'] if 'REDIS_DB' in e else 'TEST_DB'

        self.connection = redis.Redis(host)

    def get(self, key):
        return self.connection.get(key)

    def set(self, key, value):
        if self.connection.set(key, value):
            return True
        else:
            print('Failed to set %s to %s.' % (key, value))

    def exists(self, key):
        return self.connection.exists(key)

    def keys(self):
        return self.connection.keys()
