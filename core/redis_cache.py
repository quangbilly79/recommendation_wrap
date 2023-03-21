from redis.sentinel import Sentinel, SentinelConnectionPool
from redis.connection import PythonParser
import redis

# Chứa các method custom được xây dựng dựa trên method có sẵn trong thư viện redis
# Vd như set, get, append, delete,...
class RedisCache(object):

    def __init__(self, ser_name, sen_configs, db=1):
        self.sentinel = Sentinel(sen_configs, socket_timeout=0.1)
        self.pool = SentinelConnectionPool(ser_name, self.sentinel, parser_class=PythonParser, db=db)

    def get_redis(self):
        return redis.StrictRedis(connection_pool=self.pool)

    def get(self, key, default=None):
        rd = self.get_redis()
        result = rd.get(key)
        if result:
            return result.decode('utf-8')
        else:
            return default

    def getlistkeybyprefix(self, prefix_key):
        rd = self.get_redis()
        return rd.scan_iter(prefix_key+":*")

    # expired = seconds
    def set(self, key, value, expires=None):
        rd = self.get_redis()
        rd.set(key, value, ex=expires)

    def append(self, key, value, expires=None):
        rd = self.get_redis()
        if not rd.exists(key):
            self.set(key, value, expires)
        else:
            value_exist = rd.get(key)
            s = set()
            if value_exist:
                s = s.union(set(value_exist.split(",")))
            if value:
                s = s.union(set(value.split(",")))
            self.set(key, ",".join(s), expires)

    def delete(self, key):
        rd = self.get_redis()
        rd.delete(key)

    def sadd(self, queue_name, value):
        rd = self.get_redis()
        rd.sadd(queue_name, value)

    def put_to_priority(self, queue_name, value, level_priority):
        rd = self.get_redis()
        rd.zincrby(queue_name, value, level_priority)
