from core.redis_cache import RedisCache
from config import config
redis_cache = RedisCache(config.REDIS_SERVICE, config.SENTINEL_CONFIGS, db=5)
import logging
import pymysqlpool

logger = logging.getLogger()
logger.setLevel("INFO")
logger.handlers[0].setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s -%(filename)s:%(funcName)s:%(lineno)d\t%(message)s'
))

if __name__ == '__main__':
    res = redis_cache.get("wakarec:c2bb3723c7c24d00037c4255cedd09bc")
    print(res)
    logging.info("asdfadf: %s", res)

