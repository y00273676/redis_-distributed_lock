#!/usr/bin/env python
# -*- coding:utf8 -*-

from redis import ConnectionPool, StrictRedis
import datetime
import time



"""
redis 分布式锁
"""
class redis_op(object):
    """
        确保单例模式
    """
    def __new__(cls, *arg, **kw):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, redis_key):
        self.rc = StrictRedis(connection_pool=ConnectionPool(host="127.0.0.1", port=6379, db=0))
        self._lock = 0
        self._lock_key = f"{redis_key}_lock_key"

    def __enter__(self):
        while not self._lock:
            time_out = time.time() + 10 + 1
            self._lock = self.rc.setnx(self._lock_key, time_out)
            if self._lock or (time.time() > float(self.rc.get(self._lock_key)) and time.time() > float(self.rc.getset(self._lock_key, time_out))):
               return self
            else:
                time.sleep(0.3)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if time.time() < float(self.rc.get(self._lock_key)):
            self.rc.delete(self._lock_key)



def my_func():
    with redis_op("test") as rp:
        rp.rc.set("rc1", "11")

if __name__ == "__main__":
    my_func()
