# -*- coding: utf-8 -*-
"""
Created on 2018/4/2

@author: qiusy

@file: generate_id.py
"""

import logging
import uuid
import time
import os

ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz'
# Tue, 21 Mar 2006 20:50:14.000 GMT
#TWEPOCH = 1142974214000L

# 2001/11/05 00:00:00 CST
TWEPOCH = 1004889600000L

log = logging.getLogger(__name__)


class SnowFlakeId(object):
    def __init__(self, snowflake_id):
        if isinstance(snowflake_id, int) or isinstance(snowflake_id, long):
            self.snowflake_id = snowflake_id
        else:
            raise TypeError("SnowFlakeId must init by a integer.")

    @property
    def timestamp(self):
        timestamp = self.snowflake_id >> 22
        timestamp += TWEPOCH
        return timestamp

    @property
    def mac_addr(self):
        mac_addr = self.snowflake_id >> 12
        mac_addr = mac_addr & 0x3ff
        return mac_addr

    @property
    def pid(self):
        pid = self.snowflake_id >> 8
        pid = pid & 0xf
        return pid

    @property
    def sequence(self):
        sequence = self.snowflake_id & 0xff
        return sequence

    def base62id(self):
        id_list = []
        snowflake_id = self.snowflake_id
        while snowflake_id > 0:
            snowflake_id, rem = divmod(snowflake_id, 62)
            id_list.append((ALPHABET[rem]))

        if len(id_list) < 11:
            id_list.append(ALPHABET[0])

        id_str = "".join(reversed(id_list))

        return id_str

    @classmethod
    def decode_base62id(cls, base62_id):
        snowflake_id = 0
        for x in base62_id:
            snowflake_id = snowflake_id * 62 + ALPHABET.index(x)

        return SnowFlakeId(snowflake_id)


    def __str__(self):
        return "SnowFlakeId('{0}')".format(self.snowflake_id)


class SnowFlakeGenerator(object):
    sequence = 0
    last_timestamp = -1

    def __init__(self):
        pass

    def generate_id(self):
        pid = os.getpid()
        mac_addr = uuid.getnode()
        timestamp = 0
        sequence = 0
        retry = 3
        while retry:
            timestamp = long(time.time() * 1000)
            sequence = SnowFlakeGenerator.sequence
            SnowFlakeGenerator.sequence = (SnowFlakeGenerator.sequence + 1) % 256

            if SnowFlakeGenerator.last_timestamp > timestamp:
                log.warn("clock is moving backwards. waiting until %i" % SnowFlakeGenerator.last_timestamp)
                time.sleep(SnowFlakeGenerator.last_timestamp - timestamp)
                retry -= 1
                continue
            elif SnowFlakeGenerator.last_timestamp == timestamp:
                if SnowFlakeGenerator.sequence == 0:
                    log.warn("SnowFlakeGenerator sequence is over run.")
                    time.sleep(0.001)
                    retry -=1
                    continue

            break

        id_value = (((timestamp - TWEPOCH) << 22 ) |
                ((mac_addr & 0x3ff) << 12) |
                ((pid & 0xf) << 8) |
                sequence)

        # log.debug("%d, %d, %d, %d", timestamp, mac_addr, pid, sequence)
        log.debug("%d, %d, %d, %d", timestamp - TWEPOCH, mac_addr & 0x3ff, pid & 0xf, sequence)

        return SnowFlakeId(id_value)


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s][%(levelname)s][%(name)s][%(process)d][%(threadName)s] %(filename)s:%(lineno)d: %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='generator.log',
                        filemode='a+'
                        )

    def multi_process_generate(process_num, range_num):
        for x in range(process_num):
            pid = os.fork()
            if pid == 0:
                id_generator = SnowFlakeGenerator()
                for i in xrange(range_num):
                    test_id = id_generator.generate_id()
                    log.info("{} {}".format(test_id,test_id.base62id()))
                break
            else:
                print 'I (%s) just created a child process (%s).' % (os.getpid(), pid)

    import threading

    class MyThread(threading.Thread):
        def __init__(self, range_num):
            if not isinstance(range_num, int):
                raise TypeError("range_num must be int")
            self.range_num = range_num
            super(MyThread, self).__init__()
        def run(self):
            print "running {}".format(self.name)
            id_generator = SnowFlakeGenerator()
            for i in xrange(self.range_num):
                test_id = id_generator.generate_id()
                log.info("{} {}".format(test_id,test_id.base62id()))

    def multi_thread_generate(thread_num, range_num):
        threads = [ MyThread(range_num) for x in range(thread_num)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

    lock = threading.RLock()

    class SafeMyThread(threading.Thread):
        def __init__(self, range_num):
            if not isinstance(range_num, int):
                raise TypeError("range_num must be int")
            self.range_num = range_num
            super(SafeMyThread, self).__init__()
        def run(self):
            print "running {}".format(self.name)
            id_generator = SnowFlakeGenerator()
            for i in xrange(self.range_num):
                if lock.acquire():
                    test_id = id_generator.generate_id()
                lock.release()
                log.info("{} {}".format(test_id,test_id.base62id()))

    def safe_multi_thread_generate(thread_num, range_num):
        threads = [ SafeMyThread(range_num) for x in range(thread_num)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

    #multi_process_generate(8, 10000)
    multi_process_generate(1, 100000)
    multi_thread_generate(8, 100000)
    #safe_multi_thread_generate(8, 10000)

