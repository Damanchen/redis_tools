#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 对单实例上没有设置过期时间的某种前缀的数据进行处理

import sys
import time
import random
import redis


def main():
    argv = sys.argv
    prefix = argv[1]
    idle = int(argv[2])
    ip = argv[3]
    port = argv[4]
    print ip, port, prefix, idle

    rec = redis.StrictRedis(host=ip, port=port, db=0)

    cursor = 0
    while 1:
        key = rec.scan(cursor, match=prefix, count=1024)
        cursor = key[0]
        if cursor == 0:
            break
        for n in key[1]:
            idletime = rec.object('idletime', n)
            # 如果 idletime 大于需要设置的过期周期，就直接删除
            if idletime > idle:
                rec.delete(n)
                print n, idletime
            # 如果 idletime 小于需要设置的过期周期，就设置过期时间
            else:
                expire_time = int(idle - idletime + random.randint(0, 99))
                rec.expire(n, expire_time)
                print n, idletime, rec.ttl(n)


if __name__ == '__main__':
    main()