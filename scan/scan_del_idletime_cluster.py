#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 集群维度将没有设置过期时间的某种前缀的数据进行处理

import sys
import time
import random
from rediscluster import RedisCluster


def main():
    argv = sys.argv
    ip = argv[1]
    port = argv[2]
    prefix = argv[3]
    idle = int(argv[4])
    print ip, port, prefix, idle
    startup_nodes = [{"host": ip, "port": port}]

    rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)

    for key in rc.scan_iter(prefix):
        idletime = rc.object('idletime', key)
        # 如果 idletime 大于需要设置的过期周期，就直接删除
        if idletime > idle:
            print key, idletime
            rc.delete(key)
        # 如果 idletime 小于需要设置的过期周期，就设置过期时间
        else:
            expire_time = idle - idletime
            rc.expire(key, expire_time+random.randint(0, 99))
            print key, idletime, rc.ttl(key)
        time.sleep(1)


if __name__ == '__main__':
    main()