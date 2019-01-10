# /usr/bin/env python3

import os
import sys
import time
import redis
import configparser

"""千信用户抓取"""

r: redis.Redis = None
POST_QX_ID_START = 0
POST_QX_ID_STOP = 9999999
POST_ID_KEY = 'post_qx_id'
QX_ID_SEQ = 'qx_id_seq'
cf: configparser.ConfigParser = None


def __init():
    """系统初始化"""
    global r, cf
    cf = configparser.ConfigParser()
    cf.read(sys.argv[1])
    r = redis.Redis(host=cf.get("redis", 'DB_HOST'), port=cf.get("redis", "DB_PORT"), db=cf.getint("redis", "DB_INDEX"),
                    password=cf.get("redis", 'DB_PASSWORD'), decode_responses=True)


def main():
    __init()
    for next_id in range(next_sequence(), POST_QX_ID_STOP + 1):
        list_size = int(r.llen(QX_ID_SEQ))
        if list_size > 100000:
            print("队列当前剩余 %d 条 ..." % list_size)
            time.sleep(1)
        elif list_size > 50000:
            print("队列当前剩余 %d 条 ..." % list_size)
            time.sleep(0.2)
        pipe = r.pipeline()
        pipe.rpush(QX_ID_SEQ, next_id)
        pipe.set(POST_ID_KEY, next_id)
        pipe.execute()
        if list_size > 50000:
            print("当前投递 %d, 速度放缓 ..." % next_id)
        else:
            print("当前投递 %d" % next_id)


def next_sequence():
    """获取下一个待投递的号码序列"""
    next_num = r.get(POST_ID_KEY)

    if next_num:
        print("last post id is %s \n continue ... \n\n" % next_num)
        return int(next_num) + 1
    else:
        return POST_QX_ID_START

    print('所有号码，均已经投递完成 .....')
    exit(0)


main()

