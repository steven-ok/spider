# /usr/bin/env python3

import os
import sys
import time
import redis
import configparser

r: redis.Redis = None
POST_SEQ_KEY = 'phone_number_post'
CONSUME_SEQ_KEQ = 'phone_number_seq'
cf: configparser.ConfigParser = None


PHONE_INTERVAL = [130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 145, 146, 147, 148, 149, 150, 151, 152, 153, 155,
                  156, 157, 158, 159, 165, 166, 170, 171, 172, 173, 174, 175, 176, 177, 178, 180, 181, 182, 183, 184,
                  185, 186, 187, 188, 189, 191, 198, 199]


def __init():
    """系统初始化"""
    global r, cf
    cf = configparser.ConfigParser()
    cf.read(sys.argv[1])
    r = redis.Redis(host=cf.get("redis", 'DB_HOST'), port=cf.get("redis", "DB_PORT"), db=cf.getint("redis", "DB_INDEX"),
                    password=cf.get("redis", 'DB_PASSWORD'), decode_responses=True)


def main():
    __init()
    global PHONE_INTERVAL
    prefix, cur_number = next_sequence()
    post_seq = [i for i in PHONE_INTERVAL if i >= prefix]
    for i in post_seq:
        for next_number in range(cur_number, 100000000):
            while r.llen(CONSUME_SEQ_KEQ) >= 600000:
                sys.stdout.write('当前队列剩余 {0} 条 , 等待执行...\r'.format(r.llen(CONSUME_SEQ_KEQ)))
                sys.stdout.flush()
                time.sleep(3)

            number_str = str(i) + str(next_number).zfill(8)

            pipe = r.pipeline()
            if next_number == 0:
                pipe.hset(POST_SEQ_KEY, i, 0)
            else:
                pipe.hincrby(POST_SEQ_KEY, i, 1)

            pipe.rpush(CONSUME_SEQ_KEQ, number_str)
            pipe.execute()

            sys.stdout.write('当前投递序列 {0} \r'.format(number_str))
            sys.stdout.flush()

        else:
            cur_number = 0


def next_sequence():
    """获取下一个待投递的号码序列"""
    for prefix in PHONE_INTERVAL:
        fs = r.hget(POST_SEQ_KEY, prefix)
        if fs is None:
            return prefix, 0
        else:
            fs = int(fs)
            if fs < 99999999:
                return prefix, fs

    print('所有号码，均已经投递完成 .....')
    exit(0)


main()

