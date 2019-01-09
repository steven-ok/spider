#!/usr/bin/env python

import os
import sys
import redis
import signal
import requests
import configparser

r: redis.Redis = None
CONSUME_SEQ_KEY = 'phone_number_seq'
GET_PHONES_KEY = 'validate_phones'
cf: configparser.ConfigParser = None
process_number: str = None

REQUEST_TOKEN = 'bearer: eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJkc19tZXNzZW5nZXIiLCJleHAiOjE1NDkxNzk2Mjg'\
                'sImlhdCI6MTU0NjU4NzYyOCwiaXNzIjoiZHNfbWVzc2VuZ2VyIiwianRpIjoiNjE5ZmIwNzktYWEwYi00OWZmLTgxMGMtMDJ'\
                'kM2RjMmJhNjRlIiwibmJmIjoxNTQ2NTg3NjI3LCJyZXNvdXJjZV90eXBlIjoidXNlciIsInN1YiI6IjYyOTgiLCJ0eXAiOiJ'\
                'hY2Nlc3MifQ.uqjMfeV7NLJs54k-skCRSFkxt_MOMlmXX-odmow2XxWl6zenVbVB3Zgi0jn-SoUbrJSlVsW9BYGBwKSkAaCoeg'


def __init():
    """系统初始化"""
    global r, cf
    cf = configparser.ConfigParser()
    cf.read(sys.argv[1])
    r = redis.Redis(host=cf.get("redis", 'DB_HOST'), port=cf.get("redis", "DB_PORT"), db=cf.getint("redis", "DB_INDEX"),
                    password=cf.get("redis", 'DB_PASSWORD'), decode_responses=True)


def main():
    __init()
    while True:
        try:
            _, phone = r.blpop(CONSUME_SEQ_KEY, 20)
            phone = str(phone)
            request(phone)
        except TypeError:
            continue


def request(phone: str):
    global process_number
    process_number = phone
    try:
        res = requests.get('http://api.echat188.com:9000/users?mobile=%s' % phone,
                           headers={'User-Agent': 'okhttp/3.0.1', 'authorization': REQUEST_TOKEN, 'Platform': 'android',
                                    'AppVersion': '1.0.6'}, timeout=5)
    except BaseException:
            r.lpush(CONSUME_SEQ_KEY, phone)
            print('phone %s net work error' % phone)
    else:
        if res.status_code != 200:
            r.lpush(CONSUME_SEQ_KEY, phone)
        else:
            if res.json()['data']['users']:
                r.rpush(GET_PHONES_KEY, phone)
            else:
                print('phone %s is not found' % phone)
        res.close()


def halt(signum, frame):
    if process_number:
        r.lpush(CONSUME_SEQ_KEY, '%s' % process_number)
        print("sync phone_number [%s]" % process_number)
    print("Bye Bye ....")
    sys.exit()


signal.signal(signal.SIGINT, halt)
signal.signal(signal.SIGTERM, halt)
main()
