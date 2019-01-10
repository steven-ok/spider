#!/usr/bin/env python

import sys
import json
import redis
import time
import signal
import requests
import configparser

r: redis.Redis = None
QX_ID_SEQ = 'qx_id_seq'
GET_USERS_KEY = 'qx_validate_users'
cf: configparser.ConfigParser = None
process_number: str = None

REQUEST_TOKEN = """Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiIxMDIyNzY2MSIsImFjY2lkIjoiMTY4NTYwNzIiLCJuaWNr\
IjoiMTExIiwiYWNjVG9rZW4iOiJiNGJkZTNlZWNmOGFjZjc5YmRlYmY2MjZkY2YzNjcxNSIsIm5iZiI6MTU0NjU4NTUxOCwiZXhwIjoxNTQ5MDA0NzE4LCJ\
pc3MiOiJ0aGUgbmFtZSBvZiB0aGUgaXNzdWVyIiwiYXVkIjoidGhlIG5hbWUgb2YgdGhlIGF1ZGllbmNlIn0.D8fbdlNuKMlovDbvjpOD2Sy0Ned7wgCbRM\
5h5vnlCnQ"""


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
            _, id = r.blpop(QX_ID_SEQ, 20)
            request(str(id))
            time.sleep(0.2)
        except TypeError:
            continue


def request(qxid: str):
    global process_number
    process_number = qxid
    try:
        res = requests.post('http://66.liuliuda668.com:8080/user/search',
                           headers={'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 8.0.0; MIX 2 MIUI/9.1.2)',
                                    'Authorization': REQUEST_TOKEN}, data={"qxid": "qx" + qxid}, timeout=3)

    except requests.exceptions.BaseHTTPError:
        r.lpush(QX_ID_SEQ, qxid)
        print('qxid %s net work error' % qxid)
    except requests.exceptions.ConnectTimeout:
        r.lpush(QX_ID_SEQ, qxid)
        print('qxid %s net work error' % qxid)
    except requests.exceptions.ReadTimeout:
        r.lpush(QX_ID_SEQ, qxid)
        print('qxid %s net work error' % qxid)
    else:
        if res.status_code != 200:
            print('server response code %d ' % res.status_code)
            r.lpush(QX_ID_SEQ, qxid)
        else:
            if res.json()['data']['accid']:
                print(res.json()['data'])
                r.rpush(GET_USERS_KEY, json.dumps(res.json()['data']))
            else:
                print('qxid %s is not found' % qxid)
        res.close()


def halt(signum, frame):
    if process_number:
        r.lpush(QX_ID_SEQ, '%s' % process_number)
        print("sync qx_id [%s]" % process_number)
    print("Bye Bye ....")
    sys.exit()


signal.signal(signal.SIGINT, halt)
signal.signal(signal.SIGTERM, halt)
main()
