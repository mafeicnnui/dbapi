#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : ma.fei
# @File : analysis_slowlog_detail_ecs.py.py
# @Software: PyCharm

import json
import re
import hashlib
import redis
import traceback
import warnings
import sys
import requests
import os
import datetime

def get_hour():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%H')
    return time1_str

def get_min():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%M')
    return time1_str

def get_day():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_time():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%Y-%m-%d %H:%M:%S')
    return time1_str

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(30,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_config():
    cfg = {
        'ip':'r-2ze9f53dad8419b4.redis.rds.aliyuncs.com',
        'port':6379,
        'password':'WXwk2018'
    }
    return cfg

def get_redis(ip,port,password):
    try:
        conn = redis.StrictRedis(host=ip,port=int(port),password=password,db=1)
        return conn
    except  Exception as e:
        print('get_db exceptiion:' + traceback.format_exc())
        return None

def get_slow_log(db):
    v = db.slowlog_get()
    print('slow log....\n')
    for i in v:
       print(i)


if __name__ == "__main__":
    config = ""
    mode   = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        else:
            pass

    db = get_redis(**get_config())
    print('db=',db)
    get_slow_log(db)