#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/12/13 14:03
# @Author : ma.fei
# @File : schedule_clear_sync_log.py
# @Software: PyCharm

import sys
import time
import traceback
import requests
import pymysql
from clickhouse_driver import Client
import warnings

def clear_real_sync_log(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   cr.execute("truncate table`t_db_sync_log`")

def get_real_sync_log_num(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   cr.execute("SELECT COUNT(0) as num FROM `t_db_sync_log` WHERE STATUS='0'")
   rs = cr.fetchone()
   return rs['num']

def set_real_sync_status(cfg,p_status):
    try:
        par = {'status': p_status}
        url = 'http://{}/set_mysql_real_sync_status'.format(cfg['api_server'])
        res = requests.post(url, data=par,timeout=3).json()
        return res
    except:
         traceback.print_exc()
         sys.exit(0)

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://124.127.103.190:21080/read_db_decrypt'
        res = requests.post(url, data=par,timeout=1).json()
        if res['code'] == 200:
            config = res['msg']
            return config
        else:
            print('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        print('aes_decrypt api not available!')

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn

def get_config_from_db(tag):
    url = 'http://124.127.103.190:21080/read_config_sync'
    res = requests.post(url, data= { 'tag': tag},timeout=1).json()
    if res['code'] == 200:
        config  = res['msg']
        del config['ds_ro']
        del config['sync_table']
        del config['cols']

        if config.get('ds_log') is not None and config.get('ds_log') != '':
            config['db_mysql_ip_log']      = config['ds_log']['ip']
            config['db_mysql_port_log']    = config['ds_log']['port']
            config['db_mysql_service_log'] = config['ds_log']['service']
            config['db_mysql_user_log']    = config['ds_log']['user']
            config['db_mysql_pass_log']    = aes_decrypt(config['ds_log']['password'],config['ds_log']['user'])
            config['db_log']               = get_ds_mysql_dict(config['db_mysql_ip_log'],
                                                               config['db_mysql_port_log'],
                                                               config['log_db_name'],
                                                               config['db_mysql_user_log'],
                                                               config['db_mysql_pass_log'])
        else:
            print('mysql日志库不能为空!')
            sys.exit(0)
        return config
    else:
        print('load config failure:{0}'.format(res['msg']))
        sys.exit(0)

def print_dict(config):
    print('-'.ljust(85, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    print('-'.ljust(85, '-'))
    for key in config:
        print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '='+str(config[key]))
    print('-'.ljust(85, '-'))


if __name__ == "__main__":
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    # call api get config
    cfg = get_config_from_db(tag)

    # print cfg
    print_dict(cfg)

    # set sync logger status is pause
    print('set sync logger process is pause status!')
    set_real_sync_status(cfg,'PAUSE')

    # loop check sync log num
    while True:
        # get sync log num
        num = get_real_sync_log_num(cfg)
        print('\rsync log num is {} {}'.format(num,' '*10),end='')
        if num == 0 :
           print('\nstart clear real sync log!')
           clear_real_sync_log(cfg)
           print('clear real sync log ok!')
           break
        else:
           time.sleep(1)
           continue

    # set sync logger status is running
    print('set sync logger process is running status!')
    set_real_sync_status(cfg, 'STOP')
    time.sleep(30)
    set_real_sync_status(cfg,'RUNNING')