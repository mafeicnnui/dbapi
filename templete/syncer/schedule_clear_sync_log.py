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

def get_ds_ck(ip,port,service ,user,password):
    return  Client(host=ip,
                   port=port,
                   user=user,
                   password=password,
                   database=service,
                   send_receive_timeout=600000)

def clear_real_sync_log(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   cr.execute("truncate table`t_db_sync_log`")

def clear_ck_log(cfg):
    db = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    db.execute('truncate table system.query_thread_log')
    db.execute('truncate table system.query_log')
    db.execute('truncate table system.part_log')
    db.execute('truncate table system.trace_log')
    db.execute('truncate table system.asynchronous_metric_log')
    db.execute('truncate table system.metric_log')
    db.execute('truncate table system.session_log')

def get_real_sync_log_num(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   cr.execute("SELECT COUNT(0) as num FROM `t_db_sync_log` WHERE STATUS='0'")
   rs = cr.fetchone()
   return rs['num']

def set_real_sync_status(cfg,p_status):
    try:
        par = {'status': p_status}
        url = 'http://{}/set_real_sync_status'.format(cfg['api_server'])
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

        db_ck_ip                = config['sync_db_dest'].split(':')[0]
        db_ck_port              = config['sync_db_dest'].split(':')[1]
        db_ck_service           = config['sync_db_dest'].split(':')[2]
        db_ck_user              = config['sync_db_dest'].split(':')[3]
        db_ck_pass              = aes_decrypt(config['sync_db_dest'].split(':')[4], db_ck_user)
        config['db_ck_ip']      = db_ck_ip
        config['db_ck_port']    = db_ck_port
        config['db_ck_service'] = db_ck_service
        config['db_ck_user']    = db_ck_user
        config['db_ck_pass']    = db_ck_pass
        config['db_ck_string']  = db_ck_ip + ':' + db_ck_port + '/' + db_ck_service

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
            print('clickhouse日志库不能为空!')
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

    tag = 'hst_prod_real_sync_mysql_clickhouse_logger'

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
           clear_ck_log(cfg)
           print('clear clickhouse log ok!')
           break
        else:
           time.sleep(1)
           continue

    # set sync logger status is running
    print('set sync logger process is running status!')
    set_real_sync_status(cfg,'RUNNING')