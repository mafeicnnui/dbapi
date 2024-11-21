#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2022/04/01 14:03
# @Author : ma.fei
# @File : mysql2clickhouse_clear.py
# @Software: PyCharm

import sys
import time
import traceback

import os
import requests
import pymysql
import logging
import datetime
import warnings
from clickhouse_driver import Client

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn

def get_ds_ck(ip,port,service ,user,password):
    return  Client(host=ip,
                   port=port,
                   user=user,
                   password=password,
                   database=service,
                   send_receive_timeout=600000)

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par,timeout=1).json()
        if res['code'] == 200:
            config = res['msg']
            return config
        else:
            logging.info('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
            sys.exit(0)
    except:
        logging.info('aes_decrypt api not available!')
        sys.exit(0)

def clear_real_sync_log(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   logging.info("delete from `t_db_sync_log` where sync_tag='{}' and status='1'".format(cfg['exec_tag']))
   cr.execute("delete from `t_db_sync_log` where sync_tag='{}' and status='1'".format(cfg['exec_tag']))
   logging.info("delete from `t_db_sync_log` where sync_tag='{}' and status='1' ok!".format(cfg['exec_tag']))


def clear_ck_log(cfg):
    db = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    logging.info('truncate table system.query_thread_log')
    db.execute('truncate table system.query_thread_log')
    logging.info('truncate table system.query_log')
    db.execute('truncate table system.query_log')
    logging.info('truncate table system.part_log')
    db.execute('truncate table system.part_log')
    logging.info('truncate table system.trace_log')
    db.execute('truncate table system.trace_log')
    logging.info('truncate table system.asynchronous_metric_log')
    db.execute('truncate table system.asynchronous_metric_log')
    logging.info('truncate table system.metric_log')
    db.execute('truncate table system.metric_log')
    logging.info('truncate table system.session_log')
    db.execute('truncate table system.session_log')

def get_real_sync_log_num(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   logging.info("SELECT COUNT(0) as num FROM `t_db_sync_log` WHERE STATUS='0'")
   cr.execute("SELECT COUNT(0) as num FROM `t_db_sync_log` WHERE STATUS='0'")
   rs = cr.fetchone()
   logging.info("get_real_sync_log_num={}".format(rs['num']))
   return rs['num']

def read_real_sync_status(p_tag):
    try:
        par = {'tag': p_tag}
        url = 'http://$$API_SERVER$$/get_real_sync_status'
        res = requests.post(url,data=par,timeout=3).json()
        return res
    except:
        logging.info('read_real_sync_status failure!')
        return None

def set_real_sync_status(cfg,p_status):
    try:
        par = {'status': p_status,'tag':cfg['sync_tag']}
        url = 'http://$$API_SERVER$$/set_real_sync_status'.format(cfg['api_server'])
        res = requests.post(url, data=par,timeout=10).json()
        logging.info("set_real_sync_status is ok")
        return res
    except:
        logging.info("set_real_sync_status error")
        logging.info(traceback.format_exc())
        sys.exit(0)

def get_config_from_db(tag):
    url = 'http://$$API_SERVER$$/read_config_sync'
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
            config['exec_tag'] = config['sync_tag'].replace('_executer', '_logger')
        else:
            logging.info('clickhouse日志库不能为空!')
            return None
        return config
    else:
        logging.info('load config failure:{0}'.format(res['msg']))
        return None

def print_dict(config):
    logging.info('-'.ljust(85, '-'))
    logging.info(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    logging.info('-'.ljust(85, '-'))
    for key in config:
        logging.info(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
    logging.info('-'.ljust(85, '-'))

if __name__ == "__main__":
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    # check sys parameter
    if read_real_sync_status(tag) == None or read_real_sync_status(tag)['msg']['real_sync_status'] == 'STOP':
          logging.info("\033[1;37;40m sync task {} terminate!\033[0m".format(tag))
          sys.exit(0)

    # init logger
    logging.basicConfig(filename='/tmp/{}.{}.log'.format(tag, datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    # call api get config
    cfg = get_config_from_db(tag)
    if cfg is None:
          logging.info('load config failure,exit sync!')
          sys.exit(0)

    # print cfg
    print_dict(cfg)

    # set sync logger status is stop
    logging.info('set sync logger process is stop status!')
    set_real_sync_status(cfg,'STOP')
    logging.info('sleep 6s!')

    time.sleep(6)
    logging.info('delete {} log file!'.format(tag))
    os.system("rm /tmp/{}*.log".format(tag))
    logging.info('start clear real sync log!')
    clear_real_sync_log(cfg)
    logging.info('clear real sync log ok!')
    clear_ck_log(cfg)
    logging.info('clear ck log ok!')
    time.sleep(3)
    logging.info("waiting log apply process...")
    # set sync logger status is running
    logging.info('set sync logger process is running status!')
    set_real_sync_status(cfg,'RUNNING')
