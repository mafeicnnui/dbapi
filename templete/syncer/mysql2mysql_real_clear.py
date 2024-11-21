#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/12/13 14:03
# @Author : ma.fei
# @File : schedule_clear_sync_log.py
# @Software: PyCharm

import sys
import time
import traceback

import os
import requests
import pymysql
import warnings
import logging
import datetime

def clear_real_sync_log(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   logging.info("delete from `t_db_sync_log` where sync_tag='{}' and status='1'".format(cfg['exec_tag']))
   cr.execute("delete from `t_db_sync_log` where sync_tag='{}' and status='1'".format(cfg['exec_tag']))
   logging.info("delete from `t_db_sync_log` where sync_tag='{}' and status='1' ok!".format(cfg['exec_tag']))

def get_real_sync_log_num(cfg):
   db = cfg['db_log']
   cr = db.cursor()
   cr.execute("SELECT COUNT(0) as num FROM `t_db_sync_log` WHERE  status='0' and sync_tag='{}'".format(cfg['exec_tag']))
   rs = cr.fetchone()
   return rs['num']

def set_real_sync_status(cfg,p_status):
    try:
        par = {'tag':cfg['sync_tag'],'status': p_status}
        url = 'http://{}/set_real_sync_status'.format(cfg['api_server'])
        logging.info("par=" + str(par))
        logging.info("url=" + url)
        res = requests.post(url, data=par,timeout=10).json()
        return res
    except:
        logging.info("set_real_sync_status error")
        logging.info(traceback.format_exc())
        sys.exit(0)

def aes_decrypt(p_api,p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://{}/read_db_decrypt'.format(p_api)
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
    url = 'http://210.13.35.136:20080/read_config_sync'
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
            config['db_mysql_pass_log']    = aes_decrypt(config['api_server'],config['ds_log']['password'],config['ds_log']['user'])
            config['db_log']               = get_ds_mysql_dict(config['db_mysql_ip_log'],
                                                               config['db_mysql_port_log'],
                                                               config['log_db_name'],
                                                               config['db_mysql_user_log'],
                                                               config['db_mysql_pass_log'])
            config['exec_tag'] = config['sync_tag'].replace('_executer', '_logger')
        else:
            logging.info('mysql日志库不能为空!')
            sys.exit(0)
        return config
    else:
        logging.info('load config failure:{0}'.format(res['msg']))
        sys.exit(0)

def print_dict(config):
    logging.info('-'.ljust(85, '-'))
    logging.info(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    logging.info('-'.ljust(85, '-'))
    for key in config:
        logging.info(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
    logging.info('-'.ljust(85, '-'))

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def read_real_sync_status(p_tag):
    try:
        par = {'tag': p_tag}
        url = 'http://$$API_SERVER$$/get_real_sync_status'
        res = requests.post(url,data=par,timeout=3).json()
        return res
    except:
        logging.info('read_real_sync_status failure!')
        return None


if __name__ == "__main__":
    tag = ""
    sync_time = datetime.datetime.now()
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    # init logger
    logging.basicConfig(filename='/tmp/{}.{}.log'.format(tag,datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    # call api get config
    cfg = get_config_from_db(tag)

    # query system parameters to determine whether to run the  program
    if read_real_sync_status(tag) is None or read_real_sync_status(tag)['msg']['real_sync_status'] == 'STOP':
        logging.info("\033[1;37;40mclear log task {} terminate!\033[0m".format(cfg['sync_tag']))
        sys.exit(0)

    # print cfg
    print_dict(cfg)

    # set sync logger status is pause
    logging.info('set sync logger process is pause status[sync_tag:{}]!'.format(cfg['exec_tag']))
    set_real_sync_status(cfg,'STOP')

    logging.info('sleep 6s!')
    time.sleep(6)
    logging.info('delete {} log file!'.format(tag))
    os.system("rm /tmp/{}*.log".format(tag))

    logging.info('\nstart clear real sync log![{}]'.format(cfg['exec_tag']))
    clear_real_sync_log(cfg)
    logging.info('clear real sync log ok![{}]'.format(cfg['exec_tag']))

    # set sync logger status is running
    logging.info('set sync logger process is running status[{}]!'.format(cfg['exec_tag']))
    set_real_sync_status(cfg,'RUNNING')