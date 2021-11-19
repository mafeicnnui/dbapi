#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/8/6 10:54
# @Author : ma.fei
# @File : schedule.py.py
# @Software: PyCharm

import sys
import time
import requests
import pymysql
import datetime
import warnings
import traceback
import json
from clickhouse_driver import Client
from concurrent.futures import ProcessPoolExecutor,wait,as_completed

def log(msg,pos='l'):
    tm = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    if pos == 'l':
       print("""{} : {}""".format(tm,msg))
    else:
       print("""{} : {}""".format(msg,tm))

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par,timeout=1).json()
        if res['code'] == 200:
            config = res['msg']
            return config
        else:
            log('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        log('aes_decrypt api not available!')

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           autocommit=True)
    return conn

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

def check_tab_exists_pk(cfg,event):
   db = get_ds_mysql(cfg['db_mysql_ip'],
                     cfg['db_mysql_port'],
                     cfg['db_mysql_service'],
                     cfg['db_mysql_user'],
                     cfg['db_mysql_pass'])

   cr = db.cursor()
   st = """select count(0) from information_schema.columns
              where table_schema='{}' and table_name='{}' and column_key='PRI'""".format(event['schema'],event['table'])
   cr.execute(st)
   rs=cr.fetchone()
   cr.close()
   return rs[0]

def get_tables(cfg,o):
    db = get_ds_mysql(cfg['db_mysql_ip'],
                      cfg['db_mysql_port'],
                      cfg['db_mysql_service'],
                      cfg['db_mysql_user'],
                      cfg['db_mysql_pass'])
    cr  = db.cursor()
    sdb = o.split('$')[0].split('.')[0]
    tab = o.split('$')[0].split('.')[1]
    ddb = o.split('$')[1]
    if tab.count('*') > 0:
       tab = tab.replace('*','')
       st = """select table_name from information_schema.tables
                  where table_schema='{}' and instr(table_name,'{}')>0 order by table_name""".format(sdb,tab)
       cr.execute(st)
       rs = cr.fetchall()
       vv = ''
       for i in list(rs):
           evt = {'schema': o.split('$')[0].split('.')[0], 'table': i[0]}
           if check_tab_exists_pk(cfg,evt)>0:
              vv = vv + '{}.{}${},'.format(sdb,i[0],ddb)
       cr.close()
       return vv[0:-1]
    else:
       return o

def get_sync_tables(cfg):
    v = ''
    for o in cfg['sync_table'].split(','):
        if o !='':
           v = v + get_tables(cfg,o)+','
    cfg['sync_table'] = v[0:-1]
    return cfg

def get_config_from_db(tag):
    url = 'http://$$API_SERVER$$/read_config_sync'
    res = requests.post(url, data= { 'tag': tag},timeout=1).json()
    if res['code'] == 200:
        config                           = res['msg']
        db_mysql_ip                      = config['sync_db_sour'].split(':')[0]
        db_mysql_port                    = config['sync_db_sour'].split(':')[1]
        db_mysql_service                 = config['sync_db_sour'].split(':')[2]
        db_mysql_user                    = config['sync_db_sour'].split(':')[3]
        db_mysql_pass                    = aes_decrypt(config['sync_db_sour'].split(':')[4],db_mysql_user)
        db_ck_ip                         = config['sync_db_dest'].split(':')[0]
        db_ck_port                       = config['sync_db_dest'].split(':')[1]
        db_ck_service                    = config['sync_db_dest'].split(':')[2]
        db_ck_user                       = config['sync_db_dest'].split(':')[3]
        db_ck_pass                       = aes_decrypt(config['sync_db_dest'].split(':')[4],db_ck_user)
        config['db_mysql_ip']            = db_mysql_ip
        config['db_mysql_port']          = db_mysql_port
        config['db_mysql_service']       = db_mysql_service
        config['db_mysql_user']          = db_mysql_user
        config['db_mysql_pass']          = db_mysql_pass
        config['db_ck_ip']               = db_ck_ip
        config['db_ck_port']             = db_ck_port
        config['db_ck_service']          = db_ck_service
        config['db_ck_user']             = db_ck_user
        config['db_ck_pass']             = db_ck_pass
        config['db_ck_string']           = db_mysql_ip + ':' + db_mysql_port + '/' + db_mysql_service
        config['db_ck_string']           = db_ck_ip + ':' + db_ck_port + '/' + db_ck_service
        if config.get('ds_log') is not None and config.get('ds_log') != '':
            config['db_mysql_ip_log']      = config['ds_log']['ip']
            config['db_mysql_port_log']    = config['ds_log']['port']
            config['db_mysql_service_log'] = config['ds_log']['service']
            config['db_mysql_user_log']    = config['ds_log']['user']
            config['db_mysql_pass_log']    = aes_decrypt(config['ds_log']['password'],config['ds_log']['user'])
        else:
            print('clickhouse日志库不能为空!')
            sys.exit(0)

        config = get_sync_tables(config)
        return config
    else:
        log('load config failure:{0}'.format(res['msg']))
        return None

def get_tasks(cfg):
    db = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                          cfg['db_mysql_port_log'],
                          cfg['log_db_name'],
                          cfg['db_mysql_user_log'],
                          cfg['db_mysql_pass_log'])
    cr = db.cursor()
    st = "SELECT sync_table,count(0) as amount FROM t_db_sync_log WHERE status='0' GROUP BY sync_table limit 20"
    cr.execute(st)
    rs =cr.fetchall()
    return  rs

def get_ins_header(cfg,event):
    v_ddl = 'insert into {0}.{1} ('.format(get_ck_schema(cfg,event), event['table'])
    if event['action'] == 'insert':
        for key in event['data']:
            v_ddl = v_ddl + '`{0}`'.format(key) + ','
        v_ddl = v_ddl[0:-1] + ')'
    elif event['action'] == 'update':
        for key in event['after_values']:
            v_ddl = v_ddl + '`{0}`'.format(key) + ','
        v_ddl = v_ddl[0:-1] + ')'
    return v_ddl

def get_ck_schema(cfg,event):
    for o in cfg['sync_table'].split(','):
      mysql_schema = o.split('.')[0]
      mysql_table  = o.split('$')[0].split('.')[1]
      ck_schema    = o.split('$')[1] if o.split('$')[1] !='auto' else mysql_schema
      if event['schema'] == mysql_schema and event['table'] == mysql_table:
         return  cfg['desc_db_prefix']+ck_schema
    return 'test'

def get_ins_values(event):
    v_tmp=''
    if event['action'] == 'insert':
        for key in event['data']:
            if event['data'][key]==None:
               v_tmp=v_tmp+"null,"
            elif event['type'][key] in('tinyint','int','bigint','float','double','decimal'):
               v_tmp = v_tmp +  str(event['data'][key]) + ","
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['data'][key])) + "',"
    elif  event['action'] == 'update':
        for key in event['after_values']:
            if event['after_values'][key]==None:
               v_tmp=v_tmp+"null,"
            elif  event['type'][key] in('tinyint','int','bigint','float','double','decimal'):
               v_tmp = v_tmp +  str(event['after_values'][key]) + ","
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['after_values'][key])) + "',"
    return v_tmp[0:-1]

def set_column(event):
    v_set = ' '
    for key in event['after_values']:
        if event['after_values'][key] is None:
           v_set = v_set + key + '=null,'
        else:
           if event['pkn'].count(key)==0:
               if event['type'][key] in ('tinyint', 'int', 'bigint', 'float', 'double','decimal'):
                   v_set = v_set + key + '='+ format_sql(str(event['after_values'][key])) + ','
               else:
                   v_set = v_set + key + '=\''+ format_sql(str(event['after_values'][key])) + '\','
    return v_set[0:-1]

def get_where(event):
    v_where = ' where '
    if event['action'] == 'delete':
        for key in event['data']:
            if event['pks'] :
                if key in event['pkn']:
                    if event['type'][key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                       v_where = v_where + key + ' = ' + str(event['data'][key]) + ' and '
                    else:
                       v_where = v_where + key + ' = \'' + str(event['data'][key]) + '\' and '
            else:
               v_where = v_where+ key+' = \''+str(event['data'][key]) + '\' and '
    elif event['action'] == 'update':
        for key in event['after_values']:
            if event['pks'] :
                if key in event['pkn']:
                    if event['type'][key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                       v_where = v_where + key + ' = ' + str(event['after_values'][key]) + ' and '
                    else:
                       v_where = v_where + key + ' = \'' + str(event['after_values'][key]) + '\' and '
            else:
               v_where = v_where+ key+' = \''+str(event['after_values'][key]) + '\' and '
    return v_where[0:-5]

def gen_sql(cfg,event):
    if event['action'] in ('insert'):
        sql  = get_ins_header(cfg,event)+ ' values ('+get_ins_values(event)+');'
    elif event['action'] == 'update':
        sql = 'alter table {0}.{1} update {2} {3}'.\
               format(get_ck_schema(cfg, event),event['table'],set_column(event),get_where(event))
    elif event['action']=='delete':
        sql  = 'alter table {0}.{1} delete {2}'.format(get_ck_schema(cfg,event),event['table'],get_where(event))
    return sql

def write_ck(cfg,tab):
    db_log = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                               cfg['db_mysql_port_log'],
                               cfg['log_db_name'],
                               cfg['db_mysql_user_log'],
                               cfg['db_mysql_pass_log'])
    cr_log = db_log.cursor()
    st_log = "select id,statement from t_db_sync_log where sync_table='{}' and status='0'  order by id".format(tab)
    cr_log.execute(st_log)
    rs_log = cr_log.fetchall()
    db_ck  = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    for r in rs_log:
        #print('>>>>>>>>>1:',r['statement'])
        #print('>>>>>>>>>2:',gen_sql(cfg,json.loads(r['statement'])))
        #time.sleep(10)
        #st = gen_sql(cfg,json.loads(r['statement']))
        db_ck.execute(r['statement'])
        cr_log.execute("update t_db_sync_log set status='1' where id={}".format(r['id']))

    # 一批可以更新一次，提升性能
    log('Task {} execute complete!'.format(tab))

def main():
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    cfg = get_config_from_db(tag)

    print_dict(cfg)

    with ProcessPoolExecutor(max_workers=20) as executor:
        while True:
            # 定时重新加载配置

            tasks = get_tasks(cfg)
            if tasks!=():
                log('\n检测到新事件：','r')
                print('-'.ljust(85, '-'))
                for task in tasks:
                    print(' '.ljust(3, ' ') + task['sync_table'].ljust(50, ' ') + ' = ', task['amount'])
                print('-'.ljust(85, '-'))

                all_task = [executor.submit(write_ck,cfg,t['sync_table']) for t in tasks]

                for future in as_completed(all_task):
                    res = future.result()
                    if res is not None:
                       log(res)
            else:
               time.sleep(0.5)
               print('\nSleepping...'.format(),end='')

if __name__=="__main__":
     main()
