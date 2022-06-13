#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/8/6 10:54
# @Author : ma.fei
# @File : schedule.py.py
# @Software: PyCharm

import os
import sys
import time
import json
import requests
import pymysql
import datetime
import warnings
import traceback
import logging
from clickhouse_driver import Client
from concurrent.futures import ProcessPoolExecutor,wait,as_completed

def log(msg,pos='l'):
    tm = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    if pos == 'l':
       print("""{} : {}""".format(tm,msg))
    else:
       print("""{} : {}""".format(msg,tm))

def log2(msg):
    tm = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    print("""\r{} : {}""".format(tm,msg),end='')

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    print('-'.ljust(85, '-'))
    logging.info('-'.ljust(85, '-'))
    logging.info(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    logging.info('-'.ljust(85, '-'))
    for key in config:
        print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '='+str(config[key]))
        logging.info(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
    print('-'.ljust(85, '-'))
    logging.info('-'.ljust(85, '-'))

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
        config['exec_tag']               = config['sync_tag'].replace('_executer', '_logger')
        config['sleep_time']             = float(config['sync_gap'])

        if config.get('ds_ro') is not None and config.get('ds_ro') != '':
            config['db_mysql_ip_ro']      = config['ds_ro']['ip']
            config['db_mysql_port_ro']    = config['ds_ro']['port']
            config['db_mysql_service_ro'] = config['ds_ro']['service']
            config['db_mysql_user_ro']    = config['ds_ro']['user']
            config['db_mysql_pass_ro']    = aes_decrypt(config['ds_ro']['password'],config['ds_ro']['user'])

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
        sys.exit(0)

def get_ck_schema(cfg,event):
    for o in cfg['sync_table'].split(','):
      mysql_schema = o.split('.')[0]
      mysql_table  = o.split('$')[0].split('.')[1]
      ck_schema    = o.split('$')[1] if o.split('$')[1] !='auto' else mysql_schema
      if event['schema'] == mysql_schema and event['table'] == mysql_table:
         return  cfg['desc_db_prefix']+ck_schema
    return 'test'

def check_ck_database(cfg,db,event):
    sql = """select count(0) from system.databases d 
                  where name ='{}'""".format(get_ck_schema(cfg, event))
    rs = db.execute(sql)
    return rs[0][0]

def check_ck_tab_exists(cfg,db,event):
   sql="""select count(0) from system.tables
            where database='{}' and name='{}'""".format(get_ck_schema(cfg,event),event['table'])
   rs = db.execute(sql)
   return rs[0][0]

def get_ck_async_task(cfg):
    db = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    st = "select count(0) from system.mutations where is_done=0"
    rs = db.execute(st)
    return rs[0][0]

def get_ck_async_task_table(cfg,db,event):
    st = "select count(0) from system.mutations where is_done=0 and database='{}' and table='{}'".format(get_ck_schema(cfg,event),event['table'])
    rs = db.execute(st)
    return rs[0][0]

def check_ck_tab_exists_data(cfg,db,event):
   st = "select count(0) from {}.{}".format(get_ck_schema(cfg,event),event['table'])
   rs = db.execute(st)
   return rs[0][0]

def get_ck_table_defi_mysql(cfg,event):
    db = get_ds_mysql(cfg['db_mysql_ip'],
                      cfg['db_mysql_port'],
                      cfg['db_mysql_service'],
                      cfg['db_mysql_user'],
                      cfg['db_mysql_pass'])
    cr = db.cursor()
    st = """SELECT  `column_name`,data_type,is_nullable
              FROM information_schema.columns
              WHERE table_schema='{}'
                AND table_name='{}'  ORDER BY ordinal_position""".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    st= 'create table `{}`.`{}` (\n '.format(get_ck_schema(cfg,event),event['table']+'_tmp')
    for i in rs:
        if i[1] == 'tinyint':
            st = st + ' `{}` {},\n'.format(i[0], 'Int16' if i[2] == 'NO' else 'Nullable(Int16)')
        elif i[1] == 'int':
            st = st + ' `{}` {},\n'.format(i[0], 'Int32' if i[2] == 'NO' else 'Nullable(Int32)')
        elif i[1] == 'bigint':
            st = st + ' `{}` {},\n'.format(i[0], 'Int64' if i[2] == 'NO' else 'Nullable(Int64)')
        elif i[1] == 'varchar':
            st = st + ' `{}` {},\n'.format(i[0], 'String' if i[2] == 'NO' else 'Nullable(String)')
        elif i[1] == 'timestamp':
            st = st + ' `{}` {},\n'.format(i[0], 'DateTime' if i[2] == 'NO' else 'Nullable(DateTime)')
        elif i[1] == 'datetime':
            st = st + ' `{}` {},\n'.format(i[0], 'DateTime' if i[2] == 'NO' else 'Nullable(DateTime)')
        elif i[1] == 'date':
            st = st + ' `{}` {},\n'.format(i[0], 'Date' if i[2] == 'NO' else 'Nullable(Date)')
        elif i[1] == 'text':
            st = st + ' `{}` {},\n'.format(i[0], 'String' if i[2] == 'NO' else 'Nullable(String)')
        elif i[1] == 'longtext':
            st = st + ' `{}` {},\n'.format(i[0], 'String' if i[2] == 'NO' else 'Nullable(String)')
        elif i[1] == 'float':
            st = st + ' `{}` {},\n'.format(i[0], 'Float32' if i[2] == 'NO' else 'Nullable(Float32)')
        elif i[1] == 'double':
            st = st + ' `{}` {},\n'.format(i[0], 'Float64' if i[2] == 'NO' else 'Nullable(Float64)')
        else:
            st = st + ' `{}` {},\n'.format(i[0], 'String' if i[2] == 'NO' else 'Nullable(String)')
    db.commit()
    cr.close()
    if cfg.get('ds_ro') is not None and cfg.get('ds_ro') != '':
        engine = """ ENGINE = MySQL('{}','{}','{}','{}','{}')""" \
            .format(cfg['db_mysql_ip_ro'] + ':' + cfg['db_mysql_port_ro'],
                    event['schema'],
                    event['table'],
                    cfg['db_mysql_user'],
                    cfg['db_mysql_pass'])
    else:
        engine = """ ENGINE = MySQL('{}','{}','{}','{}','{}')"""\
            .format(cfg['db_mysql_ip']+':'+cfg['db_mysql_port'],
                    event['schema'],
                    event['table'],
                    cfg['db_mysql_user'],
                    cfg['db_mysql_pass'])
    st = st[0:-2]+') ' + engine
    return st

def get_cols_from_mysql(cfg,event):
    db = get_ds_mysql(cfg['db_mysql_ip'],
                      cfg['db_mysql_port'],
                      cfg['db_mysql_service'],
                      cfg['db_mysql_user'],
                      cfg['db_mysql_pass'])
    cr = db.cursor()
    v_col = ''
    v_sql = """select column_name 
                 from information_schema.columns
                 where table_schema='{}'
                   and table_name='{}'  order by ordinal_position
             """.format(event['schema'], event['table'])
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col = v_col + '`{}`,'.format(i[0])
    cr.close()
    return v_col[0:-1]

def full_sync(cfg,event):
    db = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    log('create clickhouse temporary table: {}.{} ok!'.format(get_ck_schema(cfg, event),event['table']+'_tmp'))
    st = get_ck_table_defi_mysql(cfg,event)
    db.execute(st)

    log('full sync table:{}.{} ...'.format(get_ck_schema(cfg, event),event['table']))
    col = get_cols_from_mysql(cfg, event)
    st = """insert into {}.{} ({}) select {} from {}.{}
         """.format(get_ck_schema(cfg, event),event['table'], col,col,get_ck_schema(cfg, event),event['table']+'_tmp')
    db.execute(st)

    log('drop temp table:{}.{}'.format(get_ck_schema(cfg, event), event['table'] + '_tmp'))
    st = 'drop table {}.{}'.format(get_ck_schema(cfg, event),event['table']+'_tmp')
    db.execute(st)

def merge_buffer(buffer):
    type = buffer[0]['type']
    table = buffer[0]['sync_table']
    if type == 'insert':
       h = buffer[0]['statement'].split(' values ')[0]
       b = ''
       i = ''
       for o in buffer:
         b = b + o['statement'].split(' values ')[1]+','
         i = i + str(o['id'])+','
       return  [
           {'sync_table':table,'type': type, 'statement': h + ' values ' + b[0:-1],'id':i[0:-1]}
       ]

    if type == 'delete':
        hd = buffer[0]['statement'].split(' = ')[0]
        bd = ''
        id= ''
        for o in buffer:
            if len(hd.split('where')[1].split(',')) > 1:
                bd = bd + o['statement'].split('^^^')[0].split(' = ')[1] + ' union all '
            else:
                bd = bd + o['statement'].split('^^^')[0].split(' = ')[1] + ','

            id = id + str(o['id']) + ','

        if len(hd.split('where')[1].split(',')) > 1:
            return [
                {'sync_table': table, 'type': 'delete', 'statement': hd + ' in (' + bd[0:-10] + ')', 'id': id[0:-1]},
            ]
        else:
            return [
                {'sync_table': table, 'type': 'delete', 'statement': hd + ' in (' + bd[0:-1] + ')', 'id': id[0:-1]},
            ]

    if type == 'update':
       bd = ''
       bi = ''
       id = ''
       hd = buffer[0]['statement'].split('^^^')[0].split(' = ')[0]
       hi = buffer[0]['statement'].split('^^^')[1].split(' values ')[0]
       for o in buffer:
           if len(hd.split('where')[1].split(','))>1:
              bd = bd + o['statement'].split('^^^')[0].split(' = ')[1]+ ' union all '
           else:
              bd = bd + o['statement'].split('^^^')[0].split(' = ')[1] + ','

           bi = bi + o['statement'].split('^^^')[1].split(' values ')[1]+','
           id = id + str(o['id']) + ','

       if  len(hd.split('where')[1].split(','))>1:
           return [
                  { 'sync_table':table,'type': 'delete', 'statement': hd + ' in (' + bd[0:-10] + ')','id': id[0:-1] },
                  { 'sync_table':table,'type': 'insert', 'statement': hi + ' values ' + bi[0:-1],'id': id[0:-1] }
           ]
       else:
           return [
               {'sync_table': table, 'type': 'delete', 'statement': hd + ' in (' + bd[0:-1] + ')', 'id': id[0:-1]},
               {'sync_table': table, 'type': 'insert', 'statement': hi + ' values ' + bi[0:-1], 'id': id[0:-1]}
           ]

def process_sql(logs):
    nbatch = []
    buffer = []
    latest_table = logs[0]['sync_table']
    latest_event = logs[0]['type']
    buffer.append({'sync_table':logs[0]['sync_table'],'type':latest_event,'statement':logs[0]['statement'],'id':logs[0]['id']})

    for log in logs[1:]:
        if log['sync_table'] == latest_table and log['type'] == latest_event:
           buffer.append({'sync_table':log['sync_table'],'type':log['type'],'statement':log['statement'],'id':log['id']})
        else:
           if len(buffer)>0:
               nbatch.extend(merge_buffer(buffer))
               buffer = []
           buffer.append({'sync_table':log['sync_table'],'type':log['type'],'statement':log['statement'],'id':log['id']})
           latest_table = log['sync_table']
           latest_event = log['type']

    if  len(buffer)>0:
        nbatch.extend(merge_buffer(buffer))
    return nbatch

def get_mysql_columns(cfg,schema,table,ck_cols):
    db = get_ds_mysql_dict(cfg['db_mysql_ip'],
                           cfg['db_mysql_port'],
                           cfg['db_mysql_service'],
                           cfg['db_mysql_user'],
                           cfg['db_mysql_pass'])
    cr = db.cursor()
    st = """select table_name,column_name,data_type,is_nullable,datetime_precision
            from information_schema.columns  
             where table_schema='{}' and table_name='{}' and column_name not in ({})""".format(schema,table,ck_cols)
    cr.execute(st)
    rs =cr.fetchall()
    return rs

def get_ck_columns(cfg,schema,table):
    db = get_ds_ck(cfg['db_ck_ip'],
                   cfg['db_ck_port'],
                   cfg['db_ck_service'],
                   cfg['db_ck_user'],
                   cfg['db_ck_pass'])
    event = {'schema': schema, 'table': table}
    st = """select name from system.columns where database='{}' and table='{}'""".format(get_ck_schema(cfg, event), event['table'])
    print('get_ck_columns=',st)
    rs = db.execute(st)
    v = ''
    for i in rs:
       v = v + "'{}',".format(i[0])
    print('v=',v)
    return v[0:-1]

def mysql_type_convert_ck(p_type,p_is_null,p_datetime_precision):
    if p_type == 'tinyint':
        return 'Int16' if p_is_null == 'NO' else 'Nullable(Int16)'
    elif p_type == 'int':
       return 'Int32' if p_is_null == 'NO' else 'Nullable(Int32)'
    elif p_type == 'bigint':
       return 'Int64' if p_is_null == 'NO' else 'Nullable(Int64)'
    elif p_type == 'varchar':
       return 'String' if p_is_null == 'NO' else 'Nullable(String)'
    elif p_type == 'timestamp':
        return 'DateTime' if p_is_null == 'NO' else 'Nullable(DateTime)'
    elif p_type == 'datetime':
        if p_datetime_precision == 6:
            return 'DateTime64' if p_is_null == 'NO' else 'Nullable(DateTime64)'
        else:
            return 'DateTime' if p_is_null == 'NO' else 'Nullable(DateTime)'
    elif p_type== 'date':
        return 'Date' if p_is_null == 'NO' else 'Nullable(Date)'
    elif p_type == 'text':
        return 'String' if p_is_null == 'NO' else 'Nullable(String)'
    elif p_type == 'longtext':
        return 'String' if p_is_null == 'NO' else 'Nullable(String)'
    elif p_type == 'float':
        return 'Float32' if p_is_null == 'NO' else 'Nullable(Float32)'
    elif p_type == 'double':
        return 'Float64' if p_is_null == 'NO' else 'Nullable(Float64)'
    else:
        return 'String' if p_is_null == 'NO' else 'Nullable(String)'

def sync_alter(cfg,schema,table):
    db_ck = get_ds_ck(cfg['db_ck_ip'],
                   cfg['db_ck_port'],
                   cfg['db_ck_service'],
                   cfg['db_ck_user'],
                   cfg['db_ck_pass'])
    ck_cols = get_ck_columns(cfg,schema,table)
    mysql_cols = get_mysql_columns(cfg,schema,table,ck_cols)
    event = {'schema': schema, 'table': table}
    for m in mysql_cols:
        v ="""alter table {}.{} add column {} {}
           """.format(get_ck_schema(cfg, event),
                      event['table'],
                      m['column_name'],
                      mysql_type_convert_ck(m['data_type'],m['is_nullable'],m['datetime_precision']))
        print(v)
        db_ck.execute(v)

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def write_sync_log(config):
    par = {
            'sync_tag'       : config['sync_tag'],
            'event_amount'   : config['event_amount'],
            'insert_amount'  : config['insert_amount'],
            'update_amount'  : config['update_amount'],
            'delete_amount'  : config['delete_amount'],
            'ddl_amount'     : config['ddl_amount'],
            'binlogfile'     : '',
            'binlogpos'      : '',
            'c_binlogfile'   : '',
            'c_binlogpos'    : '',
            'create_date'    : get_time()
    }
    try:
        url = 'http://$$API_SERVER$$/write_sync_real_log'
        res = requests.post(url, data={'tag': json.dumps(par)},timeout=3)
        if res.status_code != 200:
           print('Interface write_sync_log call failed!')
    except:
         traceback.print_exc()
         sys.exit(0)


def write_ck(cfg,tab):
    db_log = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                               cfg['db_mysql_port_log'],
                               cfg['log_db_name'],
                               cfg['db_mysql_user_log'],
                               cfg['db_mysql_pass_log'])
    cr_log = db_log.cursor()
    st_log = """select id,sync_table,statement,type
                   from t_db_sync_log where  status='0' and sync_tag='{}' and sync_table='{}' 
                       order by id limit {}""".format(cfg['exec_tag'],tab,cfg['batch_size_incr'])
    cr_log.execute(st_log)
    rs_log = cr_log.fetchall()

    cfg['event_amount'] = len(rs_log)
    cfg['insert_amount'] = 0
    cfg['update_amount'] = 0
    cfg['delete_amount'] = 0
    cfg['ddl_amount'] = 0
    write_sync_log(cfg)
    log("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))

    rs_log_process=process_sql(rs_log)
    print('rs_log_process>>>:',rs_log_process)
    db_ck  = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    ids=''
    for r in rs_log_process:
        event = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        if check_ck_tab_exists(cfg,db_ck,event) == 0:
           log('Table:{}.{} not exists,skip incr sync!'.format(get_ck_schema(cfg,event),event['table']))
           continue

        try:
           log2('wait ck async task complete...')
           while True:
               evt = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
               if get_ck_async_task_table(cfg,db_ck,evt) == 0:
                  break
               time.sleep(0.1)
           print('Execute>>>:',r['statement'])
           db_ck.execute(r['statement'])
           ids = ids + '{},'.format(r['id'])
        except:
           if traceback.format_exc().count('No such column')>0:
              sync_alter(cfg,event['schema'],event['table'])
              db_ck.execute(r['statement'])
           else :
              traceback.print_exc()
              print('\033[1;36;40mrs_log\033[0m', rs_log)
              print('\033[1;36;40mrs_log_process\033[0m', rs_log_process)
              print('\033[0;36;40m'+r['statement']+'\033[0m')
              sys.exit(0)


    if ids != '':
        upd = "update t_db_sync_log set status='1' where id in({})".format(ids[0:-1])
        try:
          print('Execute>>>:', upd)
          cr_log.execute(upd)
          print('Task {} execute complete!'.format(tab))
        except:
          traceback.print_exc()
          print('\033[1;36;40mrs_log\033[0m', rs_log)
          print('\033[1;36;40mrs_log_process\033[0m', rs_log_process)
          print('\033[1;36;40mids\033[0m', ids)
          print('\033[1;36;40mupd===========>\033[0m', upd)
          sys.exit(0)


def get_tasks(cfg):
    db = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                           cfg['db_mysql_port_log'],
                           cfg['log_db_name'],
                           cfg['db_mysql_user_log'],
                           cfg['db_mysql_pass_log'])
    cr = db.cursor()
    st = """SELECT sync_table,count(0) as amount FROM t_db_sync_log 
               WHERE  status='0' and sync_tag='{}' GROUP BY sync_table limit {}""".format(cfg['exec_tag'],cfg['process_num'])
    cr.execute(st)
    rs =cr.fetchall()
    return  rs

def start_syncer(cfg):
    apply_time = datetime.datetime.now()
    sleep_time = datetime.datetime.now()
    with ProcessPoolExecutor(max_workers=cfg['process_num']) as executor:
        while True:
            if get_seconds(apply_time) >= cfg['apply_timeout']:
               apply_time = datetime.datetime.now()
               cfg = get_config_from_db(cfg['sync_tag'])
               log("\033[1;36;40\nmapply config success\033[0m")

            tasks = get_tasks(cfg)
            if tasks!=():
                log('\n检测到新事件：','r')
                print('-'.ljust(85, '-'))
                for task in tasks:
                    print(' '.ljust(3, ' ') + task['sync_table'].ljust(50, ' ') + ' = ', task['amount'])
                print('-'.ljust(85, '-'))
                print('\n')

                async_task_amount = get_ck_async_task(cfg)
                if round(async_task_amount / 500)>=1:
                   log("\033[1;36;40msleep {}s wait ck async task!\033[0m".format(round(async_task_amount / 500)*3))
                   time.sleep(round(async_task_amount / 500)*3)

                sleep_time = datetime.datetime.now()
                all_task = [executor.submit(write_ck,cfg,t['sync_table']) for t in tasks]

                for future in as_completed(all_task):
                    res = future.result()
                    if res is not None:
                       log(res)
            else:
               time.sleep(0.1)
               print('\r未检测到任务，休眠中:{}s ...'.format(str(get_seconds(sleep_time))),end='')


def get_task_status(cfg):
    c = 'ps -ef|grep {} |grep {} | grep -v grep |  wc -l'.format(cfg['sync_tag'],cfg['script_file'])
    r = os.popen(c).read()
    if int(r) > 2 :
       log('Executer Task already running!')
       return True
    else:
       return False


if __name__=="__main__":
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    # init logger
    logging.basicConfig(filename='/tmp/{}.{}.log'.format(tag, datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    # call api get config
    cfg = get_config_from_db(tag)

    # print cfg
    print_dict(cfg)

    # check task
    if not get_task_status(cfg):
       start_syncer(cfg)
