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
import psutil
from clickhouse_driver import Client
from concurrent.futures import ProcessPoolExecutor,wait,as_completed

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    logging.info('-'.ljust(85, '-'))
    logging.info(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    logging.info('-'.ljust(85, '-'))
    for key in config:
        logging.info(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
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
            logging.info('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
            sys.exit(0)
    except:
        logging.info('aes_decrypt api not available!')
        sys.exit(0)

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

def get_ds_ck_cluster(cfg):
    ds = {
        'shards' :{},
        'cluster':''
    }
    if cfg['dest_db_type'] == '10':
        for ip in cfg['db_ck_ip'].split(','):
            ds['shards'][ip] = Client(host=ip,
                                      port=cfg['db_ck_port'],
                                      user=cfg['db_ck_user'],
                                      password=cfg['db_ck_pass'],
                                      database=cfg['db_ck_service'],
                                      send_receive_timeout=600000)

        ds['cluster'] =  Client(host=cfg['db_ck_ip'].split(',')[0],
                                port=cfg['db_ck_port'],
                                user=cfg['db_ck_user'],
                                password=cfg['db_ck_pass'],
                                database=cfg['db_ck_service'],
                                send_receive_timeout=600000)
        return  ds
    else:
        return None

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
            logging.info('clickhouse日志库不能为空!')
            return None

        config = get_sync_tables(config)
        return config
    else:
        logging.info('load config failure:{0}'.format(res['msg']))
        return None

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

def check_ck_tab_exists_all(cfg,db,event):
   sql="""select count(0) from system.tables
            where database='{}' and name='{}'""".format(get_ck_schema(cfg,event)+'_all',event['table'])
   rs = db.execute(sql)
   return rs[0][0]

def get_ck_async_task(cfg):
    res = {}
    db_ck= get_ds_ck_cluster(cfg)
    for ip in db_ck['shards']:
        st = "select count(0) from system.mutations where is_done=0"
        rs = db_ck['shards'][ip].execute(st)
        res[ip] = rs[0][0]
    return res

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
    logging.info('create clickhouse temporary table: {}.{} ok!'.format(get_ck_schema(cfg, event),event['table']+'_tmp'))
    st = get_ck_table_defi_mysql(cfg,event)
    db.execute(st)

    logging.info('full sync table:{}.{} ...'.format(get_ck_schema(cfg, event),event['table']))
    col = get_cols_from_mysql(cfg, event)
    st = """insert into {}.{} ({}) select {} from {}.{}
         """.format(get_ck_schema(cfg, event),event['table'], col,col,get_ck_schema(cfg, event),event['table']+'_tmp')
    db.execute(st)

    logging.info('drop temp table:{}.{}'.format(get_ck_schema(cfg, event), event['table'] + '_tmp'))
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
       return {'sync_table':table,'type': type, 'statement': h + ' values ' + b[0:-1],'id':i[0:-1]}

    else:
       return buffer

def process_sql(logs):
    n_batch = []
    ins_buffer = []
    for log in logs:
        if log['type']=='insert':
           ins_buffer.append({'sync_table':log['sync_table'],'type':log['type'],'statement':log['statement'],'id':log['id']})
        else:
           if len(ins_buffer)>0:
               n_batch.append(merge_buffer(ins_buffer))
               ins_buffer = []
           n_batch.append({'sync_table':log['sync_table'],'type':log['type'],'statement':log['statement'],'id':log['id']})
    if  len(ins_buffer)>0:
        n_batch.append(merge_buffer(ins_buffer))
    return n_batch

def merge_buffer_n(buffer):
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

def process_sql_n(logs):
    batch  = []
    buffer = []
    latest_table = logs[0]['sync_table']
    latest_event = logs[0]['type']
    buffer.append({'sync_table':logs[0]['sync_table'],'type':latest_event,'statement':logs[0]['statement'],'id':logs[0]['id']})
    for log in logs[1:]:
        if log['sync_table'] == latest_table and log['type'] == latest_event:
           buffer.append({'sync_table':log['sync_table'],'type':log['type'],'statement':log['statement'],'id':log['id']})
        else:
           if len(buffer)>0:
               batch.extend(merge_buffer_n(buffer))
               buffer = []
           buffer.append({'sync_table':log['sync_table'],'type':log['type'],'statement':log['statement'],'id':log['id']})
           latest_table = log['sync_table']
           latest_event = log['type']
    if  len(buffer)>0:
        batch.extend(merge_buffer_n(buffer))
    #logging.info("batch="+str(batch))
    return batch

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
    logging.info('get_ck_columns='+st)
    rs = db.execute(st)
    v = ''
    for i in rs:
       v = v + "'{}',".format(i[0])
    logging.info('v='+v)
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
        logging.info(v)
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
           logging.info('Interface write_sync_log call failed!')
    except:
        logging.info('Interface write_sync_log call failed!')
        logging.info(traceback.format_exc())
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
    logging.info("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))
    db_ck  = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    ids=''
    for r in  rs_log: #rs_log_process:
        event = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        if check_ck_tab_exists(cfg,db_ck,event) == 0:
           logging.info('Table:{}.{} not exists,skip incr sync!'.format(get_ck_schema(cfg,event),event['table']))
           continue
        try:
           #logging.info('Execute>>>:'+r['statement'])
           db_ck.execute(r['statement'])
           ids = ids + '{},'.format(r['id'])
        except:
           if traceback.format_exc().count('No such column')>0:
              sync_alter(cfg,event['schema'],event['table'])
              db_ck.execute(r['statement'])
           else :
              logging.info(traceback.format_exc())
              logging.info('\033[0;36;40m'+r['statement']+'\033[0m')
              logging.info("sleep 3s...")
              time.sleep(3)

    logging.info('wait ck async task for table :{}/res:{} complete...'.format(event['table'],len(rs_log)))
    while True:
        evt = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        if get_ck_async_task_table(cfg,db_ck,evt) == 0:
           break
        time.sleep(0.1)

    if ids != '':
        upd = "update t_db_sync_log set status='1' where id in({})".format(ids[0:-1])
        try:
          #logging.info('Execute>>>:'+upd)
          cr_log.execute(upd)
          logging.info('Task {} execute complete!'.format(tab))
        except:
          logging.info(traceback.format_exc())
          logging.info('\033[1;36;40m rs_log\033[0m'+rs_log)
          logging.info('\033[1;36;40m ids\033[0m'+ids)
          logging.info('\033[1;36;40m upd===========>\033[0m'+ upd)
          sys.exit(0)

def write_ck_batch(cfg,tab):
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
    logging.info("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))
    db_ck = get_ds_ck(cfg['db_ck_ip'], cfg['db_ck_port'], cfg['db_ck_service'], cfg['db_ck_user'], cfg['db_ck_pass'])
    ids=''
    for r in  process_sql(rs_log):
        event = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        if check_ck_tab_exists(cfg,db_ck,event) == 0:
           logging.info('Table:{}.{} not exists,skip incr sync!'.format(get_ck_schema(cfg,event),event['table']))
           continue
        try:
           db_ck.execute(r['statement'])
           ids = ids + '{},'.format(r['id'])
        except:
           if traceback.format_exc().count('No such column')>0:
              sync_alter(cfg,event['schema'],event['table'])
              db_ck.execute(r['statement'])
           else :
              logging.info(traceback.format_exc())
              logging.info('\033[0;36;40m'+r['statement']+'\033[0m')
              time.sleep(1)

    logging.info('wait ck async task for table :{}/res:{} complete...'.format(event['table'],len(rs_log)))
    while True:
        evt = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        if get_ck_async_task_table(cfg,db_ck,evt) == 0:
           break
        time.sleep(0.1)

    if ids != '':
        upd = "update t_db_sync_log set status='1' where id in({})".format(ids[0:-1])
        try:
          #logging.info('Execute>>>:'+upd)
          cr_log.execute(upd)
          logging.info('Task {} execute complete!'.format(tab))
        except:
          logging.info(traceback.format_exc())
          logging.info('\033[0;36;40m' + upd + '\033[0m')
          sys.exit(0)

def write_ck_multi(cfg,tab):
    db_log = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                               cfg['db_mysql_port_log'],
                               cfg['log_db_name'],
                               cfg['db_mysql_user_log'],
                               cfg['db_mysql_pass_log'])
    cr_log = db_log.cursor()
    st_map = """ select MAX(id) as id,
                        GROUP_CONCAT(id) as map_id
                  from (select * from `t_db_sync_log`
                         where sync_tag='{}' AND sync_table='{}' 
                           and status='0' order by id limit {} ) as t group by type,pk_val """.format(cfg['exec_tag'],tab,cfg['batch_size_incr'])
    cr_log.execute(st_map)
    rs_map = cr_log.fetchall()
    id_map = {}
    for m in rs_map:
       id_map[str(m['id'])] = str(m['map_id'])

    id = ','.join([str(x['id']) for x in rs_map])
    st_log = """select id,sync_table,statement,type from t_db_sync_log where id in({}) order by id""".format(id)
    cr_log.execute(st_log)
    rs_log = cr_log.fetchall()
    cfg['event_amount'] = len(rs_log)
    cfg['insert_amount'] = 0
    cfg['update_amount'] = 0
    cfg['delete_amount'] = 0
    cfg['ddl_amount'] = 0
    write_sync_log(cfg)
    logging.info("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))
    db_ck = get_ds_ck_cluster(cfg)
    for r in  process_sql_n(rs_log):
        event = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        try:
           if r['type'] == 'insert':
               if check_ck_tab_exists_all(cfg, db_ck['cluster'], event) == 0:
                   logging.info('\033[1;36;40mDistributed table:{}.{} not exists,skip sync!\033[0m'.format(get_ck_schema(cfg, event)+'_all', event['table']))
                   continue
               else:
                   logging.info('\033[1;36;40mDistributed table:{}.{} exec statement!\033[0m'.format(get_ck_schema(cfg, event) + '_all',event['table']))
                   logging.info(r['statement'])
                   db_ck['cluster'].execute(r['statement'])

           if r['type'] == 'delete':
              for ip in  db_ck['shards']:
                 logging.info('check local table {}.{} for sharding `{}`...'.format(get_ck_schema(cfg, event),event['table'],ip))
                 if check_ck_tab_exists(cfg, db_ck['shards'][ip], event) == 0:
                     logging.info('Local Table:{}.{} not exists,skip sync for delete event!'.format(get_ck_schema(cfg, event),event['table']))
                     continue
                 else:
                     logging.info('\033[1;36;40mLocal table {}.{} for sharding `{}`...\033[0m'.format(get_ck_schema(cfg, event),event['table'],ip))
                     logging.info(r['statement'])
                     db_ck['shards'][ip].execute(r['statement'])
                     logging.info('wait ck async task for table :{}/res:{}/type:{}/len:{}...'.
                                 format(event['table'],
                                        len(rs_log),
                                        r['type'],
                                        len(r['id'].split(','))))
                     while True:
                       evt = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
                       if get_ck_async_task_table(cfg, db_ck['shards'][ip], evt) == 0:
                           break
                       time.sleep(0.1)
                     logging.info('ck async task for table :{}.{}/res:{}/type:{}/len:{} complete!'.
                                format(get_ck_schema(cfg, event),
                                       event['table'],
                                       len(rs_log),
                                       r['type'],
                                       len(r['id'].split(','))))

           if r['id'] != '':
               uid = ''
               for id in r['id'].split(','):
                   uid = uid + id_map[id] +','

               upd = "update t_db_sync_log set status='1' where id in({})".format(uid[0:-1])
               try:
                   cr_log.execute(upd)
                   logging.info('Task {} execute complete!'.format(tab))
               except:
                   logging.info(traceback.format_exc())
                   sys.exit(0)

        except:
          logging.info(traceback.format_exc())
          time.sleep(1)

    # delete repeat data
    event = {'schema': tab.split('.')[0], 'table': tab.split('.')[1]}
    for ip in db_ck['shards']:
        if check_ck_tab_exists(cfg,db_ck['shards'][ip],event) and check_ck_tab_repeat_data(cfg,db_ck['shards'][ip],event) >0:
           logging.info('delete local table {}.{} repeat data!'.format(get_ck_schema(cfg, event), event['table']))
           optimize_table(cfg,db_ck['shards'][ip],event)

def get_tasks(cfg):
    db = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                           cfg['db_mysql_port_log'],
                           cfg['log_db_name'],
                           cfg['db_mysql_user_log'],
                           cfg['db_mysql_pass_log'])
    cr = db.cursor()
    st = """SELECT sync_table,count(0) as amount 
             FROM (select * from t_db_sync_log  WHERE  status='0' and sync_tag='{}' limit 100000) as x 
               WHERE  instr('{}',x.sync_table)>0 
                 GROUP BY x.sync_table limit {}""".format(cfg['exec_tag'],cfg['sync_table'],cfg['process_num'])
    cr.execute(st)
    rs =cr.fetchall()
    return  rs

def start_sync(cfg):
    apply_time = datetime.datetime.now()
    sleep_time = datetime.datetime.now()
    flag = read_real_sync_status()
    with ProcessPoolExecutor(max_workers=cfg['process_num']) as executor:
        while True:
            if not flag is None:
                if  flag['msg']['value'] == 'STOP':
                    logging.info("\033[1;37;40m execute log task {} terminate!\033[0m".format(cfg['sync_tag']))
                    sys.exit(0)

            if get_seconds(apply_time) >= cfg['apply_timeout']:
               apply_time = datetime.datetime.now()
               cfg = get_config_from_db(cfg['sync_tag'])
               flag = read_real_sync_status()
               logging.info("\033[1;36;40m apply config success\033[0m")
               if cfg is None:
                   logging.info('load config failure,exit sync!')
                   sys.exit(0)

            tasks = get_tasks(cfg)
            if tasks!=():
                logging.info('检测到新事件：')
                logging.info('-'.ljust(85, '-'))
                for task in tasks:
                    logging.info(' '.ljust(3, ' ') + task['sync_table'].ljust(50, ' ') + ' = '+ str(task['amount']))
                logging.info('-'.ljust(85, '-'))

                for k,v in get_ck_async_task(cfg).items():
                    logging.info("ck async task amount:{} for shard:{}".format(str(v),k))
                    if round(v / 300)>=1:
                       logging.info("\033[1;36;40m sleep {}s wait ck async task for shard {}!\033[0m".format(round(v / 500)*3),k)
                       time.sleep(round(v / 300)*6)

                all_task = [executor.submit(write_ck_multi,cfg,t['sync_table']) for t in tasks]
                for future in as_completed(all_task):
                    res = future.result()
                    if res is not None:
                       logging.info(res)
            else:
               if get_seconds(sleep_time) % cfg['apply_timeout'] == 0:
                  logging.info('\r未检测到任务，休眠中:{}s ...'.format(str(get_seconds(sleep_time))))
                  sleep_time = datetime.datetime.now()
                  time.sleep(1)

def get_task_status(cfg):
    if check_pid(cfg):
        pid = read_pid(cfg).get('pid')
        if pid :
            if pid == os.getpid():
                return False

            if int(pid) in psutil.pids():
              return True
            else:
              return False
        else:
            return False
    else:
        return False

def write_pid(cfg):
    ck = {
       'pid':os.getpid()
    }
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'w') as f:
        f.write(json.dumps(ck, ensure_ascii=False, indent=4, separators=(',', ':')))
    logging.info('write pid:{}'.format(ck['pid']))

def check_pid(cfg):
    return os.path.isfile('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']))

def read_pid(cfg):
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        ckpt = json.loads(contents)
        return ckpt

def read_real_sync_status():
    try:
        url = 'http://$$API_SERVER$$/get_real_sync_status'
        res = requests.post(url,timeout=3).json()
        return res
    except:
        logging.info('read_real_sync_status failure!')
        return None

def get_sync_table_pk_names(cfg,db,event):
    st="""select name  from system.columns where database='{}' and table='{}' and is_in_primary_key=1 order by position"""\
        .format(get_ck_schema(cfg, event),event['table'])
    rs = db.execute(st)
    v_col = ''
    for i in list(rs):
        v_col=v_col+i[0]+','
    return v_col[0:-1]

def check_ck_tab_repeat_data(cfg,db,event):
   pk = get_sync_table_pk_names(cfg,db,event)
   st="""select count(0) from (select  {} from {}.{} group by {} having count(0)>1)""".\
          format(pk,get_ck_schema(cfg, event),event['table'],pk)
   rs = db.execute(st)
   return rs[0][0]

def optimize_table(cfg,db,event):
    if check_ck_tab_exists(cfg,db,event) >0:
        st ='''optimize table {}.{} final'''.format(get_ck_schema(cfg, event),event['table'])
        logging.info(st)
        db.execute(st)
        logging.info('\033[0;31;40moptimize clickhouse table {}.{} complete!\033[0m'.format(get_ck_schema(cfg, event),event['table']))

if __name__=="__main__":
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    # query system parameters to determine whether to run the  program
    if read_real_sync_status() == None or read_real_sync_status()['msg']['value'] == 'STOP':
        print("\033[1;37;40m Running execute log task {} failure!\033[0m".format(tag))
        sys.exit(0)


    # init logger
    logging.basicConfig(filename='/tmp/{}.{}.log'.format(tag, datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    # call api get config
    cfg = get_config_from_db(tag)

    # check task
    if not get_task_status(cfg):
       write_pid(cfg)
       print_dict(cfg)
       try:
         start_sync(cfg)
       except:
         logging.info(traceback.print_exc())
    else:
       logging.info('sync program:{} is running!'.format(tag))
