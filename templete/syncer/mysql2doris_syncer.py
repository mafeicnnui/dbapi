#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/10/22 8:52
# @Author : ma.fei
# @File : mysql2doris_syncer.py.py
# @Software: PyCharm

import sys
import time
import json
import pymysql
import re
import os
import traceback
import logging
import requests
import warnings
import threading
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import *
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)

META_DS = {
    "db_ip"        : "10.2.39.18",
    "db_port"      : "3306",
    "db_user"      : "puppet",
    "db_pass"      : "Puppet@123",
    "db_service"   : "puppet",
    "db_charset"   : "utf8"
}

META_DB = pymysql.connect(
    host       = META_DS['db_ip'],
    port       = int(META_DS['db_port']),
    user       = META_DS['db_user'],
    passwd     = META_DS['db_pass'],
    db         = META_DS['db_service'],
    charset    = META_DS['db_charset'],
    autocommit = True,
    cursorclass= pymysql.cursors.DictCursor)

DORIS_TAB_CONFIG = '''ENGINE=OLAP
    UNIQUE KEY($$PK_NAMES$$)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH($$PK_NAMES$$) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
)
'''

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_ds_by_dsid(p_dsid):
    sql="""select cast(id as char) as dsid,
                  db_type,
                  db_desc,
                  ip,
                  port,
                  service,
                  user,
                  password,
                  status,
                  date_format(creation_date,'%Y-%m-%d %H:%i:%s') as creation_date,
                  creator,
                  date_format(last_update_date,'%Y-%m-%d %H:%i:%s') as last_update_date,
                  updator ,
                  db_env,
                  inst_type,
                  market_id,
                  proxy_status,
                  proxy_server,
                  id_ro
           from t_db_source where id={0}""".format(p_dsid)
    cur = META_DB.cursor()
    cur.execute(sql)
    ds = cur.fetchone()
    ds['password'] = aes_decrypt(ds['password'],ds['user'])
    return ds

def get_obj_op(p_sql):
    if re.split(r'\s+', p_sql)[0].upper() in('CREATE','DROP') and re.split(r'\s+', p_sql)[1].upper() in('TABLE','INDEX','DATABASE'):
       return re.split(r'\s+', p_sql)[0].upper()+'_'+re.split(r'\s+', p_sql)[1].upper()
    if re.split(r'\s+', p_sql)[0].upper() in('TRUNCATE'):
       return 'TRUNCATE_TABLE'
    if re.split(r'\s+', p_sql)[0].upper()== 'ALTER' and re.split(r'\s+', p_sql)[1].upper()=='TABLE' and  re.split(r'\s+', p_sql)[3].upper() in('ADD','DROP','MODIFY'):
       return re.split(r'\s+', p_sql)[0].upper()+'_'+re.split(r'\s+', p_sql)[1].upper()+'_'+re.split(r'\s+', p_sql)[3].upper()
    if re.split(r'\s+', p_sql)[0].upper() in('INSERT','UPDATE','DELETE') :
       return re.split(r'\s+', p_sql)[0].upper()

def get_obj_name(p_sql):
    if p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("TABLE") > 0 \
        or p_sql.upper().count("TRUNCATE") > 0 and p_sql.upper().count("TABLE") > 0 \
         or p_sql.upper().count("ALTER") > 0 and p_sql.upper().count("TABLE") > 0 \
           or p_sql.upper().count("DROP") > 0 and p_sql.upper().count("TABLE") > 0 \
             or p_sql.upper().count("DROP") > 0 and p_sql.upper().count("DATABASE") > 0 \
                or  p_sql.upper().count("CREATE")>0 and p_sql.upper().count("VIEW")>0 \
                   or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("FUNCTION") > 0 \
                    or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("PROCEDURE") > 0 \
                      or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("INDEX") > 0 \
                        or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("TRIGGER") > 0  \
                           or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("DATABASE") > 0:

       if p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("INDEX") > 0 and p_sql.upper().count("UNIQUE") > 0:
           obj = re.split(r'\s+', p_sql)[3].replace('`', '')
       else:
           obj=re.split(r'\s+', p_sql)[2].replace('`', '')

       if ('(') in obj:
           if obj.find('.')<0:
              return obj.split('(')[0]
           else:
              return obj.split('(')[0].split('.')[1]
       else:
           if obj.find('.') < 0:
              return obj
           else:
              return obj.split('.')[1]

    if get_obj_op(p_sql) in('INSERT','DELETE'):
         if re.split(r'\s+', p_sql.strip())[2].split('(')[0].strip().replace('`','').find('.')<0:
            return  re.split(r'\s+', p_sql.strip())[2].split('(')[0].strip().replace('`','')
         else:
            return re.split(r'\s+', p_sql.strip())[2].split('(')[0].strip().replace('`', '').split('.')[1]

    if get_obj_op(p_sql) in('UPDATE'):
        if re.split(r'\s+', p_sql.strip())[1].split('(')[0].strip().replace('`','').find('.')<0:
           return re.split(r'\s+', p_sql.strip())[1].split('(')[0].strip().replace('`','')
        else:
           return re.split(r'\s+', p_sql.strip())[1].split('(')[0].strip().replace('`', '').split('.')[1]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_db(MYSQL_SETTINGS):
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',autocommit=True)
    return conn

def get_db_by_ds(p_ds):
    conn = pymysql.connect(host=p_ds['ip'],
                           port=int(p_ds['port']),
                           user=p_ds['user'],
                           passwd=p_ds['password'],
                           charset='utf8',autocommit=True)
    return conn

def get_doris_db(p_ds,p_db):
    conn = pymysql.connect(host=p_ds['ip'],
                           port=int(p_ds['port']),
                           user=p_ds['user'],
                           passwd=p_ds['password'],
                           db=p_db
                          )
    return conn

def get_doris_table_defi(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    st = """SELECT  `column_name`,data_type
              FROM information_schema.columns
              WHERE table_schema='{}'
                AND table_name='{}'  ORDER BY ordinal_position""".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    st= 'create table `{}`.`{}` (\n '.format(get_doris_schema(cfg,event),event['table'])
    for i in rs:
        if i[1] == 'varchar':
           st =  st + ' `{}`  String,\n'.format(i[0])
        elif i[1] == 'timestamp':
           st = st + ' `{}`  datetime,\n'.format(i[0])
        elif i[1] == 'longtext':
            st = st + ' `{}`  text,\n'.format(i[0])
        else:
           st = st + '  `{}`  {},\n'.format(i[0],i[1])
    db.commit()
    cr.close()
    st = st[0:-2]+') \n' + cfg['doris_config']
    return st

def check_doris_tab_exists(cfg,event):
   db=cfg['db_doris']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema='{}' and table_name='{}'""".format(get_doris_schema(cfg,event),event['table'])
   cr.execute(sql)
   rs=cr.fetchone()
   db.commit()
   cr.close()
   return rs[0]

def check_tab_exists_pk(cfg,event):
   db = cfg['db_mysql']
   cr = db.cursor()
   st = """select count(0) from information_schema.columns
              where table_schema='{}' and table_name='{}' and column_key='PRI'""".format(event['schema'],event['table'])
   cr.execute(st)
   rs=cr.fetchone()
   cr.close()
   return rs[0]

def get_table_pk_names(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    v_col=''
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' and column_key='PRI' order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in list(rs):
        v_col = v_col + '`{}`,'.format(i[0])
    cr.close()
    return v_col[0:-1]

def create_doris_table(cfg,event):
    db = cfg['db_doris']
    cr = db.cursor()
    if check_tab_exists_pk(cfg,event) >0:
        st = get_doris_table_defi(cfg,event)
        cr.execute(st.replace('$$PK_NAMES$$',get_table_pk_names(cfg,event)))
        time.sleep(0.1)
        db.commit()
        cr.close()
        print('create doris table `{}` success!'.format(cfg['sync_table']))
    else:
        print('Table `{}` have no primary key!'.format(cfg['sync_table']))
        sys.exit(0)

def set_column(p_data,p_pk):
    v_set = ' set '
    for key in p_data:
        if p_data[key] is None:
           v_set = v_set + key + '=null,'
        else:
           if p_pk.count(key)==0:
              v_set = v_set + key + '=\''+ str(p_data[key]) + '\','
    return v_set[0:-1]

def get_doris_schema(cfg,event):
    for o in cfg['sync_table'].split(','):
      mysql_schema = o.split('.')[0]
      mysql_table  = o.split('$')[0].split('.')[1]
      doris_schema = o.split('$')[1]
      if event['schema'] == mysql_schema and event['table'] == mysql_table:
         return  doris_schema
    return 'test'

def get_ins_header(cfg,event):
    v_ddl = 'insert into {0}.{1} ('.format(get_doris_schema(cfg,event), event['table'])
    if event['action'] == 'insert':
        for key in event['data']:
            v_ddl = v_ddl + '`{0}`'.format(key) + ','
        v_ddl = v_ddl[0:-1] + ')'
    elif event['action'] == 'update':
        for key in event['after_values']:
            v_ddl = v_ddl + '`{0}`'.format(key) + ','
        v_ddl = v_ddl[0:-1] + ')'
    return v_ddl

def get_ins_values(event):
    v_tmp=''
    if event['action'] == 'insert':
        for key in event['data']:
            if event['data'][key]==None:
               v_tmp=v_tmp+"null,"
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['data'][key])) + "',"
    elif  event['action'] == 'update':
        for key in event['after_values']:
            if event['after_values'][key]==None:
               v_tmp=v_tmp+"null,"
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['after_values'][key])) + "',"
    return v_tmp[0:-1]

def get_where(cfg,event):
    cols = get_table_pk_names( cfg,event)
    v_where = ' where '
    for key in event['data']:
        if check_tab_exists_pk( cfg,event) > 0:
            if key in cols:
                v_where = v_where + key + ' = \'' + str(event['data'][key]) + '\' and '
        else:
           v_where = v_where+ key+' = \''+str(event['data'][key]) + '\' and '
    return v_where[0:-5]

def gen_sql(cfg,event):
    if event['action'] in ('insert','update'):
        sql  = get_ins_header(cfg,event)+ ' values ('+get_ins_values(event)+');'
    elif event['action']=='delete':
        sql  = 'delete from {0}.{1} {2}'.format(get_doris_schema(cfg,event),event['table'],get_where(cfg,event))
    return sql

def gen_ddl_sql(p_ddl):
    if p_ddl.find('create table')>=0:
       return p_ddl
    else:
       return None

def get_file_and_pos(p_db):
    cr = p_db.cursor()
    cr.execute('show master status')
    ds = cr.fetchone()
    return ds

def get_binlog_files(p_db):
    cr = p_db.cursor()
    cr.execute('show binary logs')
    files = []
    rs = cr.fetchall()
    for r in rs:
        files.append(r[0])
    return files


def merge_insert(data):
    header = data[0]['sql'].split(' values ')[0]
    body = ''
    for d in data:
        body = body +d['sql'].split(' values ')[1][0:-1]+','
    sql = header+' values '+body[0:-1]
    return {'event':'insert','sql': sql ,'amount':len(data)}

def process_batch(batch):
    nbatch = {}
    insert = []
    for tab in batch:
        flag = False
        nbatch[tab] = []
        for st in batch[tab]:
            if st['event']  == 'insert':
                  insert.append(st)

            if st['event'] == 'delete':
                   if len(insert)>0:
                      nbatch[tab].append(merge_insert(insert))
                      insert = []
                   nbatch[tab].append(st)
                   flag = True
        if not flag and insert!=[]:
           nbatch[tab].append(merge_insert(insert))
           insert = []
    return nbatch

def doris_exec_multi(cfg,batch,flag='N'):
    nbatch = process_batch(batch)
    exec_threading(cfg,nbatch,flag)

def exec_threading(cfg,nbatch,flag):
    threads = []
    for tab in nbatch:
        if len(nbatch[tab]) > 0:
           print('start threading for {}，flag={}...'.format(tab,flag))
           thread = threading.Thread(target=exec_sql, args=(cfg, tab,nbatch[tab],flag,))
           threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()

    for i in range(0, len(threads)):
        threads[i].join()


def exec_sql(cfg,tab,tab_batch,flag):
    db = cfg['db_doris']
    cr = db.cursor()
    if flag == 'F':
        if len(tab_batch) > 0:
           print('exec {} nbatch  for {}...'.format(len(tab_batch), tab))
           for st in tab_batch:
               if len(tab_batch) > 0 and st['amount'] % cfg['batch_size'] == 0:
                  event = {'schema': tab.split('.')[0], 'table': tab.split('.')[1]}
                  if check_doris_tab_exists(cfg, event) == 0:
                     print('create doris table {}.{} success!'.format(get_doris_schema(cfg, event), event['table']))
                     create_doris_table(cfg, event)
                  start_time = datetime.datetime.now()
                  cr.execute(st['sql'])
                  cr.close()
                  print('Table:{}, multi rec:{},time:{}s'.format(tab,st['amount'], get_seconds(start_time)))
                  write_ckpt(cfg)
                  time.sleep(config['sleep_time'])
    else:
        if len(tab_batch) > 0:
            print('exec {} nbatch for {}'.format(len(tab_batch), tab))
            for st in tab_batch:
                event = {'schema': tab.split('.')[0], 'table': tab.split('.')[1]}
                if check_doris_tab_exists(cfg, event) == 0:
                    print('create doris table {}.{} success!'.format(get_doris_schema(cfg, event), event['table']))
                    create_doris_table(cfg, event)
                start_time = datetime.datetime.now()
                print('Table:{}, multi rec:{},time:{}s'.format(tab,st['amount'], get_seconds(start_time)))
                cr.execute(st['sql'])
                cr.close()
                write_ckpt(cfg)
                time.sleep(config['sleep_time'])

def doris_exec(cfg,batch,flag='N'):
    db = cfg ['db_doris']
    cr = db.cursor()
    nbatch = process_batch(batch)
    for tab in nbatch:
        if flag =='F':
            print('exec nbatch {} for {}'.format(len(nbatch[tab]),tab))
            for st in nbatch[tab]:
                if len(nbatch[tab])>0 and len(batch[tab]) % cfg['batch_size'] == 0:
                    event = {'schema':tab.split('.')[0],'table':tab.split('.')[1]}
                    if check_doris_tab_exists(cfg, event) == 0:
                       print('create doris table {}.{} success!'.format(get_doris_schema(cfg,event),event['table']))
                       create_doris_table(cfg, event)
                    start_time = datetime.datetime.now()
                    cr.execute(st['sql'])
                    print('multi rec:{},time:{}s'.format(st['amount'], get_seconds(start_time)))
                    write_ckpt(cfg)
                    time.sleep(config['sleep_time'])

        else:
            if len(nbatch[tab])>0:
                print('exec nbatch {} for {}'.format(len(nbatch[tab]),tab))
                for st in nbatch[tab]:
                    event = {'schema':tab.split('.')[0],'table':tab.split('.')[1]}
                    if check_doris_tab_exists(cfg, event) == 0:
                        print('create doris table {}.{} success!'.format(get_doris_schema(cfg,event), event['table']))
                        create_doris_table(cfg, event)
                    start_time = datetime.datetime.now()
                    print('multi rec:{},time:{}s'.format(st['amount'], get_seconds(start_time)))
                    cr.execute(st['sql'])
                    write_ckpt(cfg)
                    time.sleep(config['sleep_time'])

def check_sync(cfg,event):
    res = False
    for o in cfg['sync_table'].split(','):
        schema,table = o.split('$')[0].split('.')
        if event['schema'] == schema  and  event['table'] == table:
            res = True
    return res

def check_batch_exist_data(batch):
    for k in batch:
        if len(batch[k])>0:
           return True
    return False

def check_batch_full_data(batch,cfg):
    for k in batch:
        if len(batch[k])>0 and len(batch[k]) % cfg['batch_size'] == 0:
           return True
    return False

def write_ckpt(cfg):
    print('write_ckpt file:{}'.format(cfg['binlogfile']))
    print('write_ckpt pos:{}'.format(cfg['binlogpos']))
    ckpt = {
        'binlogfile':cfg['binlogfile'],
        'binlogpos':cfg['binlogpos']
    }
    with open('mysqlbinlog.json', 'w') as f:
        f.write(json.dumps(ckpt, ensure_ascii=False, indent=4, separators=(',', ':')))

def check_ckpt():
    return os.path.isfile('mysqlbinlog.json')


def read_ckpt():
    with open('mysqlbinlog.json', 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        binlog = json.loads(contents)
        file = binlog['binlogfile']
        pos = binlog['binlogpos']
        return file,pos

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8mb4')
    return conn

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par,timeout=1).json()
        if res['code'] == 200:
            config = res['msg']
            return config
        else:
            print('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        print('aes_decrypt api not available!')


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
        db_doris_ip                      = config['sync_db_dest'].split(':')[0]
        db_doris_port                    = config['sync_db_dest'].split(':')[1]
        db_doris_service                 = config['sync_db_dest'].split(':')[2]
        db_doris_user                    = config['sync_db_dest'].split(':')[3]
        db_doris_pass                    = aes_decrypt(config['sync_db_dest'].split(':')[4],db_doris_user)
        config['db_mysql_ip']            = db_mysql_ip
        config['db_mysql_port']          = db_mysql_port
        config['db_mysql_service']       = db_mysql_service
        config['db_mysql_user']          = db_mysql_user
        config['db_mysql_pass']          = db_mysql_pass
        config['db_doris_ip']            = db_doris_ip
        config['db_doris_port']          = db_doris_port
        config['db_doris_service']       = db_doris_service
        config['db_doris_user']          = db_doris_user
        config['db_doris_pass']          = db_doris_pass
        config['db_mysql_string']        = db_mysql_ip + ':' + db_mysql_port + '/' + db_mysql_service
        config['db_doris_string']        = db_doris_ip + ':' + db_doris_port + '/' + db_doris_service
        config['db_mysql']               = get_ds_mysql(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)
        config['db_doris']               = get_ds_mysql(db_doris_ip, db_doris_port, db_doris_service, db_doris_user, db_doris_pass)

        if check_ckpt():
            file, pos = read_ckpt()
            if file not in get_binlog_files(config['db_mysql']):
               file, pos = get_file_and_pos(config['db_mysql'])[0:2]
               print('from mysql database read binlog...')
            else:
               print('from mysqlbinlog.json read ckpt...')
        else:
            file, pos = get_file_and_pos(config['db_mysql'])[0:2]
            print('from mysql database read binlog...')

        config['binlogfile']            = file
        config['binlogpos']             = pos
        config['doris_config']          = DORIS_TAB_CONFIG
        config['batch_size']            = config['batch_size_incr']
        config['sleep_time']            = int(config['sync_gap'])
        return config
    else:
        print('load config failure:{0}'.format(res['msg']))
        return None


'''
   检查点：
    1.事件缓存batch[tab]列表长度达到 batch_size 时
    2.非同步表的数据库行事件达到100个
    3.上一次执行后，缓存未满，达到超时时间                        
'''
def start_syncer(cfg):

    MYSQL_SETTINGS = {
        "host"   : cfg['db_mysql_ip'],
        "port"   : int(cfg['db_mysql_port']),
        "user"   : "canal2021",
        "passwd" : "canal@Hopson2018",
    }

    logging.info("MYSQL_SETTINGS=",MYSQL_SETTINGS)
    batch = {}
    row_event_count = 0

    for o in cfg['sync_table'].split(','):
        batch[o.split('$')[0]] = []

    try:
        stream = BinLogStreamReader(
            connection_settings = MYSQL_SETTINGS,
            only_events         = (QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
            server_id           = 9999,
            blocking            = True,
            resume_stream       = True,
            log_file            = cfg['binlogfile'],
            log_pos             = int(cfg['binlogpos']))

        print('\nSync Configuration:')
        print('-------------------------------------------------------------')
        print('batch_size=',cfg['batch_size'])
        print('batch_timeout=',cfg['batch_timeout'])
        print('batch_row_event=',cfg['batch_row_event'])
        print('apply_timeout=', cfg['apply_timeout'])
        print('sleep_time=', cfg['sleep_time'])
        print('')

        start_time = datetime.datetime.now()
        apply_time = datetime.datetime.now()

        for binlogevent in stream:

            if get_seconds(apply_time) >= config['apply_timeout']:
               cfg['db_mysql'].close()
               cfg['db_doris'].close()
               cfg = get_config_from_db(cfg['sync_tag'])
               apply_time = datetime.datetime.now()
               print("\033[0;31;40mapply config success\033[0m")

            for o in cfg['sync_table'].split(','):
               if batch.get(o.split('$')[0]) is None:
                  batch[o.split('$')[0]] = []
                  print("\033[0;31;40mbatch['{}'] init success!\033[0m".format(o.split('$')[0]))

            if isinstance(binlogevent, RotateEvent):
                current_master_log_file = binlogevent.next_binlog
                print("Next binlog file: %s" ,current_master_log_file)
                cfg['binlogfile'] = current_master_log_file

            row_event_count = row_event_count + 1

            if isinstance(binlogevent, QueryEvent):
                cfg['binlogpos'] = binlogevent.packet.log_pos
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
                    ddl = gen_ddl_sql(event['query'])
                    event['table'] = get_obj_name(event['query']).lower()
                    if check_sync(cfg,event) and ddl is not None:
                       if check_doris_tab_exists(cfg,event) == 0:
                          create_doris_table(cfg,event)

            if isinstance(binlogevent, DeleteRowsEvent) or \
                    isinstance(binlogevent, UpdateRowsEvent) or \
                        isinstance(binlogevent, WriteRowsEvent):

                for row in binlogevent.rows:

                    cfg['binlogpos'] = binlogevent.packet.log_pos
                    event = {"schema": binlogevent.schema.lower(), "table": binlogevent.table.lower()}
                    if check_sync(cfg, event):

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"] = row["values"]
                            sql = gen_sql(cfg,event)
                            batch[event['schema']+'.'+event['table']].append({'event':'delete','sql':sql})

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["after_values"] = row["after_values"]
                            event["before_values"] = row["before_values"]
                            sql = gen_sql(cfg,event)
                            batch[event['schema']+'.'+event['table']].append({'event':'insert','sql':sql})

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"] = row["values"]
                            sql = gen_sql(cfg,event)
                            batch[event['schema']+'.'+event['table']].append({'event':'insert','sql':sql})


                        if check_batch_full_data(batch,cfg):
                           print("\033[0;31;40mexec full batch...\033[0m")
                           doris_exec_multi(cfg, batch,'F')
                           for o in cfg['sync_table'].split(','):
                               if len(batch[o.split('$')[0]]) % cfg['batch_size'] == 0:
                                   batch[o.split('$')[0]] = []
                           start_time = datetime.datetime.now()
                           row_event_count = 0

            if get_seconds(start_time) >= cfg['batch_timeout'] :
                if check_batch_exist_data(batch):
                    print("\033[0;31;40mtimoeout:{},start_time:{}\033[0m".format(get_seconds(start_time),start_time))
                    doris_exec_multi(cfg, batch)
                    for o in cfg['sync_table'].split(','):
                         batch[o.split('$')[0]]
                    start_time = datetime.datetime.now()
                    row_event_count = 0

            if  row_event_count>0 and row_event_count % cfg['batch_row_event'] == 0:
                if check_batch_exist_data(batch):
                    print("\033[0;31;40mrow_event_count={}\033[0m".format(row_event_count))
                    doris_exec_multi(cfg, batch)
                    for o in cfg['sync_table'].split(','):
                        batch[o.split('$')[0]]
                    start_time = datetime.datetime.now()
                    row_event_count = 0


    except Exception as e:
        traceback.print_exc()
        write_ckpt(cfg)
    finally:
        stream.close()


'''
  1.support single db multi table
  2.supprt multi db multi table ,exaple:db1.tab1,db2.tab2
  3.exec sucesss write binlog,exception write binlog
  4.support monitor db all tables(N),db.*
  5.first empty table support full table sync(N),before query get binlogfile and pos like mysqldump
    (1) afetr create table. 
    (2) empty table,no data 
    (3) get binlog ckpt
  
'''

if __name__ == "__main__":
    tag = ""
    debug = False
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    config = get_config_from_db(tag)
    print_dict(config)

    if config is None:
       print('load config faulure,exit sync!')
       sys.exit(0)

    start_syncer(config)

