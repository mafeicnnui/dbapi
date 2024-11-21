#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/10/22 8:52
# @Author : ma.fei
# @File : mysql2doris_syncer.py.py
# @Software: PyCharm

import sys
import time
import datetime
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
from clickhouse_driver import Client

CK_TAB_CONFIG = '''ENGINE = MergeTree()
   PRIMARY KEY ($$PK_NAMES$$)
   ORDER BY ($$PK_NAMES$$)
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

def log(msg):
    tm = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    print("""{} : {}""".format(tm,msg))

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

def get_ck_table_defi(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    st = """SELECT  `column_name`,data_type,is_nullable
              FROM information_schema.columns
              WHERE table_schema='{}'
                AND table_name='{}'  ORDER BY ordinal_position""".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    st= 'create table `{}`.`{}` (\n '.format(get_ck_schema(cfg,event),event['table'])
    for i in rs:
        if i[1] == 'tinyint':
            st = st + ' `{}` {},\n'.format(i[0],'Int16' if i[2]=='NO' else 'Nullable(Int16)')
        elif i[1] == 'int':
            st = st + ' `{}` {},\n'.format(i[0],'Int32' if i[2]=='NO' else 'Nullable(Int32)' )
        elif i[1] == 'bigint':
            st = st + ' `{}` {},\n'.format(i[0],'Int64' if i[2]=='NO' else 'Nullable(Int64)' )
        elif i[1] == 'varchar':
           st =  st + ' `{}` {},\n'.format(i[0],'String' if i[2]=='NO' else 'Nullable(String)' )
        elif i[1] =='timestamp' :
           st = st + ' `{}` {},\n'.format(i[0],'DateTime' if i[2]=='NO' else 'Nullable(DateTime)' )
        elif i[1] == 'datetime':
            st = st + ' `{}` {},\n'.format(i[0],'DateTime' if i[2]=='NO' else 'Nullable(DateTime)' )
        elif i[1] == 'date':
            st = st + ' `{}` {},\n'.format(i[0],'Date' if i[2]=='NO' else 'Nullable(Date)' )
        elif i[1] == 'text':
            st = st + ' `{}` {},\n'.format(i[0],'String' if i[2]=='NO' else 'Nullable(String)' )
        elif i[1] == 'longtext':
            st = st + ' `{}` {},\n'.format(i[0],'String' if i[2]=='NO' else 'Nullable(String)' )
        elif i[1] == 'float':
            st = st + ' `{}` {},\n'.format(i[0],'Float32' if i[2]=='NO' else 'Nullable(Float32)' )
        elif i[1] == 'double':
            st = st + ' `{}` {},\n'.format(i[0],'Float64' if i[2]=='NO' else 'Nullable(Float64)' )
        else:
            st = st + ' `{}` {},\n'.format(i[0],'String' if i[2]=='NO' else 'Nullable(String)' )
    db.commit()
    cr.close()
    st = st[0:-2]+') \n' + cfg['ck_config']
    return st

def get_ck_table_defi_mysql(cfg,event):
    db = cfg['db_mysql']
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

def check_ck_tab_exists(cfg,event):
   db=cfg['db_ck']
   sql="""select count(0) from system.tables
            where database='{}' and name='{}'""".format(get_ck_schema(cfg,event),event['table'])
   rs = db.execute(sql)
   return rs[0][0]

def check_ck_database(cfg,event):
    db = cfg['db_ck']
    sql = """select count(0) from system.databases d 
                  where name ='{}'""".format(get_ck_schema(cfg, event))
    rs = db.execute(sql)
    return rs[0][0]

def check_ck_tab_exists_data(cfg,event):
   db = cfg['db_ck']
   st = "select count(0) from {}.{}".format(get_ck_schema(cfg,event),event['table'])
   rs = db.execute(st)
   return rs[0][0]

def check_ck_tab_exists_by_param(cfg,event):
   db=cfg['db_ck']
   sql="""select count(0) from system.tables
            where database='{}' and name='{}'""".format(event['schema'],event['table'])
   rs = db.execute(sql)
   return rs[0][0]

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

def create_ck_table(cfg,event):
    db = cfg['db_ck']
    if check_tab_exists_pk(cfg,event) >0:
        if check_ck_database(cfg,event) == 0:
           db.execute('create database {}'.format(get_ck_schema(cfg, event)))
           log('\033[0;36;40mclickhouse => create  database `{}` success!\033[0m'.format(get_ck_schema(cfg, event)))

        st = get_ck_table_defi(cfg,event)
        db.execute(st.replace('$$PK_NAMES$$',get_table_pk_names(cfg,event)))
        time.sleep(0.1)
        log('\033[0;36;40mclickhouse => create  table `{}.{}` success!\033[0m'.format(get_ck_schema(cfg, event),event['table']))
    else:
        log('Table `{}` have no primary key,exit sync!'.format(event['table']))
        sys.exit(0)

def optimize_table(cfg,event):
    db = cfg['db_ck']
    if check_ck_tab_exists_by_param(cfg,event) >0:
        st = '''optimize table {}.{} final'''.format(event['schema'],event['table'])
        db.execute(st)
        log('\033[0;31;40moptimize clickhouse table {}.{} complete!\033[0m'.format(get_ck_schema(cfg, event),event['table']))

def full_sync(cfg,event):
    db = cfg['db_ck']
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

def get_cols_from_mysql(cfg,event):
    db = cfg['db_mysql']
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

def set_column(p_data,p_pk,p_typ):
    v_set = ' '
    for key in p_data:
        if p_data[key] is None:
           v_set = v_set + key + '=null,'
        else:
           if p_pk.count(key)==0:
               if p_typ[key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                   v_set = v_set + key + '='+ format_sql(str(p_data[key])) + ','
               else:
                   v_set = v_set + key + '=\''+ format_sql(str(p_data[key])) + '\','
    return v_set[0:-1]

def get_ck_schema(cfg,event):
    for o in cfg['sync_table'].split(','):
      mysql_schema = o.split('.')[0]
      mysql_table  = o.split('$')[0].split('.')[1]
      ck_schema    = o.split('$')[1] if o.split('$')[1] !='auto' else mysql_schema
      if event['schema'] == mysql_schema and event['table'] == mysql_table:
         return  cfg['desc_db_prefix']+ck_schema
    return 'test'

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

def get_col_type(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    st = """SELECT  `column_name`,`data_type`
                FROM information_schema.columns
                WHERE table_schema='{}'
                  AND table_name='{}' 
                  """.format(event['schema'], event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    cols = {}
    for i in rs:
      cols[i[0]] = i[1]
    return cols

def get_ins_values(event,typ):
    v_tmp=''
    if event['action'] == 'insert':
        for key in event['data']:
            if event['data'][key]==None:
               v_tmp=v_tmp+"null,"
            elif typ[key] in('tinyint','int','bigint','float','double'):
               v_tmp = v_tmp +  str(event['data'][key]) + ","
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['data'][key])) + "',"
    elif  event['action'] == 'update':
        for key in event['after_values']:
            if event['after_values'][key]==None:
               v_tmp=v_tmp+"null,"
            elif typ[key] in('tinyint','int','bigint','float','double'):
               v_tmp = v_tmp +  str(event['after_values'][key]) + ","
            else:
               v_tmp = v_tmp + "'" + format_sql(str(event['after_values'][key])) + "',"
    return v_tmp[0:-1]

def get_where(cfg,event,typ):
    cols = get_table_pk_names( cfg,event).replace('`','').split(',')
    v_where = ' where '
    if event['action'] == 'delete':
        for key in event['data']:
            if check_tab_exists_pk( cfg,event) > 0:
                if key in cols:
                    if typ[key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                       v_where = v_where + key + ' = ' + str(event['data'][key]) + ' and '
                    else:
                       v_where = v_where + key + ' = \'' + str(event['data'][key]) + '\' and '
            else:
               v_where = v_where+ key+' = \''+str(event['data'][key]) + '\' and '
    elif event['action'] == 'update':
        for key in event['after_values']:
            if check_tab_exists_pk(cfg,event) > 0:
                if key in cols:
                    if typ[key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                       v_where = v_where + key + ' = ' + str(event['after_values'][key]) + ' and '
                    else:
                       v_where = v_where + key + ' = \'' + str(event['after_values'][key]) + '\' and '
            else:
               v_where = v_where+ key+' = \''+str(event['after_values'][key]) + '\' and '
    return v_where[0:-5]

def gen_sql(cfg,event,typ):
    if event['action'] in ('insert'):
        sql  = get_ins_header(cfg,event)+ ' values ('+get_ins_values(event,typ)+');'
    elif event['action'] == 'update':
        sql = 'alter table {0}.{1} update {2} {3}'.\
              format(get_ck_schema(cfg, event),event['table'],
                     set_column(event['after_values'],get_table_pk_names(cfg,event),typ),
                     get_where(cfg, event,typ))
    elif event['action']=='delete':
        sql  = 'alter table {0}.{1} delete {2}'.format(get_ck_schema(cfg,event),event['table'],get_where(cfg,event,typ))
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

def merge_delete(data):
    pass

def process_batch(batch):
    # delete [] batch
    n_batch = {}
    for tab in batch:
        if batch[tab] != []:
           n_batch[tab] = batch[tab]
    batch = n_batch

    nbatch = {}
    insert = []
    for tab in batch:
        nbatch[tab] = []
        for st in batch[tab]:
            if st['event']  == 'insert':
               insert.append(st)
            else:
               if len(insert)>0:
                  nbatch[tab].append(merge_insert(insert))
                  insert = []
               st['amount'] = 1
               nbatch[tab].append(st)

        if insert!=[]:
           nbatch[tab].append(merge_insert(insert))
           insert = []
    return nbatch

def process_batch_n(batch):
    n_batch = {}
    for tab in batch:
        if batch[tab] != []:
           n_batch[tab] = batch[tab]
    batch = n_batch

    nbatch = {}
    insert = []
    delete = {}
    for tab in batch:
        nbatch[tab] = []
        for st in batch[tab]:

            if st['event']  == 'insert':
               if len(delete) > 0:
                   nbatch[tab].append(merge_delete(delete))
                   delete = []
               insert.append(st)

            if st['event'] == 'delete':
               if len(insert) > 0:
                   nbatch[tab].append(merge_insert(insert))
                   insert = []
               if len(get_table_pk_names(cfg, st['event']).split(',')) == 1:
                   delete.append(st)

            if st['event'] == 'update':
                if len(delete) > 0:
                    nbatch[tab].append(merge_delete(delete))
                    delete = []

                if len(insert) > 0:
                    nbatch[tab].append(merge_insert(insert))
                    insert = []

                st['amount'] = 1
                nbatch[tab].append(st)

        if insert!=[]:
           nbatch[tab].append(merge_insert(insert))
           insert = []

    return nbatch

def ck_exec_multi(cfg,batch,flag='part'):
    #nbatch = process_batch(batch)
    exec_threading(cfg,batch,flag)

def exec_threading(cfg,nbatch,flag):
    threads = []
    for tab in nbatch:
        if len(nbatch[tab]) > 0:
           log('\033[0;36;40mstart threading for {}，flag={}...\033[0m'.format(tab,flag))
           thread = threading.Thread(target=exec_sql, args=(cfg, tab,nbatch[tab],flag,))
           threads.append(thread)

    for i in range(0, len(threads)):
        threads[i].start()

    for i in range(0, len(threads)):
        threads[i].join()

def exec_sql(cfg,tab,tab_batch,flag):
    db = get_db_ck(cfg)
    if flag == 'F':
        if len(tab_batch) > 0:
           log('exec full {} nbatch  for {}...'.format(len(tab_batch), tab))
           for st in tab_batch:
               if len(tab_batch) > 0 and st['amount'] % cfg['batch_size'] == 0:
                  start_time = datetime.datetime.now()
                  try:
                      db.execute(st['sql'])
                      log('Table:{}, event:{},time:{}s'.format(tab,st['event'],get_seconds(start_time)))
                      #write_ckpt(cfg)
                      time.sleep(cfg['sleep_time'])
                  except:
                      print('tab_batch=',tab_batch)
                      print('exec=',st['sql'])
                      traceback.print_exc()

    else:
        if len(tab_batch) > 0:
            log('exec part {} nbatch for {}'.format(len(tab_batch), tab))
            for st in tab_batch:
                start_time = datetime.datetime.now()
                log('Table:{}, event:{},time:{}s'.format(tab,st['event'], get_seconds(start_time)))
                try:
                    db.execute(st['sql'])
                    #write_ckpt(cfg)
                    time.sleep(cfg['sleep_time'])
                except:
                    print('tab_batch=', tab_batch)
                    print('exec=', st['sql'])
                    traceback.print_exc()

def ck_exec(cfg,batch,flag='N'):
    db = cfg ['db_ck']
    #nbatch = process_batch(batch)
    #nbatch = batch
    for tab in batch:
        if flag =='F':
            log('exec nbatch {} for {}'.format(len(batch[tab]),tab))
            for st in batch[tab]:
                if len(batch[tab])>0 and len(batch[tab]) % cfg['batch_size'] == 0:
                    db.execute(st['sql'])

        else:
            if len(batch[tab])>0:
                log('exec nbatch {} for {}'.format(len(batch[tab]),tab))
                for st in batch[tab]:
                    db.execute(st['sql'])


def check_sync(cfg,event,pks):
    res = False
    if pks.get(event['schema'] + '.' + event['table']) is None:
       return False
    for o in cfg['sync_table'].split(','):
        schema,table = o.split('$')[0].split('.')
        if pks[event['schema'] + '.' + event['table']] :
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
    ckpt = {
        'binlogfile':cfg['binlogfile'],
        'binlogpos':cfg['binlogpos']
    }
    with open('{}.json'.format(cfg['sync_tag']), 'w') as f:
        f.write(json.dumps(ckpt, ensure_ascii=False, indent=4, separators=(',', ':')))

def check_ckpt(cfg):
    return os.path.isfile('{}.json'.format(cfg['sync_tag']))

def read_ckpt(cfg):
    with open('{}.json'.format(cfg['sync_tag']), 'r') as f:
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

def get_ds_ck(ip,port,service ,user,password):
    return  Client(host=ip,
                   port=port,
                   user=user,
                   password=password,
                   database=service,
                   send_receive_timeout=600000)

def get_db_ck(cfg):
    return  Client(host=cfg['db_ck_ip'] ,
                   port=cfg['db_ck_port'] ,
                   user=cfg['db_ck_user'] ,
                   password=cfg['db_ck_pass'] ,
                   database=cfg['db_ck_service'] ,
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
            log('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        log('aes_decrypt api not available!')

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
        config['db_mysql']               = get_ds_mysql(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)
        config['db_ck']                  = get_ds_ck(db_ck_ip, db_ck_port, db_ck_service, db_ck_user, db_ck_pass)

        if config.get('ds_ro') is not None and config.get('ds_ro') != '':
            config['db_mysql_ip_ro']      = config['ds_ro']['ip']
            config['db_mysql_port_ro']    = config['ds_ro']['port']
            config['db_mysql_service_ro'] = config['ds_ro']['service']
            config['db_mysql_user_ro']    = config['ds_ro']['user']
            config['db_mysql_pass_ro']    = aes_decrypt(config['ds_ro']['password'],
                                                        config['ds_ro']['user'])
            config['db_mysql_ro']         = get_ds_mysql(config['db_mysql_ip_ro'] ,
                                                         config['db_mysql_port_ro'],
                                                         config['db_mysql_service_ro'],
                                                         config['db_mysql_user_ro'],
                                                         config['db_mysql_pass_ro'] )

        if check_ckpt(config):
            file, pos = read_ckpt(config)
            if file not in get_binlog_files(config['db_mysql']):
               file, pos = get_file_and_pos(config['db_mysql'])[0:2]
               log('from mysql database read binlog...')
            else:
               log('from mysqlbinlog.json read ckpt...')
        else:
            file, pos = get_file_and_pos(config['db_mysql'])[0:2]
            log('from mysql database read binlog...')

        config['binlogfile']            = file
        config['binlogpos']             = pos
        config['ck_config']             = CK_TAB_CONFIG
        config['batch_size']            = config['batch_size_incr']
        config['sleep_time']            = int(config['sync_gap'])

        config = get_sync_tables(config)

        return config
    else:
        log('load config failure:{0}'.format(res['msg']))
        return None

def get_tables(cfg,o):
    db  = cfg['db_mysql']
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


'''
   检查点：
    1.事件缓存batch[tab]列表长度达到 batch_size 时
    2.非同步表的数据库行事件达到100个
    3.上一次执行后，缓存未满，达到超时时间                        
'''
def start_incr_syncer(cfg):
    log("\033[0;36;40mstart incr sync...\033[0m")
    MYSQL_SETTINGS = {
        "host"   : cfg['db_mysql_ip'],
        "port"   : int(cfg['db_mysql_port']),
        "user"   : "canal2021",
        "passwd" : "canal@Hopson2018",
    }

    logging.info("MYSQL_SETTINGS=",MYSQL_SETTINGS)
    batch = {}
    types = {}
    pks   = {}
    row_event_count = 0

    for o in cfg['sync_table'].split(','):
        evt = {'schema':o.split('$')[0].split('.')[0],'table':o.split('$')[0].split('.')[1]}
        if check_tab_exists_pk(cfg, evt) > 0:
            batch[o.split('$')[0]] = []
            types[o.split('$')[0]] = get_col_type(cfg, evt)
            pks[o.split('$')[0]]   = True
        else:
            log("\033[0;31;40mTable:{}.{} not primary key,skip sync...\033[0m".format(evt['schema'],evt['table']))
            pks[o.split('$')[0]] = False

    try:
        stream = BinLogStreamReader(
            connection_settings = MYSQL_SETTINGS,
            only_events         = (QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
            server_id           = 9999,
            blocking            = True,
            resume_stream       = True,
            log_file            = cfg['binlogfile'],
            log_pos             = int(cfg['binlogpos']),
            auto_position       = False
        )

        start_time = datetime.datetime.now()
        apply_time = datetime.datetime.now()

        for binlogevent in stream:

            if get_seconds(apply_time) >= cfg['apply_timeout']:
               cfg = get_config_from_db(cfg['sync_tag'])
               apply_time = datetime.datetime.now()
               log("\033[1;36;40mapply config success\033[0m")
               write_ckpt(cfg)
               batch = { k : v  for k,v in batch.items()
                            if k in [ tab.split('$')[0] for tab in cfg['sync_table'].split(',')] }

            pks = {}
            for o in cfg['sync_table'].split(','):
                evt = {'schema': o.split('$')[0].split('.')[0], 'table': o.split('$')[0].split('.')[1]}
                if batch.get(o.split('$')[0]) is None:
                   if check_tab_exists_pk(cfg, evt) > 0 :
                      log("\033[0;36;40mfind table:{}.{} auto config sync...\033[0m".format(evt['schema'],evt['table']))
                      batch[o.split('$')[0]] = []
                      types[o.split('$')[0]] = get_col_type(cfg, evt)
                      pks[o.split('$')[0]] = True
                      if check_ck_tab_exists(cfg, evt) == 0:
                         create_ck_table(cfg, evt)
                         full_sync(cfg, evt)
                   else:
                      log("\033[0;36;40mTable:{}.{} not primary key,skip sync...\033[0m".format(evt['schema'],evt['table']))
                      pks[o.split('$')[0]] = False
                else:
                    if check_tab_exists_pk(cfg, evt) > 0:
                       pks[o.split('$')[0]] = True
                    else:
                       pks[o.split('$')[0]] = False


            if isinstance(binlogevent, RotateEvent):
                cfg['binlogfile'] = binlogevent.next_binlog

            row_event_count = row_event_count + 1

            if isinstance(binlogevent, QueryEvent):
                cfg['binlogpos'] = binlogevent.packet.log_pos
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
                    ddl = gen_ddl_sql(event['query'])
                    event['table'] = get_obj_name(event['query']).lower()

                    if check_sync(cfg,event,pks) and ddl is not None:
                       if check_ck_tab_exists(cfg,event) == 0:
                          create_ck_table(cfg,event)
                          full_sync(cfg,event)
                          batch[event['schema']+'.'+event['table']] = []
                          types[event['schema']+'.'+event['table']] = get_col_type(cfg, event)

            if isinstance(binlogevent, DeleteRowsEvent) or \
                    isinstance(binlogevent, UpdateRowsEvent) or \
                        isinstance(binlogevent, WriteRowsEvent):

                for row in binlogevent.rows:

                    cfg['binlogpos'] = binlogevent.packet.log_pos
                    event = {"schema": binlogevent.schema.lower(), "table": binlogevent.table.lower()}

                    if check_sync(cfg, event,pks):

                        typ = types[event['schema']+'.'+event['table']]

                        if check_ck_tab_exists(cfg, event) == 0:
                            create_ck_table(cfg, event)
                            full_sync(cfg, event)
                            batch[event['schema'] + '.' + event['table']] = []
                            types[event['schema'] + '.' + event['table']] = get_col_type(cfg, event)

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"] = row["values"]
                            sql = gen_sql(cfg,event,typ)
                            batch[event['schema']+'.'+event['table']].append({'event':'delete','sql':sql})

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["after_values"] = row["after_values"]
                            event["before_values"] = row["before_values"]
                            sql = gen_sql(cfg,event,typ)
                            batch[event['schema']+'.'+event['table']].append({'event':'update','sql':sql})

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"] = row["values"]
                            sql = gen_sql(cfg,event,typ)
                            batch[event['schema']+'.'+event['table']].append({'event':'insert','sql':sql})

                        if check_batch_full_data(batch,cfg):
                           log("\033[0;31;40mprocess full batch...\033[0m")
                           ck_exec(cfg, batch,'Full')
                           for o in cfg['sync_table'].split(','):
                               if len(batch[o.split('$')[0]]) % cfg['batch_size'] == 0:
                                   batch[o.split('$')[0]] = []
                           start_time = datetime.datetime.now()
                           row_event_count = 0
                           write_ckpt(cfg)



            if get_seconds(start_time) >= cfg['batch_timeout'] :
                if check_batch_exist_data(batch):
                    log("\033[0;31;40mtimoeout:{},start_time:{}\033[0m".format(get_seconds(start_time),start_time))
                    ck_exec(cfg, batch)
                    for o in cfg['sync_table'].split(','):
                         batch[o.split('$')[0]] = []
                    start_time = datetime.datetime.now()
                    row_event_count = 0
                    write_ckpt(cfg)

            if  row_event_count>0 and row_event_count % cfg['batch_row_event'] == 0:
                if check_batch_exist_data(batch):
                    log("\033[0;31;40mrow_event_count={}\033[0m".format(row_event_count))
                    ck_exec(cfg, batch)
                    for o in cfg['sync_table'].split(','):
                        batch[o.split('$')[0]] = []
                    start_time = datetime.datetime.now()
                    row_event_count = 0
                    write_ckpt(cfg)


    except Exception as e:
        traceback.print_exc()
        write_ckpt(cfg)
    finally:
        stream.close()

def start_full_sync(cfg):
    log("\033[0;36;40mstart full sync...\033[0m")
    for o in cfg['sync_table'].split(','):
        event = {'schema': o.split('$')[0].split('.')[0], 'table': o.split('$')[0].split('.')[1]}
        if check_tab_exists_pk(cfg,event) >0 and check_ck_tab_exists(cfg, event) == 0:
            create_ck_table(cfg, event)
            full_sync(cfg, event)
            write_ckpt(cfg)
            cfg = get_config_from_db(cfg['sync_tag'])

'''
  1.support single db multi table
  2.supprt multi db multi table ,exaple:db1.tab1,db2.tab2
  3.exec sucesss write binlog,exception write binlog
  4.support monitor db all tables(N),db.*
  5.first empty table support full table sync(N),before query get binlogfile and pos like mysqldump
    (1) afetr create table. 
    (2) empty table,no data 
    (3) get binlog ckpt
  6.mysql support   wildcard character(*)
  
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

    # call api get config
    cfg = get_config_from_db(tag)

    # print cfg
    print_dict(cfg)

    if cfg is None:
       log('load config faulure,exit sync!')
       sys.exit(0)

    # init go full sync
    start_full_sync(cfg)

    # parse binlog incr sync
    start_incr_syncer(cfg)