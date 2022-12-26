#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/10/22 8:52
# @Author : ma.fei
# @File : mysql2mysql_real_syncer_v3.py
# @Software: PyCharm
# @Func : mysql2mysql real sync based on the binlog

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
import decimal
import psutil
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import *
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, datetime.timedelta):
            return str(obj)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)

def print_dict(config):
    logging.info('-'.ljust(85, '-'))
    logging.info(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    logging.info('-'.ljust(85, '-'))
    for key in config:
        logging.info(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
    logging.info('-'.ljust(85, '-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_obj_op(p_sql):
    p_sql = p_sql.replace('\n',' ')
    if re.split(r'\s+', p_sql)[0].upper() in('CREATE','DROP') and re.split(r'\s+', p_sql)[1].upper() in('TABLE','INDEX','DATABASE'):
       return re.split(r'\s+', p_sql)[0].upper()+'_'+re.split(r'\s+', p_sql)[1].upper()
    if re.split(r'\s+', p_sql)[0].upper() in('TRUNCATE'):
       return 'TRUNCATE_TABLE'
    if re.split(r'\s+', p_sql)[0].upper()== 'ALTER' and re.split(r'\s+', p_sql)[1].upper()=='TABLE' and  re.split(r'\s+', p_sql)[3].upper() in('ADD','DROP','CHANGE'):
       return re.split(r'\s+', p_sql)[0].upper()+'_'+re.split(r'\s+', p_sql)[1].upper()+'_'+re.split(r'\s+', p_sql)[3].upper()
    if re.split(r'\s+', p_sql)[0].upper() in('INSERT','UPDATE','DELETE') :
       return re.split(r'\s+', p_sql)[0].upper()

def get_obj_name(p_sql):
    if p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("TABLE") > 0 \
        or p_sql.upper().count("TRUNCATE") > 0 \
         or p_sql.upper().count("ALTER") > 0 and p_sql.upper().count("TABLE") > 0 \
           or p_sql.upper().count("DROP") > 0 and p_sql.upper().count("TABLE") > 0 \
             or p_sql.upper().count("DROP") > 0 and p_sql.upper().count("DATABASE") > 0 \
                or  p_sql.upper().count("CREATE")>0 and p_sql.upper().count("VIEW")>0 \
                   or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("FUNCTION") > 0 \
                    or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("PROCEDURE") > 0 \
                      or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("INDEX") > 0 \
                        or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("TRIGGER") > 0  \
                           or p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("DATABASE") > 0:

       if p_sql.upper().count("TRUNCATE") > 0 and p_sql.upper().count("TABLE") > 0:
           obj = re.split(r'\s+', p_sql)[2].replace('`', '')
       elif p_sql.upper().count("TRUNCATE") > 0 and p_sql.upper().count("TABLE") ==  0:
           obj = re.split(r'\s+', p_sql)[1].replace('`', '')
       elif p_sql.upper().count("CREATE") > 0 and p_sql.upper().count("INDEX") > 0 and p_sql.upper().count("UNIQUE") > 0:
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

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def check_mysql_tab_exists(cfg,event):
   cr= cfg['cr_mysql_dest']
   st="""select count(0) from information_schema.tables
            where table_schema='{}' and table_name='{}'""".format(get_mysql_schema(cfg,event),event['table'])
   cr.execute(st)
   rs = cr.fetchone()
   return rs[0]

def check_mysql_database(cfg,event):
    cr= cfg['cr_mysql_dest']
    st = """select count(0) from information_schema.`SCHEMATA` d 
                  where schema_name ='{}'""".format(get_mysql_schema(cfg, event))
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]

def check_mysql_tab_exists_data(cfg,event):
   cr = cfg['cr_mysql_dest']
   st = "select count(0) from {}.{}".format(get_mysql_schema(cfg,event),event['table'])
   cr.execute(st)
   rs = cr.fetchone()
   return rs[0]

def check_tab_exists_pk(cfg,event):
   cr = cfg['cr_mysql']
   st = """select count(0) from information_schema.columns
              where table_schema='{}' and table_name='{}' and column_key='PRI'""".format(event['schema'],event['table'])
   cr.execute(st)
   rs=cr.fetchone()
   return rs[0]

def check_tab_exists(cfg,event):
   cr = cfg['cr_mysql']
   st = """select count(0) from information_schema.tables
              where table_schema='{}' and table_name='{}' """.format(event['schema'],event['table'])
   cr.execute(st)
   rs=cr.fetchone()
   return rs[0]

def f_get_table_ddl(cfg,event):
    cr = cfg['cr_mysql']
    st = """show create table `{0}`.`{1}`""".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    re = rs[1].replace('CREATE TABLE `','CREATE TABLE `{0}`.`'.format(get_mysql_schema(cfg, event)))
    return re

def create_mysql_table(cfg,event):
    cr= cfg['cr_mysql_dest']
    if check_tab_exists_pk(cfg,event) >0:
        if check_mysql_database(cfg,event) == 0:
           cr.execute('create database `{}` /*!40100 DEFAULT CHARACTER SET utf8mb4 */'.format(get_mysql_schema(cfg, event)))
           logging.info('\033[0;36;40mmysql => create  database `{}` success!\033[0m'.format(get_mysql_schema(cfg, event)))
        st = f_get_table_ddl(cfg,event)
        cr.execute(st)
        logging.info('Table:{}.{} created!'.format(get_mysql_schema(cfg, event),event['table']))
    else:
        logging.info('Table `{}`.`{}` have no primary key,exit sync!'.format(get_mysql_schema(cfg, event),event['table']))
        sys.exit(0)

def check_mysql_tab_sync(cfg,event):
   db = cfg['db_mysql_dest']
   cr = db.cursor()
   st = "select count(0) from `{}`.`{}`".format(get_mysql_schema(cfg,event),event['table'])
   cr.execute(st)
   rs = cr.fetchone()
   return rs[0]

def get_sync_table_int_pk_num(cfg,event):
    cr = cfg['cr_mysql']
    st = """select count(0)
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' 
                and column_key='PRI'  AND data_type IN('INT','BIGINT') 
                order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]

def get_sync_table_date_pk_num(cfg,event):
    cr = cfg['cr_mysql']
    st = """select count(0)
              from information_schema.columns
              where table_schema='{}'
                and table_name='{}' 
                and column_key='PRI'  
                and data_type IN('date','timestamp','datetime') 
                order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]


def get_sync_table_id_minus(cfg,event):
    cr = cfg['cr_mysql']
    pk = get_sync_table_pk_names(cfg, event)
    rn = get_sync_table_total_rows(cfg, event)
    logging.info('pk='+pk+',rn='+str(rn))
    if rn  == 0:
       return False
    st = """select max({}) - min({})  from {}.{}""".format(pk,pk,event['schema'],event['table'])
    logging.info('get_sync_table_id_minus:'+st)
    cr.execute(st)
    rs = cr.fetchone()
    logging.info("get_sync_table_id_minus={} - minus:{}/3rn:{}".format(st, rs[0], 3 * rn))
    if rs[0] > 3 * rn :
       return True
    else:
       return False

def get_sync_table_cols(cfg,event):
    cr = cfg['cr_mysql']
    st="""select concat('`',column_name,'`') from information_schema.columns
              where table_schema='{0}' and table_name='{1}'  order by ordinal_position
          """.format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    v_col = ''
    for i in list(rs):
        v_col = v_col + i[0] + ','
    return v_col[0:-1]

def get_tab_header(cfg,event):
    s1 = "insert into `{}`.`{}` (".format(get_mysql_schema(cfg,event),event['table'].lower())
    s2 = " values "
    s1 = s1+get_sync_table_cols(cfg,event)+")"
    return s1+s2

def get_sync_table_pk_names(cfg,event):
    cr = cfg['cr_mysql']
    st="""select column_name  from information_schema.columns
           where table_schema='{0}' and table_name='{1}' and column_key='PRI' order by ordinal_position
       """.format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchall()
    v_col = ''
    for i in list(rs):
        v_col=v_col+i[0]+','
    return v_col[0:-1]

def get_sync_table_min_id(cfg,event):
    cr = cfg['cr_mysql']
    cl = get_sync_table_pk_names(cfg,event)
    st = "select min(`{}`) from `{}`.`{}`".format(cl,event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return  rs[0]

def get_sync_table_multi_pk_rq_col(cfg,event):
    db = cfg['db_mysql']
    cr = db.cursor()
    st ="""SELECT COUNT(0)
                FROM information_schema.columns
                WHERE table_schema='{}'
                AND table_name='{}' 
                AND data_type IN('date','timestamp','datetime')
                AND is_nullable='NO' 
                AND column_key IN('MUL','PRI')
                AND column_name  = '{}'
          """

    st2 = """SELECT COUNT(0)
                    FROM information_schema.columns
                    WHERE table_schema='{}'
                    AND table_name='{}' 
                    AND data_type IN('date','timestamp','datetime')
                    AND is_nullable='NO'
                    AND column_key IN('MUL','PRI')
                    AND column_name  not in ('create_time','create_dt','update_time','update_dt')
              """.format(event['schema'], event['table'])

    st3 = """SELECT column_name
                       FROM information_schema.columns
                       WHERE table_schema='{}'
                       AND table_name='{}' 
                       AND data_type IN('date','timestamp','datetime')
                       AND is_nullable='NO'
                       AND column_key IN('MUL','PRI')
                       AND column_name  not in ('create_time','create_dt','update_time','update_dt') limit 1
                 """.format(event['schema'], event['table'])


    st4 = """SELECT COUNT(0)
                       FROM information_schema.columns
                       WHERE table_schema='{}'
                       AND table_name='{}' 
                       AND column_key IN('MUL','PRI')
                       AND data_type IN('date','timestamp','datetime')  limit 1
                 """.format(event['schema'], event['table'])

    st5 = """SELECT column_name
                       FROM information_schema.columns
                       WHERE table_schema='{}'
                       AND table_name='{}' 
                       AND column_key IN('MUL','PRI')
                       AND data_type IN('date','timestamp','datetime') 
                       ORDER BY CASE 
                                   WHEN column_name = 'create_time' THEN 1
                                   WHEN column_name = 'create_dt' THEN 2
                                   WHEN column_name = 'update_time' THEN 3
                                   WHEN column_name = 'update_dt' THEN 4
                                 ELSE 5 END limit 1
                 """.format(event['schema'], event['table'])

    cr.execute(st.format(event['schema'],event['table'],'create_time'))
    rs = cr.fetchone()
    if rs[0] >0 :
       return 'create_time'

    cr.execute(st.format(event['schema'],event['table'],'create_dt'))
    rs = cr.fetchone()
    if rs[0] > 0:
        return 'create_dt'

    cr.execute(st.format(event['schema'],event['table'],'update_time'))
    rs = cr.fetchone()
    if rs[0] > 0:
        return 'update_time'

    cr.execute(st.format(event['schema'],event['table'],'update_dt'))
    rs = cr.fetchone()
    if rs[0] > 0:
        return 'update_dt'

    cr.execute(st2)
    rs = cr.fetchone()
    if rs[0] > 0:
        cr.execute(st3)
        rs = cr.fetchone()
        return rs[0]

    cr.execute(st4)
    rs = cr.fetchone()
    if rs[0] > 0:
        cr.execute(st5)
        rs = cr.fetchone()
        return rs[0]

    cr.close()
    return ''

def get_sync_table_min_rq(cfg,event):
    cr = cfg['cr_mysql']
    cl = get_sync_table_multi_pk_rq_col(cfg,event)
    st = "select min(`{}`) from `{}`.`{}`".format(cl,event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return  rs[0]

def get_mysql_rq(rq,day=1):
    cr = cfg['cr_mysql']
    st = "select DATE_ADD('{}', INTERVAL {} DAY) as rq ".format(rq,day)
    cr.execute(st)
    rs = cr.fetchone()
    return  rs[0]

def get_sync_table_total_rows(cfg,event):
    cr = cfg['cr_mysql']
    st = "select count(0) from `{0}`.`{1}`".format(event['schema'],event['table'])
    cr.execute(st)
    rs = cr.fetchone()
    return  rs[0]

def full_sync(cfg,event):
    logging.info('full sync table:{}...'.format(event['tab']))
    tab = event['table']

    # one primary key,int type and id continuous
    if (check_mysql_tab_exists(cfg, event) == 0 \
        or (check_mysql_tab_exists(cfg, event) > 0 and check_mysql_tab_sync(cfg, event) == 0)) \
            and check_tab_exists_pk(cfg, event) == 1 and get_sync_table_int_pk_num(cfg, event) == 1 and not get_sync_table_id_minus(cfg, event):
        i_counter = 0
        ins_sql_header = get_tab_header(cfg, event)
        n_batch_size = int(cfg['batch_size'])
        cr_source = cfg['cr_mysql']
        cr_desc   = cfg['cr_mysql_dest']
        n_row     = get_sync_table_min_id(cfg, event)
        if n_row is None:
            logging.info('Table:{0} data is empty ,skip full sync!'.format(tab))
            return
        n_tab_total_rows = get_sync_table_total_rows(cfg, event)
        v_pk_col_name    = get_sync_table_pk_names(cfg, event)
        v_sync_table_cols = get_sync_table_cols(cfg, event)
        while n_tab_total_rows > 0:
            st = "select {} from `{}`.`{}` where {} between {} and {}" \
                .format(v_sync_table_cols, event['schema'],tab, v_pk_col_name, str(n_row),
                        str(n_row + n_batch_size))
            logging.info('v1:'+st)
            cr_source.execute(st)
            rs_source = cr_source.fetchall()
            v_sql = ''
            if len(rs_source) > 0:
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ''
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == '253':  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                        elif col_type in ('1', '3', '8', '246'):  # int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == '12':  # datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                    v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
                logging.info("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                             format(tab, n_tab_total_rows, i_counter,
                                    round(i_counter / n_tab_total_rows * 100, 2)))
            n_row = n_row + n_batch_size + 1
            if i_counter >= n_tab_total_rows or n_tab_total_rows == 0:
                break

    # one primary key,int type and id not continuous
    if (check_mysql_tab_exists(cfg, event) == 0 \
        or (check_mysql_tab_exists(cfg, event) > 0 and check_mysql_tab_sync(cfg, event) == 0)) \
            and check_tab_exists_pk(cfg, event) == 1 and get_sync_table_int_pk_num(cfg, event) == 1 and get_sync_table_id_minus(cfg, event):
        i_counter = 0
        ins_sql_header = get_tab_header(cfg, event)
        cr_source = cfg['cr_mysql']
        cr_desc = cfg['cr_mysql_dest']
        d_min_rq = get_sync_table_min_rq(cfg, event)
        if d_min_rq is None:
            logging.info('Table:{0} date column  value is empty ,skip full sync!'.format(tab))
            return
        n_tab_total_rows = get_sync_table_total_rows(cfg, event)
        v_sync_col = get_sync_table_multi_pk_rq_col(cfg, event)
        if v_sync_col == '':
            logging.info('Table:{0} date column is not exists ,skip full sync!'.format(tab))
            return
        v_sync_table_cols = get_sync_table_cols(cfg, event)

        d_rq_start = d_min_rq
        d_rq_end = get_mysql_rq(d_min_rq, 7)
        while n_tab_total_rows > 0:
            st = "select {} from `{}`.`{}` where {} >= '{}' and  {}<'{}'" \
                .format(v_sync_table_cols, event['schema'], tab, v_sync_col, d_rq_start, v_sync_col, d_rq_end)
            logging.info('v2:'+st)
            cr_source.execute(st)
            rs_source = cr_source.fetchall()
            v_sql = ''
            if len(rs_source) > 0:
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ''
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == '253':  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                        elif col_type in ('1', '3', '8', '246'):  # int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == '12':  # datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                    v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
                logging.info("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                             format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)))

            d_rq_start = d_rq_end
            d_rq_end = get_mysql_rq(d_rq_start, 7)
            if i_counter >= n_tab_total_rows or n_tab_total_rows == 0:
                break

    # more primary key
    if (check_mysql_tab_exists(cfg, event) == 0 \
        or (check_mysql_tab_exists(cfg, event) > 0 and check_mysql_tab_sync(cfg, event) == 0)) \
            and check_tab_exists_pk(cfg, event) == 2 :
        i_counter = 0
        ins_sql_header = get_tab_header(cfg, event)
        cr_source = cfg['cr_mysql']
        cr_desc = cfg['cr_mysql_dest']
        d_min_rq = get_sync_table_min_rq(cfg, event)
        if d_min_rq is None:
            logging.info('Table:{0} date column  value is empty ,skip full sync!'.format(tab))
            return
        n_tab_total_rows = get_sync_table_total_rows(cfg, event)
        v_sync_col = get_sync_table_multi_pk_rq_col(cfg, event)
        if v_sync_col=='':
            logging.info('Table:{0} date column is not exists ,skip full sync!'.format(tab))
            return
        v_sync_table_cols = get_sync_table_cols(cfg, event)

        d_rq_start = d_min_rq
        d_rq_end   = get_mysql_rq(d_min_rq,7)
        while n_tab_total_rows > 0:
            st = "select {} from `{}`.`{}` where {} >= '{}' and  {}<'{}'" \
                .format(v_sync_table_cols, event['schema'], tab, v_sync_col, d_rq_start,v_sync_col,d_rq_end)
            logging.info('v3:'+st)
            cr_source.execute(st)
            rs_source = cr_source.fetchall()
            v_sql = ''
            if len(rs_source) > 0:
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ''
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == '253':  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                        elif col_type in ('1', '3', '8', '246'):  # int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == '12':  # datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                    v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
                logging.info("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                        format(tab, n_tab_total_rows, i_counter,round(i_counter / n_tab_total_rows * 100, 2)))

            d_rq_start = d_rq_end
            d_rq_end = get_mysql_rq(d_rq_start,7)
            if i_counter >= n_tab_total_rows or n_tab_total_rows == 0:
                break

    # one primary and date column
    if (check_mysql_tab_exists(cfg, event) == 0 \
        or (check_mysql_tab_exists(cfg, event) > 0 and check_mysql_tab_sync(cfg, event) == 0)) \
            and check_tab_exists_pk(cfg, event) == 1 and get_sync_table_date_pk_num(cfg, event) >= 1:
        i_counter = 0
        ins_sql_header = get_tab_header(cfg, event)
        cr_source = cfg['cr_mysql']
        cr_desc = cfg['cr_mysql_dest']
        d_min_rq = get_sync_table_min_rq(cfg, event)
        if d_min_rq is None:
            logging.info('Table:{0} date column  value is empty ,skip full sync!'.format(tab))
            return
        n_tab_total_rows = get_sync_table_total_rows(cfg, event)
        v_sync_col = get_sync_table_multi_pk_rq_col(cfg, event)
        if v_sync_col == '':
            logging.info('Table:{0} date column is not exists ,skip full sync!'.format(tab))
            return
        v_sync_table_cols = get_sync_table_cols(cfg, event)

        d_rq_start = d_min_rq
        d_rq_end = get_mysql_rq(d_min_rq, 7)
        while n_tab_total_rows > 0:
            st = "select {} from `{}`.`{}` where {} >= '{}' and  {}<'{}'" \
                .format(v_sync_table_cols, event['schema'], tab, v_sync_col, d_rq_start, v_sync_col, d_rq_end)
            logging.info('v3:' + st)
            cr_source.execute(st)
            rs_source = cr_source.fetchall()
            v_sql = ''
            if len(rs_source) > 0:
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ''
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == '253':  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                        elif col_type in ('1', '3', '8', '246'):  # int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == '12':  # datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        else:
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                    v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
                logging.info("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                             format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)))

            d_rq_start = d_rq_end
            d_rq_end = get_mysql_rq(d_rq_start, 7)
            if i_counter >= n_tab_total_rows or n_tab_total_rows == 0:
                break


def create_table_many(cfg):
    # create table
    for o in cfg['sync_table']:
        if o != '':
            event = {'schema': o.split('$')[0].split('.')[0], 'table': o.split('$')[0].split('.')[1],
                     'column': o.split('$')[0].split('.')[2]}
            if check_tab_exists_pk(cfg, event) > 0 and check_mysql_tab_exists(cfg, event) == 0:
                event['tab'] = event['schema'] + '.' + event['table']
                logging.info('create table {} ...'.format(event['tab']))
                create_mysql_table(cfg, event)

def full_sync_one(cfg,event):
    cfg['cr_mysql'].execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cfg['cr_mysql'].execute('START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */')
    cfg['cr_mysql'].execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cfg['cr_mysql'].execute('START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */')
    file,pos = get_file_and_pos(cfg)[0:2]
    cfg['full_checkpoint'][event['tab']] = {
         'binlog_file':file,
         'binlog_pos':pos
    }
    cfg['cr_mysql'].execute('UNLOCK TABLES')
    if check_tab_exists_pk(cfg,event) >0 and check_mysql_tab_exists_data(cfg, event) == 0:
        event['tab'] = event['schema']+'.'+event['table']
        full_sync(cfg,event)
    cfg['db_mysql'].commit()

def full_sync_many(cfg):
    cfg['cr_mysql'].execute('FLUSH /*!40101 LOCAL */ TABLES')
    cfg['cr_mysql'].execute('FLUSH TABLES WITH READ LOCK')
    cfg['cr_mysql'].execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cfg['cr_mysql'].execute('START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */')
    cfg['full_binlog_file'], cfg['full_binlog_pos'] = get_file_and_pos(cfg)[0:2]
    logging.info('full sync checkpoint:{}/{}'.format(cfg['full_binlog_file'], cfg['full_binlog_pos']))
    cfg['cr_mysql'].execute('UNLOCK TABLES')
    cfg['sync_table_diff'] = []
    for o in cfg['sync_table']:
        if o != '':
           event = {'schema': o.split('$')[0].split('.')[0], 'table': o.split('$')[0].split('.')[1],'column': o.split('$')[0].split('.')[2]}
           event['tab'] = event['schema'] + '.' + event['table']
           if check_tab_exists_pk(cfg,event) >0 and check_mysql_tab_exists(cfg, event) == 0:
              cfg['sync_table_diff'].append(o)
              if check_mysql_tab_exists(cfg, event) == 0:
                 logging.info('create table {} ...'.format(event['tab']))
                 create_mysql_table(cfg, event)
              else:
                 logging.info('Table {} exists!'.format(event['tab']))
              cfg['cr_mysql_log'].execute("delete from t_db_sync_log where sync_tag='{}' and sync_table='{}'".format(cfg['sync_tag'],event['tab']))
              logging.info("delete from t_db_sync_log where sync_tag='{}' and sync_table='{}'".format(cfg['sync_tag'],event['tab']))
              full_sync(cfg,event)
    cfg['db_mysql'].commit()

def get_cols_from_mysql(cfg,event):
    cr = cfg['cr_mysql']
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
    return v_col[0:-1]

def set_column(event):
    v_set = ' '
    for key in event['after_values']:
        if event['after_values'][key] is None:
           v_set = v_set + '`'+key +'`'+ '=null,'
        else:
           if event['pkn'].count(key)==0:
               if event['type'][key] in ('tinyint', 'int', 'bigint', 'float', 'double','decimal'):
                   v_set = v_set + '`'+key +'`'+'='+ format_sql(str(event['after_values'][key])) + ','
               else:
                   v_set = v_set + '`'+key +'`'+'=\''+ format_sql(str(event['after_values'][key])) + '\','
    return v_set[0:-1]

def get_mysql_schema(cfg,event):
    for o in cfg['sync_table']:
      mysql_schema = o.split('.')[0]
      mysql_table  = o.split('$')[0].split('.')[1]
      dest_schema    = o.split('$')[1] if o.split('$')[1] !='auto' else mysql_schema
      if event['schema'] == mysql_schema and event['table'] == mysql_table:
         return  cfg['desc_db_prefix']+dest_schema
    return 'test'

def get_ins_header(cfg,event):
    v_ddl = 'insert into {0}.{1} ('.format(get_mysql_schema(cfg,event), event['table'])
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
    cr = cfg['cr_mysql']
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

def get_where(event):
    v_where = ' where '
    v_pk_name = '('
    v_pk_value = '(select '
    if event['action'] == 'delete':
        for key in event['data']:
            if event['pks'] :
                if key in event['pkn']:
                    if len(event['pkn']) >= 2:
                        v_pk_name = v_pk_name + key + ','
                        v_pk_value = v_pk_value + get_ck_col_type(event['type'][key],str(event['data'][key])) + ','
                    else:
                        if event['type'][key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                           v_where = v_where + key + ' = ' + str(event['data'][key]) + ' and '
                        else:
                           v_where = v_where + key + ' = \'' + str(event['data'][key]) + '\' and '
            else:
               v_where = v_where+ key+' = \''+str(event['data'][key]) + '\' and '

        if len(event['pkn']) >= 2:
            v_where = v_where + v_pk_name[0:-1] + ') = ' + v_pk_value[0:-1] + ')'
            return v_where

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

def get_ck_col_type(v_type,v_value):
    if v_type == 'tinyint':
        v = 'toInt16({})'.format(v_value)
    elif v_type == 'int':
        v = 'toInt32({})'.format(v_value)
    elif v_type == 'bigint':
        v = 'toInt64({})'.format(v_value)
    elif v_type == 'float':
        v = 'toFloat32({})'.format(v_value)
    elif v_type == 'double':
        v = 'toFloat64({})'.format(v_value)
    elif v_type == 'date':
        v= "toDate('{}')".format(v_value)
    elif v_type == 'datetime':
        v= "toDateTime('{}')".format(v_value)
    else:
        v = "'{}'".format(v_value)
    return v

def get_where_upd(event):
    v_where = ' where '
    v_pk_name = '('
    v_pk_value = '(select '
    if event['action'] == 'update':
        for key in event['after_values']:
            if event['pks'] :
                if key in event['pkn']:
                    if len(event['pkn']) >= 2:
                        v_pk_name = v_pk_name + key + ','
                        v_pk_value = v_pk_value + get_ck_col_type(event['type'][key],str(event['after_values'][key])) + ','
                    else:
                        if event['type'][key] in ('tinyint', 'int', 'bigint', 'float', 'double'):
                           v_where = v_where + key + ' = ' + str(event['after_values'][key]) + ' and '
                        else:
                           v_where = v_where + key + ' = \'' + str(event['after_values'][key]) + '\' and '
            else:
               v_where = v_where+ key+' = \''+str(event['after_values'][key]) + '\' and '

        if len(event['pkn'])>=2:
           v_where =  v_where + v_pk_name[0:-1]+') = ' + v_pk_value[0:-1]+')'
           return v_where

    return v_where[0:-5]

def gen_sql(cfg,event):
    if event['action'] in ('insert'):
        sql  = get_ins_header(cfg,event)+ ' values ('+get_ins_values(event)+')'
    elif event['action'] == 'update':
        sql = 'update {0}.{1} set {2} {3}'.format(get_mysql_schema(cfg, event), event['table'],set_column(event), get_where(event))
    elif event['action']=='delete':
        sql  = 'delete from {0}.{1} {2}'.format(get_mysql_schema(cfg,event),event['table'],get_where(event))
    return sql

def gen_ddl_sql(p_ddl):
    if p_ddl.find('create table')>=0 or p_ddl.find('drop table')>=0  or p_ddl.find('truncate')>=0 or p_ddl.find('alter table')>=0:
       return p_ddl
    else:
       return None

def get_file_and_pos(cfg):
    cr = cfg['cr_mysql']
    cr.execute('show master status')
    rs = cr.fetchone()
    return rs

def get_binlog_files(cfg):
    cr = cfg['cr_mysql']
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

def check_sync(cfg,event,pks):
    res = False
    if pks.get(event['schema'] + '.' + event['table']) is None:
       return False
    
    if pks[event['schema'] + '.' + event['table']] :
       if  event['schema']+'.'+event['table'] in cfg['sync_check']:
           return True
    return res

def write_ckpt(cfg):
    ck = {
       'binlog_file': cfg['binlog_file'],
       'binlog_pos' : cfg['binlog_pos'],
       'last_update_time': get_time(),
       'pid':os.getpid()
    }
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'w') as f:
        f.write(json.dumps(ck, ensure_ascii=False, indent=4, separators=(',', ':')))
    logging.info('update checkpoint:{}/{}'.format(cfg['binlog_file'],cfg['binlog_pos'] ))

def upd_ckpt(cfg):
    ckpt = {
       'binlog_file': cfg['binlog_file'],
       'binlog_pos' : cfg['binlog_pos'],
       'last_update_time': get_time(),
       'pid':cfg['pid']
    }
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'w') as f:
        f.write(json.dumps(ckpt, ensure_ascii=False, indent=4, separators=(',', ':')))
    logging.info('update ckpt:{}/{}'.format(cfg['binlog_file'],cfg['binlog_pos'] ))

def check_ckpt(cfg):
    return os.path.isfile('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']))

def read_ckpt(cfg):
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        binlog = json.loads(contents)
        return binlog

def get_ds_mysql_sour(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8mb4',autocommit=False)
    return conn

def get_ds_mysql_dest(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8mb4',autocommit=True)
    return conn

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://210.13.35.136:21080/read_db_decrypt'
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

def get_config_from_db(tag):
    url = 'http://210.13.35.136:21080/read_config_sync'
    res = requests.post(url, data= { 'tag': tag},timeout=1).json()
    if res['code'] == 200:
        config                           = res['msg']
        db_mysql_ip                      = config['sync_db_sour'].split(':')[0]
        db_mysql_port                    = config['sync_db_sour'].split(':')[1]
        db_mysql_service                 = config['sync_db_sour'].split(':')[2]
        db_mysql_user                    = config['sync_db_sour'].split(':')[3]
        db_mysql_pass                    = aes_decrypt(config['sync_db_sour'].split(':')[4],db_mysql_user)
        db_mysql_ip_dest                 = config['sync_db_dest'].split(':')[0]
        db_mysql_port_dest               = config['sync_db_dest'].split(':')[1]
        db_mysql_service_dest            = config['sync_db_dest'].split(':')[2]
        db_mysql_user_dest               = config['sync_db_dest'].split(':')[3]
        db_mysql_pass_dest               = aes_decrypt(config['sync_db_dest'].split(':')[4],db_mysql_user_dest)
        config['db_mysql_ip']            = db_mysql_ip
        config['db_mysql_port']          = db_mysql_port
        config['db_mysql_service']       = db_mysql_service
        config['db_mysql_user']          = db_mysql_user
        config['db_mysql_pass']          = db_mysql_pass
        config['db_mysql_ip_dest']       = db_mysql_ip_dest
        config['db_mysql_port_dest']     = db_mysql_port_dest
        config['db_mysql_service_dest']  = db_mysql_service_dest
        config['db_mysql_user_dest']     = db_mysql_user_dest
        config['db_mysql_pass_dest']     = db_mysql_pass_dest
        config['db_mysql_string']        = db_mysql_ip + ':' + db_mysql_port + '/' + db_mysql_service
        config['db_mysql_dest_string']   = db_mysql_ip_dest + ':' + db_mysql_port_dest + '/' + db_mysql_service_dest
        config['db_mysql']               = get_ds_mysql_sour(db_mysql_ip, db_mysql_port, db_mysql_service, db_mysql_user, db_mysql_pass)
        config['cr_mysql']               = config['db_mysql'].cursor()
        config['db_mysql_dest']          = get_ds_mysql_dest(db_mysql_ip_dest, db_mysql_port_dest, db_mysql_service_dest, db_mysql_user_dest, db_mysql_pass_dest)
        config['cr_mysql_dest']          = config['db_mysql_dest'].cursor()

        if config.get('ds_ro') is not None and config.get('ds_ro') != '':
            config['db_mysql_ip_ro']      = config['ds_ro']['ip']
            config['db_mysql_port_ro']    = config['ds_ro']['port']
            config['db_mysql_service_ro'] = config['ds_ro']['service']
            config['db_mysql_user_ro']    = config['ds_ro']['user']
            config['db_mysql_pass_ro']    = aes_decrypt(config['ds_ro']['password'],config['ds_ro']['user'])
            config['db_mysql_ro']         = get_ds_mysql_sour(config['db_mysql_ip_ro'] ,
                                                         config['db_mysql_port_ro'],
                                                         config['db_mysql_service_ro'],
                                                         config['db_mysql_user_ro'],
                                                         config['db_mysql_pass_ro'] )

        if config.get('ds_log') is not None and config.get('ds_log') != '':
            config['db_mysql_ip_log']      = config['ds_log']['ip']
            config['db_mysql_port_log']    = config['ds_log']['port']
            config['db_mysql_service_log'] = config['log_db_name']
            config['db_mysql_user_log']    = config['ds_log']['user']
            config['db_mysql_pass_log']    = aes_decrypt(config['ds_log']['password'],config['ds_log']['user'])
            config['db_mysql_log']         = get_ds_mysql_dest(config['db_mysql_ip_log'],
                                                               config['db_mysql_port_log'],
                                                               config['db_mysql_service_log'],
                                                               config['db_mysql_user_log'],
                                                               config['db_mysql_pass_log'])
            config['cr_mysql_log']        = config['db_mysql_log'].cursor()

        if check_ckpt(config):
            ckpt = read_ckpt(config)
            file = ckpt['binlog_file']
            pos = ckpt['binlog_pos']
            pid = ckpt['pid']
            if file not in get_binlog_files(config):
                logging.info('binlog file not exist mysql server,get current file,pos!!!')
                file, pos = get_file_and_pos(config)[0:2]
        else:
            file, pos = get_file_and_pos(config)[0:2]
            pid = os.getpid()

        config['binlog_file'] = file
        config['binlog_pos'] = pos
        config['pid'] = pid
        config['sleep_time'] = float(config['sync_gap'])
        config['sync_event'] = []
        config['sync_event_timeout'] = datetime.datetime.now()
        config['full_checkpoint'] = {}
        config = get_sync_tables(config)
        upd_ckpt(config)
        return config
    else:
        logging.info('load config failure:{0}'.format(res['msg']))
        return None

def get_tables(cfg,o):
    cr  = cfg['cr_mysql']
    sdb = o.split('$')[0].split('.')[0]
    tab = o.split('$')[0].split('.')[1]
    col = o.split('$')[0].split('.')[2]
    ddb = o.split('$')[1]
    if tab.count('*') > 0:
       tab = tab.replace('*','')
       st = """select table_name from information_schema.tables
                  where table_schema='{}' and instr(table_name,'{}')>0 order by table_name""".format(sdb,tab)
    else:
       st = """select table_name from information_schema.tables
                            where table_schema='{}' and table_name='{}' order by table_name""".format(sdb, tab)
    cr.execute(st)
    rs = cr.fetchall()
    vv1 = ''
    vv2 = ''
    for i in list(rs):
        evt = {'schema': o.split('$')[0].split('.')[0], 'table': i[0]}
        if check_tab_exists_pk(cfg,evt)>0:
           vv1 = vv1 + '{}.{}.{}${},'.format(sdb, i[0], col, ddb)
           vv2 = vv2 + '{}.{},'.format(sdb,i[0])
    return vv1[0:-1],vv2[0:-1]

def get_sync_tables(cfg):
    v1 = ''
    v2 = ''
    for o in cfg['sync_table'].split(','):
        if o !='':
           t1,t2 = get_tables(cfg,o)
           v1 = v1 + t1+','
           v2 = v2 + t2+',' 
            
    cfg['sync_table'] = v1[0:-1].split(',')
    cfg['sync_check'] = v2[0:-1].split(',')
    return cfg

def write_event(cfg,event):
    try:
       st = """insert into t_db_sync_log(`sync_table`,`statement`) 
               values('{}','{}') """.format(event['tab'],json.dumps(event,cls=DateEncoder))
       cfg['cr_mysql_log'].execute(st)
    except:
        logging.info('write event failure!')
        logging.info(traceback.format_exc())

def merge_insert(data):
   s = "insert into t_db_sync_log(`sync_tag`,`sync_table`,`statement`,`type`) values "
   for row in data:
     t = '('
     for col in row:
       if col == 'statement':
          t = t + "'{}',".format(format_sql(row[col]))
       else:
          t = t + "'{}',".format(row[col])
     s = s + t[0:-1]+'),'
   return s[0:-1]

def flush_buffer(cfg):
    if len(cfg['sync_event'])>0:
        if get_seconds(cfg['sync_event_timeout']) >= int(cfg['sync_gap']):
            st = merge_insert(cfg['sync_event'])
            cfg['cr_mysql_log'].execute(st)
            logging.info('[{}] writing buffer into log table(timeout:{},binlog:{}/{})!'.
                format(cfg['sync_tag'].split('_')[0],str(len(cfg['sync_event'])),cfg['binlog_file'] ,cfg['binlog_pos'] ))
            write_ckpt(cfg)
            cfg['sync_event'] = []
            cfg['sync_event_timeout'] = datetime.datetime.now()

        if len(cfg['sync_event']) >= int(cfg['batch_size_incr']):
            st = merge_insert(cfg['sync_event'])
            cfg['cr_mysql_log'].execute(st)
            logging.info('[{}] writing buffer into log server(full:{},binlog:{}/{})!'.
                format(cfg['sync_tag'].split('_')[0],str(len(cfg['sync_event'])),cfg['binlog_file'] ,cfg['binlog_pos'] ))
            write_ckpt(cfg)
            cfg['sync_event'] = []
            cfg['sync_event_timeout'] = datetime.datetime.now()

def write_sql(cfg,event):
    if cfg['full_checkpoint'].get(event['tab']):
       ect = '{}.{}'.format(cfg['binlog_file'], str(cfg['binlog_pos']))
       fct = '{}.{}'.format(cfg['full_checkpoint'][event['tab']]['binlog_file'],str(cfg['full_checkpoint'][event['tab']]['binlog_pos']))
       if ect > fct :
           logging.info('full sync checkpoint:{}'.format(fct))
           logging.info('incr sync checkpoint:{}'.format(ect))
           cfg['sync_event'].append(
               {
                   'sync_tag': cfg['sync_tag'],
                   'sync_table': event['tab'],
                   'statement': event['sql'],
                   'type': event['action']
               }
           )
           logging.info('[{}] writing event {} into buffer[{}/{},binlog:{}/{}]!'.
                        format(cfg['sync_tag'].split('_')[0],
                               event['action'],
                               int(cfg['batch_size_incr']),
                               len(cfg['sync_event']),
                               cfg['binlog_file'],
                               cfg['binlog_pos']))
           flush_buffer(cfg)
           logging.info('delete full sync table {},checkpoint:{}'.format(event['tab'], cfg['full_checkpoint'][event['tab']]))
           del cfg['full_checkpoint'][event['tab']]
       else:
           logging.info('full sync checkpoint skipped!')
           logging.info('full checkpoint:{}'.format(fct))
           logging.info('incr checkpoint:{}'.format(ect))
    else:
        cfg['sync_event'].append(
          {
             'sync_tag'   : cfg['sync_tag'],
             'sync_table' : event['tab'],
             'statement'  : event['sql'],
             'type'       : event['action']
          }
        )
        logging.info('[{}] writing event {} into buffer[{}/{},binlog:{}/{}]!'.
             format(cfg['sync_tag'].split('_')[0],
                    event['action'],
                    int(cfg['batch_size_incr']),
                    len(cfg['sync_event']),
                    cfg['binlog_file'],
                    cfg['binlog_pos']))
        flush_buffer(cfg)

def write_sync_log(config):
    file, pos = get_file_and_pos(config)[0:2]
    par = {
            'sync_tag'       : config['sync_tag'],
            'event_amount'   : config['event_amount'],
            'insert_amount'  : config['insert_amount'],
            'update_amount'  : config['update_amount'],
            'delete_amount'  : config['delete_amount'],
            'ddl_amount'     : config['ddl_amount'],
            'binlogfile'     : config['binlog_file'],
            'binlogpos'      : config['binlog_pos'],
            'c_binlogfile'   : file,
            'c_binlogpos'    : pos,
            'create_date'    : get_time()
    }
    try:
        url = 'http://$$API_SERVER$$/write_sync_real_log'
        res = requests.post(url, data={'tag': json.dumps(par)},timeout=3)
        if res.status_code != 200:
           logging.info('Interface write_sync_log call failed!')
        else:
           logging.info('Interface write_sync_log success!')
    except:
         logging.info('write_sync_log failure!')
         logging.info(traceback.format_exc())

def read_real_sync_status(p_tag):
    try:
        par = {'tag': p_tag}
        url = 'http://$$API_SERVER$$/get_real_sync_status'
        res = requests.post(url,data=par,timeout=3).json()
        return res
    except:
        logging.info('read_real_sync_status failure!')
        return None


def get_table_pk_names(cfg,event):
    cr = cfg['cr_mysql']
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
    return v_col[0:-1]

def init_cfg(cfg):
    types = {}
    pks   = {}
    pkn   = {}
    col   = {}
    for o in cfg['sync_table']:
        if o != '':
            evt = {'schema':o.split('$')[0].split('.')[0],'table':o.split('$')[0].split('.')[1],'column': o.split('$')[0].split('.')[2]}
            tab = evt['schema']+'.'+evt['table']
            if check_tab_exists_pk(cfg, evt) > 0:
                types[tab] = get_col_type(cfg, evt)
                pks[tab]   = True
                pkn[tab]   = get_table_pk_names(cfg,evt).replace('`','').split(',')
                col[tab]   = evt['column']
            else:
                logging.info("\033[0;31;40mTable:{}.{} not primary key,skip sync...\033[0m".format(evt['schema'],evt['table']))
                types[tab] = None
                pks[tab] = False
                pkn[tab] = ''
                col[tab] = ''
    return types,pks,pkn,col

def init_diff_cfg(cfg):
    types = {}
    pks   = {}
    pkn   = {}
    col   = {}
    for o in list(set(cfg['sync_table'])-set(cfg['sync_table_diff'])):
        if o != '':
            evt = {'schema':o.split('$')[0].split('.')[0],'table':o.split('$')[0].split('.')[1],'column': o.split('$')[0].split('.')[2]}
            tab = evt['schema']+'.'+evt['table']
            if check_tab_exists_pk(cfg, evt) > 0:
                types[tab] = get_col_type(cfg, evt)
                pks[tab]   = True
                pkn[tab]   = get_table_pk_names(cfg,evt).replace('`','').split(',')
                col[tab]   = evt['column']
            else:
                logging.info("\033[0;31;40mTable:{}.{} not primary key,skip sync...\033[0m".format(evt['schema'],evt['table']))
                types[tab] = None
                pks[tab] = False
                pkn[tab] = ''
                col[tab] = ''
    return types,pks,pkn,col

def start_incr_sync(cfg):
    logging.info("\033[0;36;40m[{}] start incr sync...\033[0m".format(cfg['sync_tag'].split('_')[0]))
    logging.info("\033[0;36;40m binlog_file:{},binlog_pos:{}\033[0m".format(cfg['full_binlog_file'],cfg['full_binlog_pos']))
    MYSQL_SETTINGS = {
        "host"   : cfg['db_mysql_ip'],
        "port"   : int(cfg['db_mysql_port']),
        "user"   : "canal2021",
        "passwd" : "canal@Hopson2018",
    }

    types,pks,pkn,col = init_cfg(cfg)

    try:
        stream = BinLogStreamReader(
            connection_settings = MYSQL_SETTINGS,
            only_events         = (QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
            server_id           = int(time.time()),
            blocking            = True,
            resume_stream       = True,
            log_file            = cfg['binlog_file'] if cfg['sync_table_diff'] == [] else cfg['full_binlog_file'],
            log_pos             = int(cfg['binlog_pos']) if cfg['sync_table_diff'] == [] else int(cfg['full_binlog_pos']),
            auto_position       = False
        )

        insert_amount = 0
        update_amount = 0
        delete_amount = 0
        ddl_amount    = 0
        apply_time    = datetime.datetime.now()
        gather_time   = datetime.datetime.now()
        sync_time     = datetime.datetime.now()

        for binlogevent in stream:
            cfg['binlog_file'] = stream.log_file
            cfg['binlog_pos'] = stream.log_pos

            if get_seconds(sync_time) >= 3:
                sync_time = datetime.datetime.now()
                if not read_real_sync_status(cfg['sync_tag']) is None:
                    if read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status'] == 'PAUSE':
                       while True:
                            time.sleep(1)
                            if  read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status']  == 'PAUSE':
                               logging.info("\033[1;37;40msync task {} suspended!\033[0m".format(cfg['sync_tag']))
                               continue
                            elif read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status'] == 'STOP':
                               logging.info("\033[1;37;40msync task {} terminate!\033[0m".format(cfg['sync_tag']))
                               break
                            else:
                               break
                    elif  read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status']  == 'STOP':
                        logging.info("\033[1;37;40m sync task {} terminate!\033[0m".format(cfg['sync_tag']))
                        break

            if get_seconds(gather_time) >= int(cfg['sync_gap']):
               cfg['event_amount']  = insert_amount+update_amount+delete_amount+ddl_amount
               cfg['insert_amount'] = insert_amount
               cfg['update_amount'] = update_amount
               cfg['delete_amount'] = delete_amount
               cfg['ddl_amount']    = ddl_amount
               write_sync_log(cfg)
               logging.info("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))
               insert_amount = 0
               update_amount = 0
               delete_amount = 0
               ddl_amount = 0
               gather_time = datetime.datetime.now()
               file, pos = get_file_and_pos(cfg)[0:2]
               logging.info("\033[1;36;40m[{}] update ckpt file: [db: {}/{} - sync:{}/{}]!\033[0m".format(cfg['sync_tag'].split('_')[0],file,pos,cfg['binlog_file'],cfg['binlog_pos']))
               flush_buffer(cfg)

            if get_seconds(apply_time) >= cfg['apply_timeout']:
               sync_event = cfg['sync_event']
               sync_event_timeout = cfg['sync_event_timeout']
               sync_binlog_file = cfg['binlog_file']
               sync_binlog_pos = cfg['binlog_pos']
               full_checkpoint = cfg['full_checkpoint']
               tmp = get_config_from_db(cfg['sync_tag'])
               if tmp is None:
                   logging.info('config apply failure,skip load config!')
               else:
                   cfg = tmp
                   cfg['sync_event'] = sync_event
                   cfg['sync_event_timeout'] = sync_event_timeout
                   cfg['binlog_file'] = sync_binlog_file
                   cfg['binlog_pos'] = sync_binlog_pos
                   cfg['full_checkpoint'] = full_checkpoint
                   types,pks,pkn,col = init_cfg(cfg)
                   apply_time = datetime.datetime.now()
                   logging.info("\033[1;36;40m[{}] apply config success!\033[0m".format(cfg['sync_tag'].split('_')[0]))
                   flush_buffer(cfg)

            if isinstance(binlogevent, RotateEvent):
                cfg['binlog_file'] = binlogevent.next_binlog
                logging.info("\033[1;34;40m[{}] binlog file has changed!\033[0m".format(cfg['sync_tag'].split('_')[0]))

            if isinstance(binlogevent, QueryEvent):
                cfg['binlog_pos'] = binlogevent.packet.log_pos
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
                    logging.info('query:' + event['query'])
                    ddl = gen_ddl_sql(event['query'])
                    if ddl is not None:
                      logging.info('DDL:' + ddl)
                      event['table'] = get_obj_name(event['query']).lower()
                      event['tab'] = event['schema']+'.'+event['table']

                      if check_sync(cfg,event,pks):
                        if check_mysql_tab_exists(cfg,event) == 0:
                           create_mysql_table(cfg,event)
                           cfg['cr_mysql_log'].execute("delete from t_db_sync_log where sync_tag='{}' and sync_table='{}'".format(cfg['sync_tag'], event['tab']))
                           logging.info("delete from t_db_sync_log where sync_tag='{}' and sync_table='{}'".format(cfg['sync_tag'], event['tab']))
                           full_sync_one(cfg, event)
                           logging.info("\033[1;36;40m[{}] The table:{}  is fully synchronized!\033[0m".format(event['tab']))
                           logging.info("\033[1;37;40m full sync checkpoint:{}!\033[0m".format(json.dumps(cfg['full_checkpoint'])))
                           types[event['schema']+'.'+event['table']] = get_col_type(cfg, event)
                           ddl_amount = ddl_amount +1
                        else:
                           try:
                               logging.info('Execute DDL:{}'.format(ddl))
                               logging.info('use {}'.format(event['schema']))
                               cfg['db_mysql_dest'].cursor().execute('use {}'.format(event['schema']))
                               cfg['db_mysql_dest'].cursor().execute(ddl)
                           except:
                               traceback.print_exc()
                        write_ckpt(cfg)

            if isinstance(binlogevent, DeleteRowsEvent) or \
                    isinstance(binlogevent, UpdateRowsEvent) or \
                        isinstance(binlogevent, WriteRowsEvent):

                if get_seconds(sync_time) >= 3:
                    sync_time = datetime.datetime.now()
                    if not read_real_sync_status(cfg['sync_tag']) is None:
                        if  read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status'] == 'PAUSE':
                            while True:
                                time.sleep(1)
                                if  read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status'] == 'PAUSE':
                                    logging.info("\033[1;37;40m sync task {} suspended!\033[0m".format(cfg['sync_tag']))
                                    continue
                                elif read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status']== 'STOP':
                                    logging.info("\033[1;37;40m sync task {} terminate!\033[0m".format(cfg['sync_tag']))
                                    sys.exit(0)
                                else:
                                    break
                        elif  read_real_sync_status(cfg['sync_tag'])['msg']['real_sync_status'] == 'STOP':
                            logging.info("\033[1;37;40m sync task {} terminate!\033[0m".format(cfg['sync_tag']))
                            sys.exit(0)

                for row in binlogevent.rows:
                    cfg['binlog_pos'] = binlogevent.packet.log_pos
                    event = {"schema": binlogevent.schema.lower(), "table": binlogevent.table.lower()}

                    if check_sync(cfg, event,pks):

                        if check_mysql_tab_exists(cfg, event) == 0:
                            event['tab'] = event['schema'] + '.' + event['table']
                            event['column'] = col[event['tab']]
                            create_mysql_table(cfg, event)
                            cfg['cr_mysql_log'].execute("delete from t_db_sync_log where sync_tag='{}' and sync_table='{}'".format(cfg['sync_tag'], event['tab']))
                            logging.info("delete from t_db_sync_log where sync_tag='{}' and sync_table='{}'".format(cfg['sync_tag'], event['tab']))
                            full_sync_one(cfg, event)
                            logging.info("\033[1;36;40m[{}] The table:{}  is fully synchronized!\033[0m".format(event['tab']))
                            logging.info("\033[1;37;40m full sync checkpoint:{}!\033[0m".format(json.dumps(cfg['full_checkpoint'])))
                            types[event['schema'] + '.' + event['table']] = get_col_type(cfg, event)
                            write_ckpt(cfg)

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"]   = row["values"]
                            event['type']   = types[event['schema']+'.'+event['table']]
                            event['pks']    = pks[event['schema'] + '.' + event['table']]
                            event['pkn']    = pkn[event['schema']+'.'+event['table']]
                            event['tab']    = event['schema']+'.'+event['table']
                            event['sql']    = gen_sql(cfg,event)
                            write_sql(cfg, event)
                            delete_amount = delete_amount + 1
                            #write_ckpt(cfg)

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"]        = "update"
                            event["after_values"]  = row["after_values"]
                            event["before_values"] = row["before_values"]
                            event['type']          = types[event['schema'] + '.' + event['table']]
                            event['pks']           = pks[event['schema'] + '.' + event['table']]
                            event['pkn']           = pkn[event['schema'] + '.' + event['table']]
                            event['tab']           = event['schema'] + '.' + event['table']
                            event['sql']           = gen_sql(cfg, event)
                            write_sql(cfg, event)
                            update_amount = update_amount +1
                            #write_ckpt(cfg)

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"]   = row["values"]
                            event['type']   = types[event['schema'] + '.' + event['table']]
                            event['pks']    = pks[event['schema'] + '.' + event['table']]
                            event['pkn']    = pkn[event['schema'] + '.' + event['table']]
                            event['tab']    = event['schema'] + '.' + event['table']
                            event['sql']    = gen_sql(cfg, event)
                            write_sql(cfg, event)
                            insert_amount = insert_amount +1
                            #write_ckpt(cfg)

    except :
        logging.info('start_incr_sync failure!')
        logging.info(traceback.format_exc())
    finally:
        stream.close()

def apply_diff_logs(cfg):
    if list(set(cfg['sync_table']) - set(cfg['sync_table_diff'])) == [] or cfg['sync_table_diff'] == []:
       return

    logging.info("\033[0;36;40m[{}] apply diff logs...\033[0m".format(cfg['sync_tag'].split('_')[0]))
    logging.info("\033[0;36;40mFrom binlog_file:{}/{} To binlog_pos:{}/{}\033[0m".format(cfg['binlog_file'],cfg['binlog_pos'],cfg['full_binlog_file'],cfg['full_binlog_pos']))
    MYSQL_SETTINGS = {
        "host"   : cfg['db_mysql_ip'],
        "port"   : int(cfg['db_mysql_port']),
        "user"   : "canal2021",
        "passwd" : "canal@Hopson2018",
    }
    types,pks,pkn,col = init_diff_cfg(cfg)

    try:
        stream = BinLogStreamReader(
            connection_settings = MYSQL_SETTINGS,
            only_events         = (QueryEvent, DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
            server_id           = int(time.time()),
            blocking            = True,
            resume_stream       = True,
            log_file            = cfg['binlog_file'],
            log_pos             = int(cfg['binlog_pos']),
            auto_position       = False
        )

        insert_amount = 0
        update_amount = 0
        delete_amount = 0
        ddl_amount    = 0
        gather_time   = datetime.datetime.now()
        sync_time     = datetime.datetime.now()

        for binlogevent in stream:
            cfg['binlog_file'] = stream.log_file
            cfg['binlog_pos'] = stream.log_pos

            if get_seconds(gather_time) >= int(cfg['sync_gap']):
               cfg['event_amount']  = insert_amount+update_amount+delete_amount+ddl_amount
               cfg['insert_amount'] = insert_amount
               cfg['update_amount'] = update_amount
               cfg['delete_amount'] = delete_amount
               cfg['ddl_amount']    = ddl_amount
               write_sync_log(cfg)
               logging.info("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))
               insert_amount = 0
               update_amount = 0
               delete_amount = 0
               ddl_amount  = 0
               gather_time = datetime.datetime.now()
               logging.info("\033[1;36;40m[{}] apply diff ckpt : [{}/{}]!\033[0m".format(cfg['sync_tag'].split('_')[0],stream.log_file,stream.log_pos))
               flush_buffer(cfg)

            if isinstance(binlogevent, RotateEvent):
                logging.info("\033[1;34;40m[{}] binlog file has changed!\033[0m".format(cfg['sync_tag'].split('_')[0]))

            if isinstance(binlogevent, QueryEvent):
                cfg['binlog_pos'] = binlogevent.packet.log_pos
                event = {"schema": bytes.decode(binlogevent.schema), "query": binlogevent.query.lower()}
                if 'create' in event['query'] or 'drop' in event['query']  or 'alter' in event['query'] or 'truncate' in event['query']:
                    ddl = gen_ddl_sql(event['query'])
                    event['table'] = get_obj_name(event['query']).lower()
                    event['tab'] = event['schema']+'.'+event['table']

                    if check_sync(cfg,event,pks) and ddl is not None:
                       if check_mysql_tab_exists(cfg,event) == 0:
                          create_mysql_table(cfg,event)
                          full_sync_one(cfg, event)
                          types[event['schema']+'.'+event['table']] = get_col_type(cfg, event)
                          ddl_amount = ddl_amount +1
                       else:
                          logging.info('Execute DDL:{}'.format(ddl))
                          cfg['db_mysql_dest'].cursor().execute(ddl)

            if isinstance(binlogevent, DeleteRowsEvent) or \
                    isinstance(binlogevent, UpdateRowsEvent) or \
                        isinstance(binlogevent, WriteRowsEvent):

                for row in binlogevent.rows:
                    cfg['binlog_pos'] = binlogevent.packet.log_pos
                    event = {"schema": binlogevent.schema.lower(), "table": binlogevent.table.lower()}

                    if check_sync(cfg, event,pks):

                        if check_mysql_tab_exists(cfg, event) == 0:
                            event['tab'] = event['schema'] + '.' + event['table']
                            event['column'] = col[event['tab']]
                            create_mysql_table(cfg, event)
                            full_sync_one(cfg, event)
                            types[event['schema'] + '.' + event['table']] = get_col_type(cfg, event)

                        if isinstance(binlogevent, DeleteRowsEvent):
                            event["action"] = "delete"
                            event["data"]   = row["values"]
                            event['type']   = types[event['schema']+'.'+event['table']]
                            event['pks']    = pks[event['schema'] + '.' + event['table']]
                            event['pkn']    = pkn[event['schema']+'.'+event['table']]
                            event['tab']    = event['schema']+'.'+event['table']
                            event['sql']    = gen_sql(cfg,event)
                            write_sql(cfg,event)
                            delete_amount = delete_amount +1

                        elif isinstance(binlogevent, UpdateRowsEvent):
                            event["action"]        = "update"
                            event["after_values"]  = row["after_values"]
                            event["before_values"] = row["before_values"]
                            event['type']          = types[event['schema'] + '.' + event['table']]
                            event['pks']           = pks[event['schema'] + '.' + event['table']]
                            event['pkn']           = pkn[event['schema'] + '.' + event['table']]
                            event['tab']           = event['schema'] + '.' + event['table']
                            event['sql']           = gen_sql(cfg, event)
                            write_sql(cfg, event)
                            update_amount = update_amount +1

                        elif isinstance(binlogevent, WriteRowsEvent):
                            event["action"] = "insert"
                            event["data"]   = row["values"]
                            event['type']   = types[event['schema'] + '.' + event['table']]
                            event['pks']    = pks[event['schema'] + '.' + event['table']]
                            event['pkn']    = pkn[event['schema'] + '.' + event['table']]
                            event['tab']    = event['schema'] + '.' + event['table']
                            event['sql']    = gen_sql(cfg, event)
                            write_sql(cfg, event)
                            insert_amount = insert_amount +1

            if stream.log_file == cfg['full_binlog_file'] and (stream.log_pos + 31 == int(cfg['full_binlog_pos']) or stream.log_pos >=int(cfg['full_binlog_pos'])):
                stream.close()
                break
        logging.info("\033[0;36;40m[{}] apply diff logs ok!...\033[0m".format(cfg['sync_tag'].split('_')[0]))

    except Exception as e:
        logging.info('apply_diff_logs failure!')
        logging.info(traceback.format_exc())
    finally:
        stream.close()

def start_full_sync(cfg):
    logging.info("\033[0;36;40m[{}] start full sync...\033[0m".format(cfg['sync_tag'].split('_')[0]))
    full_sync_many(cfg)

def get_task_status(tag):
    cfg = {}
    url = 'http://210.13.35.136:20080/read_config_sync'
    res = requests.post(url, data={'tag': tag}, timeout=1).json()
    if res['code'] == 200:
        cfg = res['msg']
    else:
        logging.info('get_task_status load config failure:{0}'.format(res['msg']))
        sys.exit(0)

    if check_ckpt(cfg):
        pid = read_ckpt(cfg).get('pid')
        if pid :
            if pid == os.getpid():
               return False

            if pid in psutil.pids():
              return True
            else:
              return False
        else:
            return False
    else:
        logging.info('config file :{}.json not exists!'.format(cfg['sync_tag']))
        return False

if __name__ == "__main__":
    tag = ""
    debug = False
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    # check sys parameter
    if read_real_sync_status(tag) == None or read_real_sync_status(tag)['msg']['real_sync_status'] == 'STOP':
          logging.info("\033[1;37;40m sync task {} terminate!\033[0m".format(tag))
          sys.exit(0)


    # init logger
    logging.basicConfig(filename='/tmp/{}.{}.log'.format(tag, datetime.datetime.now().strftime("%Y-%m-%d")),
                        format='[%(asctime)s-%(levelname)s:%(message)s]',
                        level=logging.INFO, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    # check task
    if not get_task_status(tag):

       # call api get config
       cfg = get_config_from_db(tag)
       if cfg is None:
          logging.info('load config failure,exit sync!')
          sys.exit(0)

       # output parameter
       print_dict(cfg)

       # refresh pid
       cfg['pid'] = os.getpid()
       upd_ckpt(cfg)

       # init full sync
       start_full_sync(cfg)

       # apply diff logs from config  to full sync
       apply_diff_logs(cfg)

       # parse binlog incr sync
       start_incr_sync(cfg)
    else:
       logging.info('sync program:{} is running!'.format(tag))
