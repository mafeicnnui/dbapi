#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : ma.fei
# @File : analysis_slowlog_detail_ecs.py.py
# @Software: PyCharm

import json
import traceback
import warnings
import sys
import urllib.parse
import urllib.request
import ssl
import pymssql

slow_tj_log = '''
    select 
       top 20
       db_name(a.dbid) dbname,
       a.loginame,
       a.hostname,
       CONVERT(varchar(100), last_batch, 20)  as start_time,
       CONVERT(varchar(100), dateadd(second,DATEDIFF(s,a.last_batch,GETDATE()),last_batch), 20)   as end_time, 
       DATEDIFF(s,a.last_batch,GETDATE()) as query_time,
       a.physical_io,
       a.cmd,
       b.text as sql_text
    From sys.sysprocesses a
    outer apply sys.dm_exec_sql_text(a.sql_handle) b
    where a.spid>50 and a.status<>'sleeping' and spid<>@@SPID
     and b.text is not null
     and DATEDIFF(s,a.last_batch,GETDATE()) >{}
    order by DATEDIFF(s,a.last_batch,GETDATE()),physical_io desc
'''

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(30,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def aes_decrypt(p_password,p_key):
    values = {
        'password': p_password,
        'key':p_key
    }
    url = 'http://$$API_SERVER$$/read_db_decrypt'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    if res['code'] == 200:
        print('接口read_db_decrypt 调用成功!')
        config = res['msg']
        return config
    else:
        print('接口read_db_decrypt 调用失败!,{0}'.format(res['msg']))
        sys.exit(0)

def get_ds_sqlserver_test(ip, port, service, user, password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service, charset='utf8',timeout=3)
    return conn

def get_mssql_slowlog(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = config['db_service']
        db_user    = config['db_user']
        db_pass    = aes_decrypt(config['db_pass'], db_user)
        db         = get_ds_sqlserver_test(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor(as_dict=True)
        print(slow_tj_log.format(config['query_time']))
        cr.execute(slow_tj_log.format(config['query_time']))
        rs=cr.fetchall()
        return rs
        db.commit()
        cr.close()
        return rs
    except Exception as e:
        print('get_mssql_slowlog exceptiion:' + traceback.format_exc())
        return 0



def write_slow_log(d_log):
    try:
        v_tag  = d_log
        v_msg  = json.dumps(v_tag)
        values = {
            'tag': v_msg
        }
        print('values=',values)
        url     = 'http://$$API_SERVER$$/write_slow_log_mssql'
        context = ssl._create_unverified_context()
        data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        req     = urllib.request.Request(url, data=data)
        res     = urllib.request.urlopen(req, context=context)
        res     = json.loads(res.read())
        print('res=',res)
        if res['code'] != 200:
            print('Interface write_slow_log call failed!')
        else:
            print('Interface write_slow_log call success!')
    except:
        print(traceback.print_exc())

def get_config_from_db(slow_id):
    values = {
        'slow_id': slow_id
    }
    print('values=', values)
    url = 'http://$$API_SERVER$$/read_slow_config'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    print('data=', data)
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        print('接口调用成功!')
        config  = res['msg']
        db_ip = config['db_ip']
        db_port = config['db_port']
        db_service = config['db_service']
        db_user = config['db_user']
        db_type = config['db_type']
        if db_type == '1':
            db_pass = aes_decrypt(config['db_pass'], db_user)
            config['db_string'] = 'msyql://' + db_ip + ':' + db_port + '/' + db_service
            print('get_config_from_db=', db_ip, db_port, db_service, db_user, db_type, db_pass)
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        return None

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

if __name__ == "__main__":
    config = ""
    mode   = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            slow_id = sys.argv[p + 1]
        else:
            pass
    # init
    config = init(slow_id)
    rs = get_mssql_slowlog(config)
    for r in rs:
        r['sql_id'] = str(hash(r['sql_text']))
        r['ds_id'] = config['ds_id']
    write_slow_log(rs)

