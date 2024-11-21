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
import cx_Oracle

slow_tj_log = '''
SELECT 
   x.dbid,
   x.username,
   x.sql_id,
   x.priority,
   x.first_time,
   x.last_time,
   x.executions,
   x.total_time,
   x.avg_time,
   x.rows_processed,
   x.disk_reads,
   x.buffer_gets
FROM (
SELECT t.dbid,
       t.username,
       t.sql_id,
       CASE
         WHEN ROUND(SUM(t.total_time),0) >=600 OR ROUND(SUM(t.total_time)/SUM(t.executions), 0)>=200 OR ROUND(SUM(t.executions),0)>=60 THEN '★★★★★'
         WHEN ROUND(SUM(t.total_time),0) >=400 OR ROUND(SUM(t.total_time)/SUM(t.executions), 0)>=100 OR ROUND(SUM(t.executions),0)>=30 THEN '★★★★'
         WHEN ROUND(SUM(t.total_time),0) >=200 OR ROUND(SUM(t.total_time)/SUM(t.executions), 0)>=50 OR ROUND(SUM(t.executions),0)>=10 THEN '★★★'
         WHEN ROUND(SUM(t.total_time),0) =100 OR ROUND(SUM(t.total_time)/SUM(t.executions), 0)>=30 OR ROUND(SUM(t.executions),0)>=5 THEN '★★'
         ELSE '★'
      END AS priority, 
      MIN(t.startup_time) AS first_time,
      MAX(t.startup_time) AS last_time,
      ROUND(SUM(t.executions),0) AS executions,
      ROUND(SUM(t.total_time),0) AS total_time,
      ROUND(SUM(t.total_time)/SUM(t.executions), 0) AS avg_time,
      ROUND(SUM(t.rows_processed),0) AS rows_processed,
      ROUND(SUM(t.disk_reads),0)   AS disk_reads,
      ROUND(SUM(t.buffer_gets),0) AS buffer_gets
FROM (SELECT a.dbid,
             e.username,
             a.sql_id,
             to_char(b.begin_interval_time, 'yyyy-mm-dd hh24:mi:ss') AS startup_time,
             SUM(a.executions_delta) AS executions, 
             SUM(a.elapsed_time_delta)/1000000 AS total_time ,
             SUM(a.elapsed_time_delta)/1000000/SUM(a.executions_delta) AS avg_time,
             SUM(a.rows_processed_delta) AS rows_processed,              
             SUM(a.disk_reads_delta)     AS disk_reads, 
             SUM(a.buffer_gets_delta)    AS buffer_gets
      FROM dba_hist_sqlstat a, dba_hist_snapshot b, dba_users e
      WHERE a.snap_id = b.snap_id
        AND a.dbid = b.dbid
        AND a.instance_number = b.instance_number
        AND a.parsing_user_id = e.user_id
        AND b.begin_interval_time >= trunc(SYSDATE,'dd')
        AND b.begin_interval_time< trunc(SYSDATE+1,'dd')
        AND a.parsing_schema_name = USER
     GROUP BY a.dbid,e.username,a.sql_id, to_char(b.begin_interval_time, 'yyyy-mm-dd hh24:mi:ss')) t, sys.WRH$_SQLTEXT q 
    WHERE t.sql_id = q.sql_id AND t.dbid = q.dbid AND executions > 0 
   GROUP BY t.dbid,t.username,t.sql_id
  ORDER BY 9 DESC，11 desc
) X WHERE rownum<=20
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

def capital_to_lower(dict_info):
    new_dict = {}
    for i, j in dict_info.items():
        new_dict[i.lower()] = j
    return new_dict

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

def get_ds_oracle(ip,port,instance,user,password):
    tns  = cx_Oracle.makedsn(ip,int(port),instance)
    db   = cx_Oracle.connect(user,password,tns)
    return db

def get_oracle_slowlog(config):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute(slow_tj_log)
        rs=cr.fetchall()
        cr.close()
        return rs
    except:
        traceback.print_exc()
        return None

def get_oracle_slowlog_dict(config):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute(slow_tj_log)
        cols = [col[0] for col in cr.description]
        cr.rowfactory = lambda *args: dict(zip(cols, args))
        rs = cr.fetchall()
        nw = []
        for r in rs:
            nw.append(capital_to_lower(r))

        for r in nw:
            r['sql_text'] = get_text_from_sqlid(config, r['sql_id'])
            r['ds_id'] = config['ds_id']
        cr.close()
        return nw
    except:
        traceback.print_exc()
        return None

def get_text_from_sqlid(config,p_sqlid):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute("select  sql_text from sys.WRH$_SQLTEXT q where q.sql_id='{}'".format(p_sqlid))
        rs = cr.fetchone()
        return rs[0].read()
    except:
        traceback.print_exc()
        return None

def write_slow_log(d_log):
    try:
        v_tag  = d_log
        v_msg  = json.dumps(v_tag)
        values = {
            'tag': v_msg
        }
        url     = 'http://$$API_SERVER$$/write_slow_log_oracle'
        context = ssl._create_unverified_context()
        data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        req     = urllib.request.Request(url, data=data)
        res     = urllib.request.urlopen(req, context=context)
        res     = json.loads(res.read())
        print(res, res['code'])
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
            config['db_oracle'] = get_ds_oracle(db_ip, db_port, db_service, db_user, db_pass)
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
    rs = get_oracle_slowlog_dict(config)
    write_slow_log(rs)



