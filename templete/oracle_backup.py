#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/10 8:39
# @Author : 马飞
# @File : mysql_backup.py.py
# @Func : MySQL数据库备份工具
# @Software: PyCharm

import sys,os
from os.path import join, getsize
import traceback
import warnings
import pymssql
import datetime
import json
import urllib.parse
import urllib.request
import ssl

def get_now():
    return datetime.datetime.now()

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_time2(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")

def get_backup_time():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_year():
    return datetime.datetime.now().strftime("%Y")

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]


def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_seconds(a,b):
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

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

def read_config(tag):
    values = {
        'tag': tag
    }
    url = 'http://$$API_SERVER$$/read_config_backup'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    if res['code'] == 200:
        print('接口调用成功!')
        config=res['msg']
        config['year'] = get_year()
        config['day']  = get_date()
        config['bk_path']=config['bk_base']+'/'+get_date()
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        sys.exit(0)

def write_backup_total(config):
    v_tag = {
        'db_tag'          : config['db_tag'],
        'create_date'     : config['create_date'],
        'total_size'      : config['total_size'],
        'start_time'      : config['start_time'],
        'end_time'        : config['end_time'],
        'elaspsed_backup' : config['elaspsed_backup'],
        'elaspsed_gzip'   : 0,
        'bk_base'         : config['bk_base'],
        'status'          : config['status']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('values=',values)
    url = 'http://$$API_SERVER$$/write_backup_total'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req  = urllib.request.Request(url, data=data)
    res  = urllib.request.urlopen(req, context=context)
    res  = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        print('接口调用成功!')
    else:
        print('接口调用失败!')

def write_backup_detail(config):
    v_tag = {
        'db_tag'          : config['db_tag'],
        'db_name'         : config['db_name'],
        'create_date'     : config['create_date'],
        'bk_path'         : config['bk_path'],
        'file_name'       : config['file_name'],
        'db_size'         : config['db_size'],
        'start_time'      : config['start_time'],
        'end_time'        : config['end_time'],
        'elaspsed_backup' : config['elaspsed_backup'],
        'elaspsed_gzip'   : 0,
        'status'          : config['status'],
        'error'           : config['error']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('values=',values)
    url  = 'http://$$API_SERVER$$/write_backup_detail'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req  = urllib.request.Request(url, data=data)
    res  = urllib.request.urlopen(req, context=context)
    res  = json.loads(res.read())
    if res['code'] == 200:
        print('接口调用成功!')
    else:
        print('接口调用失败!')

def get_file_contents(filename):
    file_handle = open(filename, 'r')
    line = file_handle.readline()
    lines = ''
    while line:
        lines = lines + line
        line = file_handle.readline()
    lines = lines + line
    file_handle.close()
    return lines[0:-1]

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_path_size(path):
    r = os.popen('du -sh {0}'.format(path)).read()
    return r.split('\t')[0]

def format_file_size(filesize):
    try:
      if filesize/1024/1024/1024>1:
         return str(round(filesize/1024/1024/1024,2))+'G'
      elif filesize/1024/1024>1:
         return str(round(filesize/1024/1024,2))+'M'
      elif filesize/1024>1:
         return str(round(filesize/1024,2))+'K'
      else:
         return str(filesize ) + 'B'
    except:
      return 0

def get_file_size(file):
    try:
      return getsize(file.replace('\\','\\\\'))
    except:
      return 0

def get_filename(db):
    return '{}_{}'.format(db, get_backup_time())


def db_backup(config):
    print_dict(config)
    bk_begin_time=get_now()
    n_elaspsed_backup_total=0
    n_total_size=0
    g_status='0'
    for db in config['backup_databases'].split(','):
        status = '0'
        print('Performing backup database {0}...'.format(db))
        start_time        = get_now()
        file_name         = config['db_service']+'_'+get_filename(db)
        config['newpass'] = aes_decrypt(config['db_pass'], config['db_user'])
        bk_env            = 'set ORACLE_SID={}'.format(config['db_service'])
        bk_cmd            = '''
{} && {} {}/{}  directory=beifen schemas={}  COMPRESSION=ALL dumpfile="{}.dmp"  logfile="{}.log"
'''.format(bk_env,
           config['bk_cmd'],
           config['db_user'],
           config['newpass'],
           db,
           file_name,
           file_name,
          )
        print(bk_cmd)
        try:
          os.system(bk_cmd)
        except:
          status='1'

        end_time                  = get_now()
        file_size                 = get_file_size(config['bk_base']+'\\'+file_name+'.dmp')
        print('file_size=',file_size)
        config['db_name']         = db
        config['create_date']     = get_date()
        config['file_name']       = file_name+'.dmp'
        config['db_size']         = format_file_size(file_size)
        config['start_time']      = get_time2(start_time)
        config['end_time']        = get_time2(end_time)
        config['elaspsed_backup'] = get_seconds(end_time, start_time)
        config['status']          = status
        config['error']           = 'success' if status=='0' else 'failure'

        if status=='1':
           g_status='1'
        write_backup_detail(config)
        n_elaspsed_backup_total=n_elaspsed_backup_total+config['elaspsed_backup']
        n_total_size=n_total_size+config['elaspsed_backup']+file_size

    bk_end_time               = get_now()
    config['create_date']     = get_date()
    config['start_time']      = get_time2(bk_begin_time)
    config['end_time']        = get_time2(bk_end_time)
    config['total_size']      = format_file_size(n_total_size)
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip']   = 0
    config['status']          = g_status
    write_backup_total(config)

    print('delete recent {} day backup...'.format(config['expire']))
    v_del='''forfiles /p "{}" /s /m *.* /d -{} /c "cmd /c del @path"'''.format(config['bk_base'],config['expire'])
    print(v_del)
    os.system(v_del)

    print('remote backup...')
    v_remote ="""D:\\cwRsync\\rsync.bat '{}'""".format(file_name.split('_')[-1])
    print(v_remote)
    os.system(v_remote)


def main():
    warnings.filterwarnings("ignore")
    tag=''
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
           tag = sys.argv[p + 1]

    if tag=='':
       print('Please input tag value!')
       sys.exit(0)

    config=read_config(tag)
    db_backup(config)

if __name__ == "__main__":
     main()


