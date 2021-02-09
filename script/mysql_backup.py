#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/10 8:39
# @Author : 马飞
# @File : mysql_backup.py.py
# @Func : MySQL数据库备份工具
# @Software: PyCharm

import sys,os
import traceback
import warnings
import pymysql
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

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_year():
    return datetime.datetime.now().strftime("%Y")

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8')
    return conn

def get_db_mysql(config):
    return get_ds_mysql(config['db_ip'],config['db_port'],'',config['db_user'],config['db_pass'])

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

def read_config(tag):
    values = {
        'tag': tag
    }
    #print('values=', values)
    url = 'http://$$API_SERVER$$/read_config_backup'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    #write_log(res+','+str(res['code']))
    if res['code'] == 200:
        write_log('接口调用成功!')
        config=res['msg']
        config['year'] = get_year()
        config['day']  = get_date()
        config['bk_path']=config['bk_base']+'/'+get_date()
        config['db_mysql'] = get_db_mysql(config)
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
        'elaspsed_gzip'   : config['elaspsed_gzip'],
        'bk_base'         : config['bk_base'],
        'status'          : config['status']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    write_log('values='+values)
    url = 'http://$$API_SERVER$$/write_backup_total'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        write_log('接口调用成功!')
    else:
        write_log('接口调用失败!')

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
        'elaspsed_gzip'   : config['elaspsed_gzip'],
        'status'          : config['status'],
        'error'           : config['error']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    write_log('values='+ values)
    url = 'http://$$API_SERVER$$/write_backup_detail'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    #write_log(res+','+str(res['code']))
    if res['code'] == 200:
        write_log('接口调用成功!')
    else:
        write_log('接口调用失败!')

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

def get_path_size(path):
    r = os.popen('du -sh {0}'.format(path)).read()
    return r.split('\t')[0]

def get_file_size(file):
    r = os.popen('ls -lh {0}'.format(file)).read()
    return r.split(' ')[4]

def write_log(msg):
    file_name   = '/tmp/mysql_backup.log'
    file_handle = open(file_name, 'a+')
    file_handle.write(msg + '\n')
    file_handle.close()

def db_backup(config):
    print_dict(config)
    db=config['db_mysql']
    cr=db.cursor()
    if config['backup_databases']!='':
        v_sql = '''SELECT schema_name 
                      FROM information_schema.schemata 
                     WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')
                      and instr('{0}',schema_name)>0
                '''.format(config['backup_databases'])
    else:
        v_sql = '''SELECT schema_name 
                     FROM information_schema.schemata 
                    WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')                            
                '''
    cr.execute(v_sql)
    rs=cr.fetchall()

    bk_begin_time=get_now()
    n_elaspsed_backup_total=0
    n_elaspsed_gzip_total=0
    g_status='0'
    for db in list(rs):
        error  = ''
        status = '0'
        os.system('mkdir -p {0}'.format(config['bk_path']))
        write_log('Performing backup database {0}...'.format(db[0]))
        start_time     = get_now()
        file_name      = config['bk_path']+'/'+db[0]+'_'+get_date()+'.sql'
        err_name       = '/tmp/'+db[0]+'_'+get_date()+'.err'
        full_gzip_name = file_name+'.gz'
        gzip_name      = db[0]+'_'+get_date()+'.sql.gz'
        bk_cmd         = '{0} -u{1} -p{2} -h{3} --port {4} --single-transaction ' \
                        '--routines --force --databases {5} -r {6} &>{7}'.\
                        format(config['bk_cmd'],config['db_user'],config['db_pass'],
                               config['db_ip'] ,config['db_port'],db[0],file_name,err_name)
        write_log(bk_cmd)
        r=os.system(bk_cmd)
        if r!=0:
           error  = get_file_contents(err_name)
           error  = error.replace('Warning: Using a password on the command line interface can be insecure.','')
           status = '1'
           os.system('rm {0}'.format(err_name))
           #print(db[0],error)

        end_time = get_now()
        os.system('cd {0} && gzip -f {1}'.format(config['bk_path'],file_name))
        end_zip_time = get_now()

        config['db_name']     = db[0]
        config['create_date'] = get_date()
        config['file_name']   = gzip_name
        config['db_size']     = get_file_size(full_gzip_name)
        config['start_time']  = get_time2(start_time)
        config['end_time']    = get_time2(end_zip_time)
        config['elaspsed_backup'] = get_seconds(end_time, start_time)
        config['elaspsed_gzip']   = get_seconds(end_zip_time, end_time)
        config['status']          = status
        config['error']           = error
        if status=='1':
           g_status='1'
        write_backup_detail(config)
        n_elaspsed_backup_total=n_elaspsed_backup_total+config['elaspsed_backup']
        n_elaspsed_gzip_total=n_elaspsed_gzip_total+config['elaspsed_gzip']
        os.system('rm -f {0}'.format(err_name))

    bk_end_time = get_now()
    config['create_date']     = get_date()
    config['start_time']      = get_time2(bk_begin_time)
    config['end_time']        = get_time2(bk_end_time)
    config['total_size']      = get_path_size(config['bk_path'])
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip']   = n_elaspsed_gzip_total
    config['status']          = g_status
    write_backup_total(config)

    #delete recent 7 day data
    v_del='find {0} -name "*{1}*" -type d -mtime +{2} -exec rm -rf {} \;'.\
           format(config['bk_base'],config['year'],config['expire'])
    write_log(v_del)
    os.system(v_del)


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


