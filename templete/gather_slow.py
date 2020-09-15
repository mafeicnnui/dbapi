#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : 马飞
# @File : analysis_slowlog_detail_ecs.py.py
# @Software: PyCharm

import json
import re
import hashlib
import pymysql
import traceback
import warnings
import sys
import urllib.parse
import urllib.request
import ssl
import os
import datetime

def get_day():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_time():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%Y-%m-%d %H:%M:%S')
    return time1_str


def get_db(ip,port,service ,user,password):
    try:
        conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',connect_timeout=3)
        return conn
    except  Exception as e:
        print('get_db exceptiion:' + traceback.format_exc())
        return None

def get_md5(url):
    """
    由于hash不处理unicode编码的字符串（python3默认字符串是unicode）
        所以这里判断是否字符串，如果是则进行转码
        初始化md5、将url进行加密、然后返回加密字串
    """
    if isinstance(url, str):
        url = url.encode("utf-8")
    md = hashlib.md5()
    md.update(url)
    return md.hexdigest()

def get_log(parameter):
    cmd = "pt-query-digest {} --no-report --filter 'print Dumper $event'  --sample 1 > {} "\
          .format(parameter['slow_query_log_file'], parameter['slow_query_log_file']+'.pt')
    os.system(cmd)
    with open(parameter['slow_query_log_file']+'.pt', 'r') as f:
        r = f.read()
    return r

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def write_slow_log(d_log):
    v_tag =d_log
    print('write_slow_log=',v_tag)
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    url     = 'http://$$API_SERVER$$/write_slow_log'
    context = ssl._create_unverified_context()
    data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req     = urllib.request.Request(url, data=data)
    res     = urllib.request.urlopen(req, context=context)
    res     = json.loads(res.read())
    print(res, res['code'])
    if res['code'] != 200:
        print('Interface write_slow_log call failed!')

def write_db(p_cfg,d_log):
    db = p_cfg['db']
    cr = db.cursor()
    st = '''
           insert into t_slow_detail
               (sql_id,templete_id,finish_time,USER,HOST,ip,thread_id,query_time,lock_time,
                rows_sent,rows_examined,db,sql_text,finger,bytes,cmd,pos_in_log)
          values('{}','{}',STR_TO_DATE('{}', '%Y%m%d %H:%i:%s'),'{}','{}','{}','{}','{}','{}',
                 '{}','{}','{}','{}','{}','{}','{}','{}')
         '''.format(d_log['sql_id'],
                    d_log['templete_id'],
                    d_log['finish_time'],
                    d_log['user'],
                    d_log['host'],
                    d_log['ip'],
                    d_log['thread_id'],
                    d_log['query_time'],
                    d_log['lock_time'],
                    d_log['rows_sent'],
                    d_log['rows_examined'],
                    d_log['db'],
                    format_sql(d_log['sql_text']),
                    format_sql(d_log['finger']),
                    d_log['bytes'],
                    d_log['cmd'],
                    d_log['pos_in_log']
                    )
    #print(st)
    cr.execute(st)
    db.commit()
    p_cfg['row'] = p_cfg['row']+1
    print('\rinsert {} rows!'.format(p_cfg['row']),end='')

def parse_log(p_log,p_cfg):
    d_log = {}
    rows  = p_log.split('\n')[1:-1]
    d_log['lock_time']     = rows[0].split('=>')[1].replace("'",'').replace(',','').replace(' ','')
    d_log['query_time']    = rows[1].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['rows_examined'] = rows[2].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['rows_sent']     = rows[3].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['thread_id']     = rows[4].split('=>')[1].replace("'", '').replace(',', '').replace(' ', '')
    d_log['sql_text']      = re.sub(' +', ' ',''.join(rows[5:]).split(',  bytes')[0].split('arg =>')[1][2:-1].replace('\\',''))
    d_log['sql_id']        = get_md5(d_log['sql_text'])
    d_log['bytes']         = re.sub(' +', ' ',''.join(rows[5:]).split(',  bytes')[1]).split(',')[0].replace(' => ','')
    d_log['cmd']           = re.sub(' +', ' ',''.join(rows[5:]).split(',  cmd')[1]).split(',')[0].replace(' => ', '').replace("'","")
    if ''.join(rows[5:]).find('db =>')>0:
       d_log['db']         = re.sub(' +', ' ',''.join(rows[5:]).split(',  db')[1]).split(',')[0].replace(' => ', '').replace("'", "")
    d_log['finger']        = re.sub(' +', ' ',''.join(rows[5:]).split(',  host')[0].split('fingerprint =>')[1][2:-1].replace('\\',''))
    d_log['templete_id']   = get_md5(d_log['finger'])
    d_log['host']          = re.sub(' +', ' ',''.join(rows[5:]).split(',  host')[1]).split(',')[0].replace(' => ','').replace("'","")
    d_log['ip']            = re.sub(' +', ' ',''.join(rows[5:]).split(',  ip')[1]).split(',')[0].replace(' => ','').replace("'","")
    d_log['pos_in_log']    = re.sub(' +', ' ',''.join(rows[5:]).split(',  pos_in_log')[1]).split(',')[0].replace(' => ', '')
    d_log['finish_time']   = '20'+re.sub(' +', ' ',''.join(rows[5:]).split(',  ts')[1]).split(',')[0].replace(' => ', '').replace("'","")
    d_log['user']          = re.sub(' +', ' ',''.join(rows[5:]).split(',  user')[1]).split(',')[0].replace(' => ', '').replace("'","").replace('};','')
    d_log['inst_id']       = p_cfg['inst_id']
    write_slow_log(d_log)

def get_ds_mysql(ip,port,service ,user,password):
    try:
        conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',connect_timeout=3)
        return conn
    except  Exception as e:
        print('get_ds_mysql exceptiion:' + traceback.format_exc())
        return None

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

def write_config(config):
    config_mysqld   = '[mysqld]\n'
    config_mysql    = '[mysql]\n'
    config_client   = '[client]\n'
    parameter       = {}

    for c in config['cfg']:
        if  c['TYPE'] == 'mysqld':
            if c['VALUE'].split('=')[0] == 'datadir' \
                 or c['VALUE'].split('=')[0] == 'socket' \
                    or c['VALUE'].split('=')[0] == 'log-error' :
                n_val = c['VALUE'].format(config['dver'],config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif  c['VALUE'].split('=')[0] == 'pid-file':
                n_val = c['VALUE'].format(config['dver'],config['db_port'],config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['VALUE'].split('=')[0] == 'port':
                n_val = c['VALUE'].format(config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['VALUE'].split('=')[0] == 'slow_query_log':
                n_val = c['VALUE'].format('ON' if config['status'] == '1' else 'OFF')
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['VALUE'].split('=')[0] == 'slow_query_log_file':
                n_val = c['VALUE'].format(parameter['datadir']).replace('YYYYMMDD',get_day())
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['VALUE'].split('=')[0] == 'long_query_time':
                n_val = c['VALUE'].format(config['query_time'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            else:
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['NAME'],c['VALUE'])
                parameter[c['VALUE'].split('=')[0]] = c['VALUE'].split('=')[1]

        elif c['TYPE'] == 'mysql':
            config_mysql  = config_mysql + '#{}\n{}\n'.format(c['NAME'], c['VALUE'])
            parameter[c['VALUE'].split('=')[0]] = c['VALUE'].split('=')[1]
        elif c['TYPE'] == 'client':
            if c['VALUE'].split('=')[0] == 'socket' :
                n_val = c['VALUE'].format(config['dver'],config['db_port'])
                config_client = config_client + '#{}\n{}\n'.format(c['NAME'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            else:
                config_client = config_client + '#{}\n{}\n'.format(c['NAME'], c['VALUE'])
                parameter[c['VALUE'].split('=')[0]] = c['VALUE'].split('=')[1]
        else:
            pass
    config_file = config_mysqld+'\n'+config_mysql+'\n'+config_client
    print(config_file)
    filename    = 'mysql_{}.cnf'.format(config['db_port'])
    fullname    = '/tmp/{}'.format(filename)
    with open(fullname, 'w') as f:
       f.write(config_file)
    os.system('sudo cp {} /etc/'.format(fullname))
    config['cfile']  = '/etc/'+filename
    return parameter

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
        config             = res['msg']
        config['dpath']    = config['dpath'][0]['mysql_download_url']
        config['dfile']    = config['dpath'].split('/')[-1]
        config['dver']     = config['dfile'].split('-')[1]
        config['lpath']    = config['dfile'].replace('.tar.gz', '')
        # config['db_ip']    = '127.0.0.1'
        config['db_pass']  = aes_decrypt(config['db_pass'],
                                         config['db_user'])
        config['db_mysql'] = get_ds_mysql(config['db_ip'],
                                          config['db_port'],
                                          config['db_service'],
                                          config['db_user'],
                                          config['db_pass'])
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        return None

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(30,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

def write_inst_log(item,msg):
    try:
        v_tag = {
            'inst_id'    : item.get('inst_id'),
            'message'    : msg,
            'type'       : item.get('mode')
        }
        v_msg = json.dumps(v_tag)
        values = {
            'tag': v_msg
        }
        url = 'http://$$API_SERVER$$/write_db_inst_log'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        if res['code'] != 200:
            print('Interface write_inst_log call failed!',res['msg'])
        else:
            print('Interface write_inst_log call success!')
    except:
        print(traceback.print_exc())

def upd_var(config):
    parameter = write_config(config)
    print('updating mysql slo query....')
    for s in config['step_slow']:
        if s['id'] in (71,):
           s['cmd'] = s['cmd'].format(parameter['basedir'],
                                      config['db_pass'],
                                      parameter['port'],
                                      parameter['socket'],
                                      parameter['slow_query_log'], 'ON' if config['status'] == '1' else 'OFF')
        if s['id'] in (72,):
            s['cmd'] = s['cmd'].format(parameter['basedir'],
                                       config['db_pass'],
                                       parameter['port'],
                                       parameter['socket'],
                                       parameter['slow_query_log_file'].replace('YYYYMMDD', get_day()))
        if s['id'] in (73,):
           s['cmd'] = s['cmd'].format(parameter['basedir'],
                                      config['db_pass'],
                                      parameter['port'],
                                      parameter['socket'],
                                      config['query_time'])

        print('{} Execute:{}'.format(get_time(),s['cmd']))
        os.system(s['cmd'])
    print('updating mysql slo query....ok!')
    write_inst_log(config, '慢日志配置已更新!')

# def upd_var(config,parameter):
#     cr = config['db_mysql'].cursor()
#     v1 = 'set global slow_query_log={}'.format(parameter['slow_query_log'],'ON' if config['status'] == '1' else 'OFF')
#     v2 = 'set global slow_query_log_file={}'.format(parameter['slow_query_log_file'].replace('YYYYMMDD',get_day()))
#     v3 = 'set global long_query_time={}'.format(config['query_time'])
#     cr.execute(v1)
#     cr.execute(v2)
#     cr.execute(v3)
#     print(v1)
#     print(v2)
#     print(v3)
#     cr.close()

def cut(config):
    parameter = write_config(config)
    print_dict(config)
    print_dict(parameter)
    print('生成mysql配置文件:/etc/{}'.format(config['cfile']))
    upd_var(config,parameter)
    print('mysql慢日志参数已更新!')
    write_inst_log(config, '慢日志配置已更新!')
    write_inst_log(config, '慢日志切割已完成!')


def stats(config):
    parameter = write_config(config)
    for log in get_log(parameter).split('$VAR1 =')[1:]:
        print('log=',log)
        parse_log(log,config)
    print('慢日志采集已完成!')

if __name__ == "__main__":
    config = ""
    mode   = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-mode":
            mode = sys.argv[p + 1]
        else:
            pass
    # init
    config = init(config)
    config['mode'] = mode
    config['row']  = 0

    # create or destroy db
    if mode == "" or mode not in ('cut', 'stats','update'):
        print('Usage ./gather_slow.py -tag slow_id -mode cut|stats|update')
        sys.exit(0)
    elif mode == "cut":
        cut(config)
    elif mode == "stats":
        stats(config)
    elif mode == "update":
        upd_var(config)
    else:
        pass

