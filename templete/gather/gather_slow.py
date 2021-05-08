#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : ma.fei
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


def get_hour():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%H')
    return time1_str

def get_min():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%M')
    return time1_str


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
    if isinstance(url, str):
        url = url.encode("utf-8")
    md = hashlib.md5()
    md.update(url)
    return md.hexdigest()

def get_log_ecs(parameter):
    cmd = "pt-query-digest {} --no-report --filter 'print Dumper $event'  --sample 1 > {} "\
          .format(parameter['slow_query_log_file'], parameter['slow_query_log_file']+'.pt')
    print(cmd)
    os.system(cmd)
    with open(parameter['slow_query_log_file']+'.pt', 'r') as f:
        r = f.read()
    return r

def check_process_status(p_db_ip):
    try:
      r=os.popen('ps -ef|grep -v grep | grep pt-query-digest | grep {} | wc -l'.format(p_db_ip)).read()
      return int(r.split('\n')[0])
    except:
      return 0

def gen_log_file(config):
    dir = os.path.join(config['script_path'], 'slowlog')
    log = os.path.join(dir,'slow_query_log_{}_{}_{}.log'.format(get_day(),config['slow_id'],get_hour()))
    print('Generate log file :{}'.format(log))
    with open(log, 'w', encoding='utf-8') as f:
        f.write('初始化日志格式为UTF-8格式。。。')

def get_log_rds(config):
    dir = os.path.join(config['script_path'],'slowlog')
    log = os.path.join(dir,'slow_query_log_{}_{}_{}.log'.format(get_day(),config['slow_id'],get_hour()))
    cmd = "mkdir -p {}".format(dir)
    os.system(cmd)
    print('directory {} created!'.format(dir))

    print('current miniute:{}'.format(int(get_min())))
    if int(get_min()) == 0:
       print('Switch log file success!')
       print('slow log file:{}'.format(log))
       os.system("ps -ef |grep pt-query-digest | awk '{print $2}' |xargs kill -9")

    # print('ps -ef | grep pt-query-digest | grep {} | wc -l'.format(config['inst_id']))
    # os.system('ps -ef|grep pt-query-digest | grep {} | wc -l'.format(config['inst_id']))

    if check_process_status(config['db_ip']) == 0:
       gen_log_file(config)
       print('starting slow log stats task...')
       cmd = "nohup pt-query-digest --processlist h={},u={},p='{}' --charset=utf8 --interval {} --output slowlog --run-time {} >>{} &"\
              .format(config['db_ip'],config['db_user'],config['db_pass'],
                      config['query_time'],config['exec_time'],log)
       print(cmd)
       os.system(cmd)
    else:
       print('pt-query-digest --processlist task is running...')

    print('Reading slow log :{}'.format(log))
    with open(log, 'r') as f:
        r = f.read()
    return r

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def write_slow_log(d_log):
    try:
        v_tag  = d_log
        v_msg  = json.dumps(v_tag)
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
    except:
        print(traceback.print_exc())

def parse_ecs_log(p_log,p_cfg):
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
    d_log['finish_time']   = datetime.datetime.strptime(d_log['finish_time'], "%Y%m%d %H:%M:%S")
    d_log['finish_time']   = datetime.datetime.strftime(d_log['finish_time'], '%Y-%m-%d %H:%M:%S')
    d_log['user']          = re.sub(' +', ' ',''.join(rows[5:]).split(',  user')[1]).split(',')[0].replace(' => ', '').replace("'","").replace('};','')
    d_log['inst_id']       = p_cfg['inst_id']
    d_log['db_id']         = p_cfg['ds_id']
    p_cfg['sync_time']     = d_log['finish_time']

    if datetime.datetime.strptime(d_log['finish_time'], "%Y-%m-%d %H:%M:%S") > datetime.datetime.strptime(p_cfg['last_sync_time'], "%Y-%m-%d %H:%M:%S"):
       write_slow_log(d_log)
       print('slow log parse success! {}'.format(d_log['finish_time']))

def write_sync_time(p_cfg):
    d_rq  ={}
    dir   = os.path.join(p_cfg['script_path'], 'slowlog')
    d_rq['finish_time']  = p_cfg['sync_time']
    with open(dir+'/slow_sync_{}.ini'.format(p_cfg['inst_id']), 'w') as f:
        f.write(json.dumps(d_rq, ensure_ascii=False, indent=4, separators=(',', ':')))

def read_sync_time(p_cfg):
    try:
        d_rq = {}
        dir  = os.path.join(p_cfg['script_path'],'slowlog')
        with open(dir+'/slow_sync_{}.ini'.format(p_cfg['inst_id']), 'r') as f:
            d_rq=json.loads(f.read())
        return d_rq['finish_time']
    except:
        return get_time()


def write_sync_time_ecs(p_config,p_parameter):
    d_rq  ={}
    d_rq['finish_time']  = p_config['sync_time']
    with open(p_parameter['datadir']+'/slow_sync_{}.ini'.format(p_config['inst_id']), 'w') as f:
        f.write(json.dumps(d_rq, ensure_ascii=False, indent=4, separators=(',', ':')))

def read_sync_time_ecs(p_config,p_parameter):
    try:
        with open(p_parameter['datadir']+'/slow_sync_{}.ini'.format(p_config['inst_id']), 'r') as f:
            d_rq=json.loads(f.read())
        return d_rq['finish_time']
    except:
        return get_time()


def parse_log_rds(p_log,p_cfg):
    d_log = {}
    rows  = p_log.split('\n')
    # first row get Time
    v_finish_time                 =  datetime.datetime.strptime(rows[0][1:-1],"%Y-%m-%dT%H:%M:%S")
    d_log['finish_time']          =  datetime.datetime.strftime(v_finish_time, '%Y-%m-%d %H:%M:%S')
    p_cfg['sync_time']            = d_log['finish_time']
    d_finish_time                 =  datetime.datetime.strptime(d_log['finish_time'] ,"%Y-%m-%d %H:%M:%S")
    if  d_finish_time > datetime.datetime.strptime(config['last_sync_time'] ,"%Y-%m-%d %H:%M:%S") :
        # second row get User and Host
        d_log['user']             = rows[1].split('# User@Host:')[1].split('@')[0].split('[')[0][1:]
        d_log['host']             = rows[1].split('# User@Host:')[1].split('@')[1].split('[')[0][1:-1].split(':')[0]

        # fourth row get Query_time,Lock_time,Rows_sent,Rows_examined
        d_log['query_time']       = rows[2].split('# Query_time:')[1].split(' ')[1]
        d_log['lock_time']        = rows[2].split('Lock_time:')[1].split(' ')[1]
        d_log['rows_sent']        = rows[2].split('Rows_sent:')[1].split(' ')[1]
        d_log['rows_examined']    = rows[2].split('Rows_examined:')[1][1:]

        # fifth row get db
        d_log['db']               = rows[3].split('use ')[1][0:-1]

        # sixth row get sql,bytes
        d_log['sql_text']         =  re.sub(' +', ' ', ''.join(rows[4:]).replace('\n',''))
        d_log['bytes']            =  len(re.sub(' +', ' ', ''.join(rows[4:]).replace('\n', '')).encode())
        d_log['sql_id']           =  get_md5(d_log['sql_text'])
        d_log['inst_id']          = p_cfg['inst_id']
        d_log['db_id']            = p_cfg['ds_id']
        if d_log['sql_text'] != '':
            write_slow_log(d_log)
            print('slow log parse success! {}'.format(d_log['finish_time']))

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
        if  c['type'] == 'mysqld':
            if c['value'].split('=')[0] == 'datadir' \
                 or c['value'].split('=')[0] == 'socket' \
                    or c['value'].split('=')[0] == 'log-error' :
                n_val = c['value'].format(config['dver'],config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif  c['value'].split('=')[0] == 'pid-file':
                n_val = c['value'].format(config['dver'],config['db_port'],config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['value'].split('=')[0] == 'port':
                n_val = c['value'].format(config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['value'].split('=')[0] == 'slow_query_log':
                n_val = c['value'].format('ON' if config['status'] == '1' else 'OFF')
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['value'].split('=')[0] == 'slow_query_log_file':
                n_val = c['value'].format(parameter['datadir']).replace('YYYYMMDD',get_day())
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            elif c['value'].split('=')[0] == 'long_query_time':
                n_val = c['value'].format(config['query_time'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            else:
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'],c['value'])
                parameter[c['value'].split('=')[0]] = c['value'].split('=')[1]

        elif c['type'] == 'mysql':
            config_mysql  = config_mysql + '#{}\n{}\n'.format(c['name'], c['value'])
            parameter[c['value'].split('=')[0]] = c['value'].split('=')[1]
        elif c['type'] == 'client':
            if c['value'].split('=')[0] == 'socket' :
                n_val = c['value'].format(config['dver'],config['db_port'])
                config_client = config_client + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            else:
                config_client = config_client + '#{}\n{}\n'.format(c['name'], c['value'])
                parameter[c['value'].split('=')[0]] = c['value'].split('=')[1]
        else:
            pass
    config_file = config_mysqld+'\n'+config_mysql+'\n'+config_client
    print(config_file)

    if config['is_rds'] == 'N':
       filename    = 'mysql_{}.cnf'.format(config['db_port'])
       fullname    = '/tmp/{}'.format(filename)
       with open(fullname, 'w') as f:
          f.write(config_file)
       os.system('sudo cp {} /etc/'.format(fullname))
       config['cfile']  = '/etc/'+filename

    print_dict(parameter)
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
        config  = res['msg']
        if config['db_type'] == '0' and config['inst_id'] != '':
           config['dpath']    = config['dpath'][0]['mysql_download_url']
           config['dfile']    = config['dpath'].split('/')[-1]
           config['dver']     = config['dfile'].split('-')[1]
           config['lpath']    = config['dfile'].replace('.tar.gz', '')

        config['db_pass']  = aes_decrypt(config['db_pass'], config['db_user'])
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
    print('updating mysql slow query....')
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
    print('updating mysql slow query....ok!')
    write_inst_log(config, '慢日志配置已更新!')

def cut(config):
    write_config(config)
    print_dict(config)
    print('生成mysql配置文件:/etc/{}'.format(config['cfile']))
    upd_var(config)
    print('mysql慢日志参数已更新!')
    write_inst_log(config, '慢日志配置已更新!')

def stats(config):
    if config['db_type'] == '0' and config['inst_id'] != '':
        parameter = write_config(config)
        i_counter = 0
        config['last_sync_time'] = read_sync_time_ecs(config,parameter)
        print('last_sync_time=',config['last_sync_time'])
        for log in get_log_ecs(parameter).split('$VAR1 =')[1:]:
            parse_ecs_log(log,config)
            i_counter = 1
        if i_counter == 1:
           write_sync_time_ecs(config,parameter)

    if config['db_type'] == '0' and config['ds_id'] != '':
        i_counter = 0
        config['last_sync_time'] = read_sync_time(config)
        print('last_sync_time=', config['last_sync_time'])
        for log in get_log_rds(config).split('# Time:')[1:]:
            parse_log_rds(log, config)
            i_counter = 1
        if i_counter == 1:
            write_sync_time(config)


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

