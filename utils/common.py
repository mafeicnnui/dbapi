#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 9:38
# @Author : ma.fei
# @File : common.py.py
# @Software: PyCharm

import os
import json
import datetime
import pymysql
import tornado
import traceback
import paramiko
from utils.mysql_async import async_processer

class health(tornado.web.RequestHandler):
    def head(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write('health check success!')

class read_db_decrypt(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            pwd = await aes_decrypt(self.get_argument("password"),self.get_argument("key"))
            if pwd is not None:
                res = {'code':200,'msg':pwd}
            else:
                res = {'code': -1, 'msg': 'password or key is invalid!'}
            self.write(json.dumps(res))
        except Exception as e:
            res = {'code': -1, 'msg': str(e)}
            self.write(json.dumps(res))


def format_sql(v_sql):
    if v_sql is not None:
       return v_sql.replace("\\","\\\\").replace("'","\\'")
    else:
       return v_sql

def get_mysql_columns(p_sync):
    v = '''"{0}",'''.format(p_sync['sync_hbase_rowkey_sour'])
    for i in p_sync['sync_columns'].split(','):
        v = v + '''"{}",'''.format(i)
    print('get_mysql_columns=', v)
    return v[0:-1]

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_time2():
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    print('-'.ljust(85,'-'))

def get_ds_mysql(ip,port,service ,user,password,charset):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset=charset,cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn

def get_ds_mysql2(ip,port,service ,user,password,charset):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,charset=charset,autocommit=True)
    return conn

def get_db_mysql(config):
    return get_ds_mysql(config['db_ip'],config['db_port'],config['db_service'],config['db_user'],config['db_pass'],config['db_charset'])

def get_db_mysql2(config):
    return get_ds_mysql2(config['db_ip'],config['db_port'],config['db_service'],config['db_user'],config['db_pass'],config['db_charset'])

async def aes_decrypt(p_password,p_key):
    st = "select aes_decrypt(unhex('{0}'),'{1}') as password".format(p_password,p_key[::-1])
    rs = await async_processer.query_dict_one(st)
    if rs['password'] is not None:
       return str(rs['password'],encoding = "utf-8")
    else:
       return None

def get_file_contents(filename):
    file_handle = open(filename, 'r')
    line = file_handle.readline()
    lines = ''
    while line:
        lines = lines + line
        line = file_handle.readline()
    lines = lines + line
    file_handle.close()
    return lines

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def db_config():
    config = read_json('./config/config.json')
    config['db_mysql']   =  get_db_mysql(config)
    return config

def db_config2():
    config = read_json('./config/config.json')
    config['db_mysql']   =  get_db_mysql2(config)
    return config

def db_config_info():
    config = read_json('./config/config.json')
    return config

async def check_tab_exists(p_tab,p_where):
    st = "select count(0) from {0} {1}".format(p_tab,p_where)
    return (await async_processer.query_one(st))[0]

def exec_ssh_cmd(p_cfg,p_cmd):
    stdout_lines = []
    stderr_lines = []
    cmd_exec_status = True
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=p_cfg['msg']['server_ip'],port=int(p_cfg['msg']['server_port']),username=p_cfg['msg']['server_user'], password=p_cfg['msg']['server_pass'])
        stdin, stdout,stderr = ssh.exec_command(p_cmd,timeout=6)
        stdout_lines = stdout.readlines()
        stderr_lines = stderr.readlines()
        if stdout.channel.recv_exit_status() != 0:
            raise paramiko.SSHException()
        ssh.close()
    except paramiko.SSHException as e:
        print("Failed to execute the command on '{}': {}".format(p_cfg['msg']['server_ip'], str(e)))
        if len(stderr_lines) > 0:
           print("Error reported by {}: {}" .format(p_cfg['msg']['server_ip'], "\n".join(stderr_lines)))
        cmd_exec_status = False
    return {'status':cmd_exec_status,'stdout':stdout_lines}

def gen_transfer_file(p_cfg,p_flag,p_templete):
    f_path     =  os.getcwd()
    f_templete = '{}/templete/{}/{}'.format(f_path,p_flag,p_templete)
    f_local    = '{}/script/{}'.format(f_path,p_templete)
    f_remote   = '{0}/{1}'.format(p_cfg['msg']['script_path'],p_templete)
    os.system('cp -f {0} {1}'.format(f_templete, f_local))
    with open(f_local, 'w') as f:
        f.write(get_file_contents(f_templete).
                    replace('$$API_SERVER$$',   p_cfg['msg']['api_server']).
                    replace('$$PYTHON3_HOME$$', p_cfg['msg']['python3_home']).
                    replace('$$SCRIPT_PATH$$',  p_cfg['msg']['script_path']).
                    replace('$$SCRIPT_FILE$$',  p_cfg['msg']['script_file']).
                    replace('$$PORT$$',         p_cfg['msg'].get('proxy_local_port') if p_cfg['msg'].get('proxy_local_port') is not None else '').
                    replace('$$DB_TAG$$',       p_cfg['msg'].get('db_tag')  if p_cfg['msg'].get('db_tag') is not None else '').
                    replace('$$INST_ID$$',      p_cfg['msg'].get('inst_id') if p_cfg['msg'].get('inst_id') is not None else ''))
    return f_local,f_remote

def ftp_transfer_file(p_cfg,p_local,p_remote):
    try:
        transport = paramiko.Transport((p_cfg['msg']['server_ip'], int(p_cfg['msg']['server_port'])))
        transport.connect(username=p_cfg['msg']['server_user'], password= p_cfg['msg']['server_pass'])
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(localpath=p_local, remotepath=p_remote)
        transport.close()
        print('Script:{0} send to {1} ok.'.format(p_local, p_remote))
        return True
    except:
        traceback.print_exc()
        return False

def check_task(p_cfg,p_cmd):
    pass

def stop_task():
    pass

def push_task():
    pass


