#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : ma.fei
# @File    : mssql2mysql_sync.py
# @Software: PyCharm
# @func    ：mssql->mysql syncer

import sys
import traceback
import configparser
import warnings
import pymssql
import pymysql
import datetime
import smtplib
from email.mime.text import MIMEText
import json
import os
import requests
import socket

def socket_port(ip, port):
    try:
        socket.setdefaulttimeout(1)
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result=s.connect_ex((ip, port))
        if result==0:
          return True
        else:
          return False
    except:
        return False

def get_available_port():
    for p in [465,25]:
        if socket_port("smtp.exmail.qq.com",p):
            return p

def send_mail25(p_sendserver,p_from_user, p_from_pass, p_to_user, p_title, p_content):
    to_user = p_to_user.split(",")
    try:
        msg = MIMEText(p_content, 'html', 'utf-8')
        msg["Subject"] = p_title
        msg["From"] = p_from_user
        msg["To"] = ",".join(to_user)
        # server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server = smtplib.SMTP(p_sendserver, 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail465(p_sendserver,p_from_user, p_from_pass, p_to_user, p_title, p_content):
    to_user = p_to_user.split(",")
    try:
        msg = MIMEText(p_content, 'html', 'utf-8')
        msg["Subject"] = p_title
        msg["From"] = p_from_user
        msg["To"] = ",".join(to_user)
        server = smtplib.SMTP_SSL(p_sendserver, 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        print('send_mail465 send success!')
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail_param(p_sendserver,p_from_user, p_from_pass, p_to_user, p_title, p_content):
    try:
        port = get_available_port()
        if port == 465:
           print('send_mail465')
           send_mail465(p_sendserver,p_from_user, p_from_pass, p_to_user, p_title, p_content)
        else:
           print('send_mail25')
           send_mail25(p_sendserver,p_from_user, p_from_pass, p_to_user, p_title, p_content)
        print('send_mail_param send success!')
    except :
        print("send_mail_param exception:")
        traceback.print_exc()

def exception_info():
    e_str = traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_seconds(b):
    a = datetime.datetime.now()
    return int((a - b).total_seconds())

def print_dict(config):
    print('-'.ljust(85, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    print('-'.ljust(85, '-'))
    for key in config:
        print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', config[key])
    print('-'.ljust(85, '-'))

def format_sql(v_sql):
    return v_sql.replace("\\", "\\\\").replace("'", "\\'")

def get_ds_mysql(ip, port, service, user, password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=False)
    return conn

def get_ds_sqlserver(ip, port, service, user, password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service, charset='utf8')
    return conn

def get_db_sqlserver(config):
    return get_ds_sqlserver(config['db_sqlserver_ip'], config['db_sqlserver_port'], \
                            config['db_sqlserver_service'], config['db_sqlserver_user'], config['db_sqlserver_pass'])

def get_db_mysql(config):
    return get_ds_mysql(config['db_mysql_ip'], config['db_mysql_port'], config['db_mysql_service'], \
                        config['db_mysql_user'], config['db_mysql_pass'])

def get_sync_time_type_name(sync_time_type):
    if sync_time_type == "day":
        return '天'
    elif sync_time_type == "hour":
        return '小时'
    elif sync_time_type == "min":
        return '分'
    else:
        return ''

def check_api_server_status(cfg):
    print('check_api_server_status=',cfg['api_server'])
    api_status  = {}
    for api in cfg['api_server'].split(','):
        req = 'http://{}/health'.format(api)
        try:
          res = requests.head(req,timeout=1)
          api_status[api] = res.status_code
        except:
          api_status[api] = 500
    cfg['api_status'] = api_status
    print(cfg['api_status'])
    return  cfg

def query_health_api(cfg):
    apis = cfg['api_status']
    for key in apis:
        if apis[key] == 200:
           return  key

def get_config_from_db(tag, workdir):
    file_name = workdir + '/config/' + tag + '.json'
    par = {'tag': tag}
    if os.path.exists(file_name):
        cfg = get_local_config_json(file_name)
        check_api_server_status(cfg)
        write_local_config_file_json(cfg)
        print('api server status update!')
    else:
        cfg = {}
        cfg['api_status'] = {'$$API_SERVER$$': 200}
        print('File:{} not exist ,skip api server check!'.format(file_name))

    try:
        try:
            url = 'http://$$API_SERVER$$/read_config_sync'
            res = requests.post(url, data = par,timeout=1).json()
        except:
            api = query_health_api(cfg)
            url = 'http://{}/read_config_sync'.format(api)
            res = requests.post(url, data = par,timeout=1).json()

        if res['code'] == 200:
            print('read_config_sync 接口调用成功!')
            config = res['msg']
            try:
                tmp = get_local_config_json(file_name)
                if tmp.get('counter') is None or tmp.get('counter') >0 :
                    recover_running(tmp)
            except:
                pass
            try:
                config['sync_time_type_name']  = get_sync_time_type_name(config['sync_time_type'])
                db_sour_ip                     = config['sync_db_sour'].split(':')[0]
                db_sour_port                   = config['sync_db_sour'].split(':')[1]
                db_sour_service                = config['sync_db_sour'].split(':')[2]
                db_sour_user                   = config['sync_db_sour'].split(':')[3]
                db_sour_pass                   = aes_decrypt(cfg,config['sync_db_sour'].split(':')[4], db_sour_user)
                db_dest_ip                     = config['sync_db_dest'].split(':')[0]
                db_dest_port                   = config['sync_db_dest'].split(':')[1]
                db_dest_service                = config['sync_db_dest'].split(':')[2]
                db_dest_user                   = config['sync_db_dest'].split(':')[3]
                db_dest_pass                   = aes_decrypt(cfg,config['sync_db_dest'].split(':')[4], db_dest_user)
                config['db_sqlserver_ip']      = db_sour_ip
                config['db_sqlserver_port']    = db_sour_port
                config['db_sqlserver_service'] = db_sour_service
                config['db_sqlserver_user']    = db_sour_user
                config['db_sqlserver_pass']    = db_sour_pass
                config['db_mysql_ip']          = db_dest_ip
                config['db_mysql_port']        = db_dest_port
                config['db_mysql_service']     = db_dest_service
                config['db_mysql_user']        = db_dest_user
                config['db_mysql_pass']        = db_dest_pass
                config['db_sqlserver_string']  = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
                config['db_mysql_string']      = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
                config['run_mode']             = 'remote'
                config['counter']              = 0
                config['api_status']           = cfg['api_status']

                write_local_config_file_json(config)

                config['db_sqlserver']  = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                           db_sour_pass)
                config['db_sqlserver2'] = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                           db_sour_pass)
                config['db_sqlserver3'] = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                           db_sour_pass)
                config['db_mysql']      = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
                config['db_mysql3']     = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user,db_dest_pass)
                return config
            except Exception as e:
                v_error = '从接口配置数据库连接对象异常:{0}'.format(traceback.format_exc())
                print(v_error)
                exception_connect_db(config, v_error)
                return None
        else:
            file_name = workdir + '/config/' + tag + '.json'
            config = get_config_json(file_name)
            if config['counter'] <= 3:
                print('DBAPI接口调用失败!,{0}'.format(res['msg']))
                v_title = '数据同步接口异常[★]'
                v_content = '''<table class='xwtable'>
                                   <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                                   <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                                   <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr>            
                               </table>'''
                v_content = v_content.replace('$$interface$$', url)
                v_content = v_content.replace('$$parameter$$', json.dumps(par))
                v_content = v_content.replace('$$error$$', res['msg'])
                if res['code'] != -3:
                    exception_interface(cfg,v_title, v_content)
                else:
                    print(res['msg'])
                return None

    except :
        file_name = workdir + '/config/' + tag + '.json'
        config    = get_config_json(file_name)
        if config['counter']<=3:
           print('接口调用失败,从本地读取最近一次配置文件：{0}'.format(file_name))
           v_title   = '{}任务切换为本地模式[★★]'.format(config['comments'])
           v_desc    = '同步任务运行于本地配置文件模式中，请尽快处理! 自动从本地读取最近一次配置文件进行同步：{0}'.format(file_name)
           v_content = '''<table class='xwtable'>
                             <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                             <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                             <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr> 
                             <tr><td  width="30%">说明信息</td><td  width="70%">$$desc$$</td></tr>               
                          </table>'''
           v_content = v_content.replace('$$interface$$', url)
           v_content = v_content.replace('$$parameter$$', json.dumps(par))
           v_content = v_content.replace('$$error$$', traceback.format_exc())
           v_content = v_content.replace('$$desc$$', v_desc)
           exception_interface(cfg,v_title, v_content)
        return config

def aes_decrypt(p_cfg,p_password, p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
       url = 'http://$$API_SERVER$$/read_db_decrypt'
       res = requests.post(url, data = par,timeout=1).json()
       if res['code'] == 200:
           print('call interface aes_decrypt success!')
           return res['msg']
       else:
           print('call interface aes_decrypt error:{}'.format(res['msg']))
    except:
       try:
           url = 'http://{}/read_db_decrypt'.format(query_health_api(p_cfg))
           res = requests.post(url, data = par,timeout=1).json()
           if res['code'] == 200:
               print('call interface aes_decrypt success!')
               return res['msg']
           else:
               print('call interface aes_decrypt error:{}'.format(res['msg']))
       except:
           print('aes_decrypt api not available!')


def write_sync_log(config):
    par = {
        'sync_tag': config['sync_tag'],
        'create_date': get_time(),
        'duration': config['sync_duration'],
        'amount': config['sync_amount']
    }
    try:
        url = 'http://$$API_SERVER$$/write_sync_log'
        res = requests.post(url, data={'tag': json.dumps(par)},timeout=1)
        if res.status_code == 200:
            print('Interface write_sync_log call successful!')
        else:
            print('Interface write_sync_log call failed!')
    except:
        try:
            api = query_health_api(config)
            url = 'http://{}/write_sync_log'.format(api)
            res = requests.post(url, data={'tag': json.dumps(par)},timeout=1)
            if res.status_code == 200:
                print('Interface write_sync_log call successful!')
            else:
                print('Interface write_sync_log call failed!')
        except:
            print('Interface API not available,skip write_sync_log!')


def write_sync_log_detail(config,ftab):
    par = {
            'sync_tag'     : config['sync_tag'],
            'create_date'  : get_time(),
            'sync_table'   : config['sync_table_inteface'],
            'sync_amount'  : config['sync_amount'],
            'duration'     : config['sync_duration'],
            'db_name'      : config['db_sqlserver_service'].lower(),
            'schema_name'  : ftab.split(':')[0].split('.')[0].lower(),
            'tab_name'     : ftab.split(':')[0].split('.')[1].lower(),
            'sync_cols'    : gather_sync_table_cols(config,ftab.split(':')[0]).replace('`','').replace(',','#').lower(),
            'sync_incr_col': ftab.split(':')[1].lower(),
            'sync_time'    : ftab.split(':')[2]
    }

    try:
        url = 'http://$$API_SERVER$$/write_sync_log_detail'
        res = requests.post(url, data={'tag': json.dumps(par)},timeout=1)
        if res.status_code == 200:
            print('Interface write_sync_log_detail call successful!')
        else:
            print('Interface write_sync_log_detail call failed!')
    except:
        try:
            api = query_health_api(config)
            url = 'http://{}/write_sync_log_detail'.format(api)
            res = requests.post(url, data={'tag': json.dumps(par)},timeout=1)
            if res.status_code == 200:
                print('Interface write_sync_log_detail call successful!')
            else:
                print('Interface write_sync_log_detail call failed!')
        except:
            print('Interface API not available,skip write_sync_log_detail!')


def update_sync_status(config,status):
    data = {
        'tag'    : config['sync_tag'],
        'status' : status
    }
    try:
        url = 'http://$$API_SERVER$$/update_sync_status'
        res = requests.post(url, data=data,timeout=1)
        if res.status_code == 200:
            print('call interface update_sync_status :{}!'.format('running' if status == '1' else 'complete'))
        else:
            print('call interface update_sync_status error :{}'.format(res['msg']))
            sys.exit(0)
    except:
        try:
            api = query_health_api(config)
            url = 'http://{}/update_sync_status'.format(api)
            res = requests.post(url, data=data,timeout=1)
            if res.status_code == 200:
                print('call interface update_sync_status :{}!'.format('running' if status == '1' else 'complete'))
            else:
                print('call interface update_sync_status error :{}'.format(res['msg']))
        except:
            print('Interface API not available,skip update_sync_status!')


def get_local_config_json(fname):
    with open(fname, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_config_json(fname):
    with open(fname, 'r') as f:
         cfg = json.loads(f.read())

    cfg['run_mode']     = 'local'
    cfg['counter']      = cfg['counter'] + 1
    write_local_config_file_json(cfg)


    cfg['db_sqlserver'] = get_ds_sqlserver(cfg['db_sqlserver_ip'],
                                           cfg['db_sqlserver_port'],
                                           cfg['db_sqlserver_service'],
                                           cfg['db_sqlserver_user'],
                                           cfg['db_sqlserver_pass'])

    cfg['db_sqlserver2'] = get_ds_sqlserver(cfg['db_sqlserver_ip'],
                                            cfg['db_sqlserver_port'],
                                            cfg['db_sqlserver_service'],
                                            cfg['db_sqlserver_user'],
                                            cfg['db_sqlserver_pass'])

    cfg['db_sqlserver3'] = get_ds_sqlserver(cfg['db_sqlserver_ip'],
                                            cfg['db_sqlserver_port'],
                                            cfg['db_sqlserver_service'],
                                            cfg['db_sqlserver_user'],
                                            cfg['db_sqlserver_pass'])

    cfg['db_mysql']     = get_ds_mysql(cfg['db_mysql_ip'],
                                       cfg['db_mysql_port'],
                                       cfg['db_mysql_service'],
                                       cfg['db_mysql_user'],
                                       cfg['db_mysql_pass'])

    cfg['db_mysql3']   = get_ds_mysql(cfg['db_mysql_ip'],
                                       cfg['db_mysql_port'],
                                       cfg['db_mysql_service'],
                                       cfg['db_mysql_user'],
                                       cfg['db_mysql_pass'])
    return cfg

def get_config(fname, tag):
    try:
        config = {}
        cfg = configparser.ConfigParser()
        cfg.read(fname, encoding="utf-8-sig")
        sync_server_sour = cfg.get("sync", "sync_db_server")
        sync_server_dest = cfg.get("sync", "sync_db_mysql")
        db_sour_ip      = sync_server_sour.split(':')[0]
        db_sour_port    = sync_server_sour.split(':')[1]
        db_sour_service = sync_server_sour.split(':')[2]
        db_sour_user    = sync_server_sour.split(':')[3]
        db_sour_pass    = sync_server_sour.split(':')[4]
        db_dest_ip      = sync_server_dest.split(':')[0]
        db_dest_port    = sync_server_dest.split(':')[1]
        db_dest_service = sync_server_dest.split(':')[2]
        db_dest_user    = sync_server_dest.split(':')[3]
        db_dest_pass    = sync_server_dest.split(':')[4]
        config['db_sqlserver_ip'] = db_sour_ip
        config['db_sqlserver_port'] = db_sour_port
        config['db_sqlserver_service'] = db_sour_service
        config['db_sqlserver_user'] = db_sour_user
        config['db_sqlserver_pass'] = db_sour_pass
        config['db_mysql_ip']       = db_dest_ip
        config['db_mysql_port']     = db_dest_port
        config['db_mysql_service']  = db_dest_service
        config['db_mysql_user']     = db_dest_user
        config['db_mysql_pass']     = db_dest_pass
        config['db_sqlserver_string'] = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
        config['db_mysql_string'] = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
        config['db_sqlserver']    = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
        config['db_sqlserver2']   = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                   db_sour_pass)
        config['db_mysql']        = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
        config['db_sqlserver3']   = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                   db_sour_pass)
        config['db_mysql3']       = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
        config['run_mode']        = 'local'
        config['sync_tag']        = tag
        config['sync_table']      = cfg.get("sync", "sync_table").lower()
        config['sync_col_name']   = cfg.get("sync", "sync_col_name").lower()
        config['sync_col_val']    = cfg.get("sync", "sync_col_val")
        config['batch_size']      = cfg.get("sync", "batch_size")
        config['batch_size_incr'] = cfg.get("sync", "batch_size_incr")
        config['sync_gap']        = cfg.get("sync", "sync_gap")
        config['sync_time_type']  = cfg.get("sync", "sync_time_type")
        config['sync_time_type_name'] = get_sync_time_type_name(config['sync_time_type'])
        config['sync_ywlx']        = cfg.get("sync", 'sync_ywlx')
        config['sync_ywlx_name']   = cfg.get("sync", 'sync_ywlx_name')
        config['sync_type']        = cfg.get("sync", 'sync_type')
        config['sync_type_name']   = cfg.get("sync", 'sync_type_name')
        config['server_id']        = cfg.get("sync", 'server_id')
        config['server_desc']      = cfg.get("sync", 'server_desc')
        config['run_time']         = cfg.get("sync", 'run_time')
        config['api_server']       = cfg.get("sync", 'api_server')
        config['script_path']      = cfg.get("sync", 'script_path')
        config['script_file']      = cfg.get("sync", 'script_file')
        config['comments']         = cfg.get("sync", 'comments')
        config['python3_home']     = cfg.get("sync", 'python3_home')
        config['status']           = cfg.get("sync", 'status')
        config['proxy_server']     = cfg.get("sync", 'proxy_server')
        config['proxy_local_port'] = cfg.get("sync", 'proxy_local_port')
        config['cols']             = json.loads(cfg.get("sync", 'cols'))
        config['proxy_server']     = cfg.get("sync", 'proxy_server')
        config['proxy_local_port'] = cfg.get("sync", 'proxy_local_port')
        return config
    except Exception as e:
        print(traceback.print_exc())
        v_error = '从本地读取最近一次本地配置文件获取数据源时出现异常:{0}'.format(str(e))
        print(v_error)
        exception_connect_db(config, v_error)
        exit(0)

def check_mysql_tab_exists(config, tab):
    db = config['db_mysql']
    cr = db.cursor()
    sql = """select count(0) from information_schema.tables
            where table_schema='{0}' and table_name='{1}'""".format(config['db_mysql_service'], tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]

def check_mysql_col_exists(config, tab, col):
    db = config['db_mysql']
    cr = db.cursor()
    sql = """select count(0) from information_schema.columns
            where table_schema='{0}' and table_name='{1}' and column_name='{2}'""".format(config['db_mysql_service'],
                                                                                          tab, col)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]

def get_mysql_tab_rows(config, tab):
    db = config['db_mysql']
    cr = db.cursor()
    sql = """select count(0) from {0}""".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]

def check_sqlserver_tab_exists_data(config, tname):
    db = config['db_sqlserver']
    cr = db.cursor()
    sql = "select count(0) from {0}".format(tname)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]

def check_mysql_tab_sync(config, tab):
    db = config['db_mysql']
    cr = db.cursor()
    sql = "select count(0) from {0}".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]

def check_sqlserver_tab_exists_pk(config, tab):
    db = config['db_sqlserver']
    cr = db.cursor()
    sql = """select
             count(0)
            from syscolumns col, sysobjects obj
            where col.id=obj.id and obj.id=object_id('{0}')
            and  (select  1
                  from  dbo.sysindexes si
                      inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
                      inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
                      inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
                  where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]

def convert(v_sql):
    return v_sql.lower().replace("nvarchar", "varchar"). \
        replace("varchar(-1)", "longtext"). \
        replace("datetime(23)", "datetime"). \
        replace("datetime2(27)", "datetime"). \
        replace("time(16)", "time"). \
        replace("date(10)", "date"). \
        replace("numeric", "decimal"). \
        replace("nvarchar", "varchar"). \
        replace("money", "DECIMAL"). \
        replace("identity(1,1)", ""). \
        replace("smalldatetime(16)", "datetime"). \
        replace("smalldatetime", "datetime"). \
        replace("float", "decimal"). \
        replace("bit", "varchar"). \
        replace("timestamp(8)", "varchar(50)")

def get_tab_columns(config, tab):
    cr = config['db_sqlserver3'].cursor()
    sql = """select col.name
           from syscolumns col, sysobjects obj
           where col.id=obj.id 
             and obj.id=object_id('{0}')
           order by isnull((SELECT  'Y'
                            FROM  dbo.sysindexes si
                            INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                            inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                            where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
        """.format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchall()
    s1 = ""
    v_sync_cols = get_sync_cols(config, tab)
    for i in range(len(rs)):
        if v_sync_cols is None:
            s1 = s1 + '[' + rs[i][0].lower() + '],'
        else:
            if rs[i][0].lower() in v_sync_cols:
               s1 = s1 + '[' + rs[i][0].lower() + '],'
    cr.close()
    return s1[0:-1]

def get_tab_columns_incr(config, tab):
    cr = config['db_sqlserver3'].cursor()
    sql = """select col.name
           from syscolumns col, sysobjects obj
           where col.id=obj.id 
             and obj.id=object_id('{0}')
           order by isnull((SELECT  'Y'
                            FROM  dbo.sysindexes si
                            INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                            inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                            where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
        """.format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchall()
    s1 = ""
    for i in range(len(rs)):
        print('get_sync_table_pk_names(config,tab)=', get_sync_table_pk_names(config, tab), rs[i][0].lower())
        if get_sync_table_pk_names(config, tab) != rs[i][0].lower():
            s1 = s1 + '[' + rs[i][0].lower() + '],'
    cr.close()
    return s1[0:-1]

def get_tab_header(config, tab):
    cr = config['db_sqlserver'].cursor()
    sql = "select top 1 * from {0}".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    desc = cr.description
    s1 = "insert into " + get_mapping_tname(tab.lower()) + "("
    s2 = " values "
    '''for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','
    '''
    s1 = s1 + get_sync_table_cols(config, tab) + ',' + config['sync_col_name'] + ")"
    cr.close()
    return s1 + s2

def check_sync_sqlserver_col_comments(config, tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment = """SELECT  count(0)
                    FROM sys.tables A
                    INNER JOIN syscolumns B ON B.id = A.object_id
                    left join  systypes t   on b.xtype=t.xusertype
                    LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                    WHERE A.name = '{0}'  and c.value is not null        
                   """.format(tab)
    config['run_sql'] = v_comment
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_col_comments(config, tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc = config['db_mysql']
    cr_desc = db_desc.cursor()
    v_comment = """SELECT                                
                        case when t.name ='numeric' then
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+','+
                            CAST(isnull(COLUMNPROPERTY(b.id,b.name,'Scale'),0) as varchar)	   
                           +') comment '''+CAST(c.value as varchar)+''''
                        when t.name in('nvarchar','varchar','int') then
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+') comment '''+CAST(c.value as varchar)+''''
                        else
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+' comment '''+CAST(c.value as varchar)+''''
                        end
                FROM sys.tables A
                INNER JOIN syscolumns B ON B.id = A.object_id
                left join  systypes t   on b.xtype=t.xusertype
                LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                WHERE A.name = '{0}'  and c.value is not null        
               """.format(tab)
    config['run_sql'] = v_comment
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchall()
    for j in range(len(rs_source)):
        v_ddl_sql = rs_source[j][0]
        config['run_sql'] = convert(v_ddl_sql)
        cr_desc.execute(convert(v_ddl_sql))
    db_desc.commit()
    cr_desc.close()

def check_sync_sqlserver_tab_comments(config, tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment = """select count(0)  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')  and a.name='{0}'""".format(tab, tab)
    config['run_sql'] = v_comment
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_tab_comments(config, tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc = config['db_mysql']
    cr_desc = db_desc.cursor()
    v_comment = """select 
                   'alter table '+lower(a.name)+' comment '''+cast(a.value as varchar)+''''
                  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')
                    and a.name='{0}'""".format(tab, tab)
    config['run_sql'] = v_comment
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    if len(rs_source) > 0:
        v_ddl_sql = rs_source[0]
        config['run_sql'] = v_ddl_sql
        cr_desc.execute(v_ddl_sql)
        cr_desc.close()

def get_mapping_tname(tab):
    return tab.replace('.', '_')

def f_get_table_ddl(config, tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql = """SELECT       									
                    a.colorder 字段序号,
                    '`'+a.name+'`' 字段名,
                    b.name 类型,
                    COLUMNPROPERTY(a.id,a.name,'PRECISION') as 长度,
                    isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0) as 小数位,
                    (case when (SELECT count(*)
                        FROM sysobjects
                        WHERE (name in
                                (SELECT name
                                FROM sysindexes
                                WHERE (id = a.id) AND (indid in
                                         (SELECT indid
                                        FROM sysindexkeys
                                        WHERE (id = a.id) AND (colid in
                                                  (SELECT colid
                                                   FROM syscolumns
                                                  WHERE (id = a.id) AND (name = a.name))))))) AND
                            (xtype = 'PK'))>0 then '√' else '' end) 主键
                    FROM  syscolumns  a 
                    left join systypes b    on a.xtype=b.xusertype
                    inner join sysobjects d on a.id=d.id  and  d.xtype='U' and  d.name<>'dtproperties'
                    left join syscomments e on a.cdefault=e.id
                    left join sys.extended_properties g on a.id=g.major_id AND a.colid = g.major_id
                    where d.id=object_id('{0}') 
                    order by 6 desc,a.id,a.colorder""".format(tab)
    config['run_sql'] = v_sql
    cr_source.execute(v_sql)
    rs = cr_source.fetchall()
    v_cre_tab = 'create table ' + tab + '(';
    for i in range(len(rs)):
        v_name = rs[i][1]
        v_type = rs[i][2]
        v_len = str(rs[i][3])
        v_scale = str(rs[i][4])
        if v_type in ('int', 'date'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    ' + v_type + ','
        elif v_type in ('numeric', 'decimal'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    ' + v_type + '(' + v_len + ',' + v_scale + ') ,'
        else:
            v_cre_tab = v_cre_tab + '   ' + v_name + '    ' + v_type + '(' + v_len + ') ,'
    return v_cre_tab[0:-1] + ')'

def sync_sqlserver_ddl(config, debug):
    try:
        db_source = config['db_sqlserver']
        cr_source = db_source.cursor()
        db_desc = config['db_mysql']
        cr_desc = db_desc.cursor()
        for i in config['sync_table'].split(","):
            tab = i.split(':')[0]
            cr_source.execute("""select id,
                                        OBJECT_SCHEMA_NAME(id) as schema_name, 
                                        OBJECT_NAME(id) as table_name,
                                        DB_NAME() as db_name,
                                        OBJECT_SCHEMA_NAME(id)+'.'+OBJECT_NAME(id) as full_table_name
                                 from sysobjects 
                                 where xtype='U' and id=object_id('{0}') order by name""".format(tab))
            rs_source = cr_source.fetchall()
            for j in range(len(rs_source)):
                tab_name = rs_source[j][2].lower()
                tab_prefix = (str(rs_source[j][1]) + '.').lower()
                full_tab_name = rs_source[j][4].lower()
                if check_sqlserver_tab_exists_pk(config, tab) == 0:
                    print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_sqlserver_string'],
                                                                              full_tab_name))
                    v_ddl_sql = f_get_table_ddl(config, full_tab_name)
                    # print(v_ddl_sql)
                    v_cre_sql = v_ddl_sql.replace(full_tab_name, get_mapping_tname(full_tab_name))
                    if check_mysql_tab_exists(config, get_mapping_tname(full_tab_name)) > 0:
                        print("DB:{0},Table :{1} already exists!".format(config['db_mysql_string'],
                                                                         get_mapping_tname(full_tab_name)))
                    else:
                        config['run_sql'] = convert(v_cre_sql)
                        cr_desc.execute(convert(v_cre_sql))
                        print("Table:{0} creating success!".format(get_mapping_tname(full_tab_name)))

                        # add new columns. by 2020.4.13(market_id,es_id)
                        for ac in config['sync_col_name'].split(','):
                            config['run_sql'] = 'alter table {0} add {1} varchar(50)'.format(
                                get_mapping_tname(full_tab_name), ac)
                            cr_desc.execute(
                                'alter table {0} add {1} varchar(50)'.format(get_mapping_tname(full_tab_name), ac))
                            print("Table:{0} add column {1} success!".format(get_mapping_tname(full_tab_name), ac))

                        v_pk_sql = """ALTER TABLE {0} ADD COLUMN pkid INT(11) NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (pkid)
                                """.format(get_mapping_tname(full_tab_name))
                        print("Table:{0} add primary key pkid success!".format(get_mapping_tname(full_tab_name)))
                        config['run_sql'] = v_pk_sql
                        cr_desc.execute(v_pk_sql)
                        db_desc.commit()
                        # create mysql table comments
                        if check_sync_sqlserver_tab_comments(config, tab) > 0:
                            sync_sqlserver_tab_comments(config, tab)
                            print("Table:{0}  comments create complete!".format(tab))
                        # create mysql table column comments
                        if check_sync_sqlserver_col_comments(config, tab) > 0:
                            sync_sqlserver_col_comments(config, tab)
                            print("Table:{0} columns comments create complete!".format(tab))

                else:
                    # 编写函数完成生成创表语句
                    v_ddl_sql = f_get_table_ddl(config, full_tab_name)
                    v_cre_sql = v_ddl_sql.replace(full_tab_name, get_mapping_tname(full_tab_name))

                    if check_mysql_tab_exists(config, get_mapping_tname(full_tab_name)) > 0:
                        print("DB:{0},Table :{1} already exists!".format(config['db_mysql_string'],
                                                                         get_mapping_tname(full_tab_name)))
                    else:
                        config['run_sql'] = convert(v_cre_sql)
                        cr_desc.execute(convert(v_cre_sql))
                        print("Table:{0} creating success!".format(get_mapping_tname(full_tab_name)))
                        config['run_sql'] = 'alter table {0} add primary key ({1})'.format(
                            get_mapping_tname(full_tab_name), get_sync_table_pk_names(config, full_tab_name))
                        cr_desc.execute('alter table {0} add primary key ({1})'.format(get_mapping_tname(full_tab_name),
                                                                                       get_sync_table_pk_names(config,
                                                                                                               full_tab_name)))
                        print("Table:{0} add primary key {1} success!".format(get_mapping_tname(full_tab_name),
                                                                              get_sync_table_pk_names(config,
                                                                                                      full_tab_name)))

                        # add new columns. by 2020.4.13(market_id,es_id)
                        for ac in config['sync_col_name'].split(','):
                            config['run_sql'] = 'alter table {0} add {1} varchar(50)'.format(get_mapping_tname(full_tab_name),
                                                                                     ac)
                            cr_desc.execute('alter table {0} add {1} varchar(50)'.format(get_mapping_tname(full_tab_name), ac))
                            print("Table:{0} add column {1} success!".format(get_mapping_tname(full_tab_name), ac))
                        db_desc.commit()
                        # create mysql table comments
                        if check_sync_sqlserver_tab_comments(config, tab) > 0:
                            sync_sqlserver_tab_comments(config, tab)
                            print("Table:{0}  comments create complete!".format(tab))
                        # create mysql table column comments
                        if check_sync_sqlserver_col_comments(config, tab) > 0:
                            sync_sqlserver_col_comments(config, tab)
                            print("Table:{0} columns comments create complete!".format(tab))

        cr_source.close()
        cr_desc.close()
    except Exception as e:
        print('sync_sqlserver_ddl exception:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def get_sync_table_total_rows(config, tab, v_where):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql = "select count(0) from {0} with(nolock) {1}".format(tab, v_where)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchone()
    cr_source.close()
    return rs_source[0]

def get_sync_table_total_rows_mysql(config, tab, v_where):
    db_desc = config['db_mysql']
    cr_desc = db_desc.cursor()
    v_sql = "select count(0) from {0} {1}".format(tab, v_where)
    cr_desc.execute(v_sql)
    rs_desc = cr_desc.fetchone()
    cr_desc.close()
    return rs_desc[0]

def get_sync_table_pk_names(config, tab):
    cr_source = get_db_sqlserver(config).cursor()
    v_col = ''
    v_sql = """select col.name
              from syscolumns col, sysobjects obj
              where col.id=obj.id and  obj.id=object_id('{0}')
               and (SELECT  1
                    FROM dbo.sysindexes si
                        INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                        INNER JOIN dbo.syscolumns sc ON sc.id = sik.id    AND sc.colid = sik.colid
                        INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                    WHERE  sc.id = col.id  AND sc.colid = col.colid)=1  order by col.colid
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + i[0] + ','
    cr_source.close()
    return v_col[0:-1]

def gather_sync_table_cols(config, tab):
    cr_source = get_db_sqlserver(config).cursor()
    v_col = ''
    v_sql = """select '`'+col.name+'`'
               from syscolumns col, sysobjects obj
               where col.id = obj.id 
                 and obj.id = object_id('{0}')
               order by isnull((SELECT  'Y'
                                FROM  dbo.sysindexes si
                                INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                                inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                                where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid
         """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + i[0] + ','
    cr_source.close()
    return v_col[0:-1]

def get_sync_cols(config,tab):
    for dic in config['cols']:
        if dic['sync_tag'] == config['sync_tag']  \
             and dic['db_name'] == config['db_sqlserver_service'].lower() \
               and dic['schema_name'] == tab.split('.')[0].lower() \
                 and dic['tab_name'] == tab.split('.')[1].lower() :
                    return dic['sync_cols'].split('#')
    return None

def get_sync_table_cols(config, tab):
    cr_source = get_db_sqlserver(config).cursor()
    v_col = ''
    v_sql = """select '`'+col.name+'`'
               from syscolumns col, sysobjects obj
               where col.id = obj.id 
                 and obj.id = object_id('{0}')
               order by isnull((SELECT  'Y'
                                FROM  dbo.sysindexes si
                                INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                                inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                                where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid
         """.format(tab)
    v_sync_cols = get_sync_cols(config, tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        if v_sync_cols is None:
            v_col = v_col + i[0] + ','
        else:
            if i[0].lower().replace('`','') in v_sync_cols:
               v_col = v_col + i[0] + ','
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_pk_vals(config, tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_col = ''
    v_sql = """select col.name
              from syscolumns col, sysobjects obj
              where col.id=obj.id and  obj.id=object_id('{0}')
               and (SELECT  1
                    FROM dbo.sysindexes si
                        INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                        INNER JOIN dbo.syscolumns sc ON sc.id = sik.id    AND sc.colid = sik.colid
                        INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                    WHERE  sc.id = col.id  AND sc.colid = col.colid)=1 order by col.colid
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CONVERT(varchar(100)," + i[0] + ", 20)+" + "\'^^^\'" + "+"
    cr_source.close()
    return v_col[0:-7]

def get_sync_table_pk_vals_mysql(config, tab):
    db_source = config['db_mysql']
    cr_source = db_source.cursor()
    v_col = ''
    v_sql = """SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(get_mapping_tname(tab))
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CAST(" + i[0] + " as char)," + "\'^^^\'" + ","
    cr_source.close()
    return 'CONCAT(' + v_col[0:-7] + ')'

def get_sync_table_pk_names_mysql(config, tab):
    db_source = config['db_mysql']
    cr_source = db_source.cursor()
    v_col = ''
    v_sql = """SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(get_mapping_tname(tab))
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + i[0] + ","
    cr_source.close()
    return v_col[0:-1]

def get_sync_where(pk_cols, pk_vals):
    v_where = ''
    for i in range(len(pk_cols.split(','))):
        v_where = v_where + pk_cols.split(',')[i] + "='" + pk_vals.split('^^^')[i] + "' and "
    return v_where[0:-4]

def get_sync_where_incr(tab, config):
    v_rq_col = tab.split(':')[1]
    v_expire_time = tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = 'where {0} >=DATEADD(DAY,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = 'where {0} >=DATEADD(HOUR,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = 'where {0} >=DATEADD(MINUTE,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_sync_where_day(tab,day):
    v_rq_col   = tab.split(':')[1]
    v_rq_rqq   = day+' 0:0:0'
    v_rq_rqz   = day+' 23:59:59'
    v          = "where {0} between '{1}' and '{2}'".format(v_rq_col, v_rq_rqq, v_rq_rqz)
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_sync_where_incr_mysql(tab, config):
    v_rq_col = tab.split(':')[1]
    v_expire_time = tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} DAY)".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} HOUR)".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} MINUTE)".format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_sync_where_incr_rq(tab, config, currq):
    v_rq_col = tab.split(':')[1]
    v_expire_time = tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >=DATEADD(DAY,-{1},'{2}')".format(v_rq_col, v_expire_time, currq)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >=DATEADD(HOUR,-{1},'{2}')".format(v_rq_col, v_expire_time, currq)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >=DATEADD(MINUTE,-{1},'{2}')".format(v_rq_col, v_expire_time, currq)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_sync_where_incr_mysql_rq(tab, config, currq):
    v_rq_col = tab.split(':')[1]
    v_expire_time = tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} DAY)".format(v_rq_col, currq, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} HOUR)".format(v_rq_col, currq, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} MINUTE)".format(v_rq_col, currq, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def sync_sqlserver_init_pk(config, ftab):
    try:
        tab = ftab.split(':')[0]
        status = False
        if (check_mysql_tab_exists(config, get_mapping_tname(tab)) == 0 \
                or (check_mysql_tab_exists(config, get_mapping_tname(tab)) > 0
                     and check_mysql_tab_sync(config,get_mapping_tname(tab)) == 0)):
            status= True
            i_counter = 0
            start_time = datetime.datetime.now()
            n_tab_total_rows = get_sync_table_total_rows(config, tab, '')
            ins_sql_header = get_tab_header(config, tab)
            v_pk_names = get_sync_table_pk_names(config, tab)
            v_pk_cols = get_sync_table_pk_vals(config, tab)
            v_pk_cols_mysql = get_sync_table_pk_vals_mysql(config, tab)
            n_batch_size = int(config['batch_size'])
            db_source = config['db_sqlserver']
            cr_source = db_source.cursor()
            db_desc = config['db_mysql']
            cr_desc = db_desc.cursor()

            # print('delete table:{0} all data!'.format(get_mapping_tname(tab)))
            # st_desc = """select {0} as 'pk' from {1} """.format(v_pk_cols, tab)
            # cr_source.execute(st_desc)
            # for r in list(cr_source.fetchall()):
            #     v_del = get_sync_where(v_pk_names, r[0])
            #     cr_desc.execute('delete from {0} where {1}'.format(get_mapping_tname(tab), v_del))
            # print('delete table:{0} all data ok!'.format(get_mapping_tname(tab)))

            v_sql = "select  {0} as 'pk',{1} from {2} with(nolock)"\
                    .format(v_pk_cols, get_tab_columns(config, tab),tab)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                v_sql = ''
                for r in list(rs_source):
                    rs_source_desc = cr_source.description
                    ins_val = ""
                    for j in range(1, len(r)):
                        col_type = str(rs_source_desc[j][1])
                        if r[j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == "1":  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                        elif col_type == "5":  # int,decimal
                            ins_val = ins_val + "'" + str(r[j]) + "',"
                        elif col_type == "4":  # datetime
                            ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                        elif col_type == "3":  # bit
                            if str(r[j]) == "True":  # bit
                                ins_val = ins_val + "'" + "1" + "',"
                            elif str(r[j]) == "False":  # bit
                                ins_val = ins_val + "'" + "0" + "',"
                            else:  # bigint ,int
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                        elif col_type == "2":  # timestamp
                            ins_val = ins_val + "null,"
                        else:
                            ins_val = ins_val + "'" + str(r[j]) + "',"
                    v_sql = v_sql + '(' + ins_val + config['sync_col_val'].replace('$PK$',
                                                                                   r[0].replace('^^^', '_')) + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]
                config['run_sql'] = convert(batch_sql)
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)

                if n_tab_total_rows == 0:
                    print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                          format(tab, n_tab_total_rows, round(i_counter / 1 * 100, 2)))
                else:
                    print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                          .format(get_time(), tab, n_tab_total_rows, i_counter,
                                  round(i_counter / n_tab_total_rows * 100, 2), str(get_seconds(start_time))),
                          end='')
                rs_source = cr_source.fetchmany(n_batch_size)
            db_desc.commit()
            print('')
        return status
    except Exception as e:
        print('sync_sqlserver_init exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def sync_sqlserver_init_nopk(config,ftab):
    try:
        tab=ftab.split(':')[0]
        status = False
        if (check_mysql_tab_exists(config,get_mapping_tname(tab))==0 \
                or (check_mysql_tab_exists(config,get_mapping_tname(tab))>0
                     and check_mysql_tab_sync(config,get_mapping_tname(tab))==0)):
            status           = True
            i_counter        = 0
            start_time       = datetime.datetime.now()
            n_tab_total_rows = get_sync_table_total_rows(config,tab,'')
            ins_sql_header   = get_tab_header(config,tab)
            v_tab_cols       = get_tab_columns(config,tab)
            v_pk_names       = get_sync_table_pk_names(config,tab)
            v_pk_cols_mysql  = get_sync_table_pk_vals_mysql(config, tab)
            n_batch_size     = int(config['batch_size'])
            db_source        = config['db_sqlserver']
            cr_source        = db_source.cursor()
            db_desc          = config['db_mysql']
            cr_desc          = db_desc.cursor()

            # print('delete table:{0} all data!'.format(get_mapping_tname(tab)))
            # st_desc = """select {0} as 'pk' from {1} """.format(v_pk_cols_mysql, tab)
            # cr_desc.execute(st_desc)
            # rs_desc = cr_desc.fetchall()
            # for r in list(rs_desc):
            #     v_del = get_sync_where(v_pk_names, r[0])
            #     cr_desc.execute('delete from {0} where {1}'.format(get_mapping_tname(tab), v_del))
            # print('delete table:{0} all data ok!'.format(get_mapping_tname(tab)))

            v_sql = "select {0} from {1} with(nolock)".format(v_tab_cols,tab)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                v_sql      = ''
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ""
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if  rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == "1":  #varchar,date
                            ins_val = ins_val + "'"+format_sql(str(rs_source[i][j])) + "',"
                        elif col_type == "5":  #int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j])+ "',"
                        elif col_type == "4":  #datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        elif col_type == "3":  # bit
                            if str(rs_source[i][j]) == "True":  # bit
                                ins_val = ins_val + "'" + "1" + "',"
                            elif str(rs_source[i][j]) == "False":  # bit
                                ins_val = ins_val + "'" + "0" + "',"
                            else:  # bigint ,int
                                ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == "2":  # timestamp
                            ins_val = ins_val + "null,"
                        else:
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                    v_sql = v_sql +'('+ins_val+config['sync_col_val']+'),'
                batch_sql = ins_sql_header + v_sql[0:-1]

                config['run_sql'] = convert(batch_sql)
                cr_desc.execute(batch_sql)
                i_counter = i_counter +len(rs_source)

                if n_tab_total_rows == 0:
                    print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                          format(tab, n_tab_total_rows,round(i_counter / 1 * 100,2)))
                else:
                    print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                          .format(get_time(), tab, n_tab_total_rows, i_counter,
                                  round(i_counter / n_tab_total_rows * 100, 2), str(get_seconds(start_time))), end='')
                rs_source = cr_source.fetchmany(n_batch_size)
            db_desc.commit()
            print('')
        return status
    except Exception as e:
        print('sync_sqlserver_init exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def sync_sqlserver_init(config):
    config_init = {}
    for v in config['sync_table'].split(","):
        tab = v.split(':')[0].lower()
        if check_full_sync(config, tab):
            print('table:{0} sync_sqlserver_init_nopk'.format(tab))
            config_init[tab]= sync_sqlserver_init_nopk(config,v)
        else:
            print('table:{0} sync_sqlserver_init_pk'.format(tab))
            config_init[tab]= sync_sqlserver_init_pk(config, v)
    return config_init

def sync_sqlserver_init_full_tab(config, ftab):
    if check_full_sync(config, ftab.split(':')[0]):
        i_counter = 0
        tab = ftab.split(':')[0]
        v_where = get_sync_where_incr(ftab)
        start_time = datetime.datetime.now()
        n_tab_total_rows = get_sync_table_total_rows(config, tab, '')
        ins_sql_header = get_tab_header(config, tab)
        n_batch_size = int(config['batch_size'])
        db_source = config['db_sqlserver']
        cr_source = db_source.cursor()
        db_desc = config['db_mysql']
        cr_desc = db_desc.cursor()
        v_sql = "select * from {0} with(nolock) {1} ".format(tab, v_where)
        cr_source.execute(v_sql)
        rs_source = cr_source.fetchmany(n_batch_size)
        while rs_source:
            batch_sql = ''
            v_sql = ''
            for i in range(len(rs_source)):
                rs_source_desc = cr_source.description
                ins_val = ''
                for j in range(len(rs_source[i])):
                    col_type = str(rs_source_desc[j][1])
                    if rs_source[i][j] is None:
                        ins_val = ins_val + "null,"
                    elif col_type == "1":  # varchar,date
                        ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                    elif col_type == "5":  # int,decimal
                        ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                    elif col_type == "4":  # datetime
                        ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                    elif col_type == "3":  # bit
                        if str(rs_source[i][j]) == "True":  # bit
                            ins_val = ins_val + "'" + "1" + "',"
                        elif str(rs_source[i][j]) == "False":  # bit
                            ins_val = ins_val + "'" + "0" + "',"
                        else:  # bigint ,int
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                    elif col_type == "2":  # timestamp
                        ins_val = ins_val + "null,"
                    else:
                        ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                v_sql = v_sql + '(' + ins_val + config['sync_col_val'] + '),'
            batch_sql = ins_sql_header + v_sql[0:-1]

            # noinspection PyBroadException
            try:
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)
            except:
                print(traceback.format_exc())
                print(batch_sql)
                sys.exit(0)
            db_desc.commit()
            print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                  .format(get_time(), tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2),
                          str(get_seconds(start_time))), end='')
            rs_source = cr_source.fetchmany(n_batch_size)
        print('')
        if n_tab_total_rows == 0:
            print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                  format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2)), end='')
        else:
            print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                  format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')

def get_pk_vals_sqlserver(config, ftab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    tab = ftab.split(':')[0]
    v_pk_cols = get_sync_table_pk_vals(config, tab)
    v_sql = "select {0} from {1} with(nolock) {2}".format(v_pk_cols, tab, get_sync_where_incr(ftab))
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    l_pk_vals = []
    for i in list(rs_source):
        l_pk_vals.append(i[0])
    cr_source.close()
    return l_pk_vals

def get_temp_table_cols(v_pk_names):
    result = ''
    for i in range(v_pk_names.count(',') + 1):
        result = result + 'v{0},'.format(str(i))
    return result[0:-1]

def sync_sqlserver_data_pk(config, ftab, config_init):
    try:
        # start sync dml data
        config['sync_amount'] = 0
        if not check_full_sync(config, ftab.split(':')[0]) and not config_init[ftab.split(':')[0]]:
            tab = ftab.split(':')[0]
            v_where = get_sync_where_incr(ftab, config)
            v_where_mysql = get_sync_where_incr_mysql(ftab,config)
            i_counter = 0
            n_tab_total_rows = get_sync_table_total_rows(config, tab, v_where)
            ins_sql_header = get_tab_header(config, tab)
            v_pk_names = get_sync_table_pk_names(config, tab)
            v_pk_cols  = get_sync_table_pk_vals(config, tab)
            v_pk_cols_mysql = get_sync_table_pk_vals_mysql(config, tab)
            n_batch_size = int(config['batch_size_incr'])
            db_source  = config['db_sqlserver']
            cr_source  = db_source.cursor()
            db_desc    = config['db_mysql']
            cr_desc    = db_desc.cursor()

            if ftab.split(':')[1] == '':
                print("Full Sync Table :{0} ...".format(ftab.split(':')[0]))
                print('DB:{0},delete {1} table data please wait...'
                      .format(config['db_mysql_string'],get_mapping_tname(tab)))

                st_desc = """select {0} as 'pk' from {1}""".format(v_pk_cols_mysql, get_mapping_tname(tab))
                cr_desc.execute(st_desc)
                rs_desc = cr_desc.fetchall()
                for r in list(rs_desc):
                   v_del = get_sync_where(v_pk_names, r[0])
                   cr_desc.execute('delete from {0} where {1}'.format(get_mapping_tname(tab), v_del))
                print('delete table:{0} all data ok!'.format(get_mapping_tname(tab)))
            else:
                print("Increment Sync Table  :{0} for In recent {1} {2}..."
                      .format(ftab.split(':')[0], ftab.split(':')[2], config['sync_time_type']))
                print('DB:{0},delete {1} table increment data for In recent {2} {3} please wait...'
                      .format(config['db_mysql_string'], get_mapping_tname(tab),ftab.split(':')[2],config['sync_time_type']))
                print('delete from {0} {1}'.format(get_mapping_tname(tab), v_where_mysql))
                cr_desc.execute('delete from {0} {1}'.format(get_mapping_tname(tab), v_where_mysql))

            n_rows = 0
            start_time = datetime.datetime.now()
            v_sql = """select {0} as 'pk',{1} from {2} with(nolock) {3}
                    """.format(v_pk_cols, get_tab_columns(config, tab), tab, v_where)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                v_sql = ''
                v_sql_del = ''
                n_rows = n_rows + len(rs_source)
                print("\r{0},Scanning table:{1},{2}/{3} rows,elapsed time:{4}s..."
                      .format(get_time(),get_mapping_tname(tab),str(n_rows),str(n_tab_total_rows),
                              str(get_seconds(start_time))),end='')
                rs_source_desc = cr_source.description
                if len(rs_source) > 0:
                    for r in list(rs_source):
                        ins_val = ""
                        for j in range(1, len(r)):
                            col_type = str(rs_source_desc[j][1])
                            if r[j] is None:
                                ins_val = ins_val + "null,"
                            elif col_type == "1":  # varchar,date
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                            elif col_type == "5":  # int,decimal
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == "4":  # datetime
                                ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                            elif col_type == "3":  # bit
                                if str(r[j]) == "True":  # bit
                                    ins_val = ins_val + "'" + "1" + "',"
                                elif str(r[j]) == "False":  # bit
                                    ins_val = ins_val + "'" + "0" + "',"
                                else:  # bigint ,int
                                    ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == "2":  # timestamp
                                ins_val = ins_val + "null,"
                            else:
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"

                        ins_val = ins_val + config['sync_col_val'].replace('$PK$',r[0].replace('^^^','_'))
                        v_sql = v_sql + '(' + ins_val + '),'
                        v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0]) + "$$$"
                    batch_sql = ins_sql_header + v_sql[0:-1]

                    for d in v_sql_del[0:-3].split('$$$'):
                        config['run_sql'] = convert('delete from {0} where {1}'.format(get_mapping_tname(tab), d))
                        cr_desc.execute('delete from {0} where {1}'.format(get_mapping_tname(tab), d))
                    config['run_sql'] = batch_sql
                    cr_desc.execute(batch_sql)
                    i_counter = i_counter + len(rs_source)

                    if n_tab_total_rows == 0:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                              .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2),
                                      str(get_seconds(start_time))), end='')
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                              .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2),
                                      str(get_seconds(start_time))), end='')
                rs_source = cr_source.fetchmany(n_batch_size)
                print('')
            db_desc.commit()

            if config['run_mode'] == 'remote':
                config['sync_duration'] = str(get_seconds(start_time))
                config['sync_table_inteface'] = tab
                config['sync_amount'] = str(n_rows)
                write_sync_log_detail(config,ftab)
    except Exception as e:
        print('sync_sqlserver_data_pk exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def sync_sqlserver_data_nopk(config, ftab, config_init):
    try:
        config['sync_amount'] = 0
        if check_full_sync(config, ftab.split(':')[0]) and not config_init[ftab.split(':')[0]]:
            tab = ftab.split(':')[0]
            v_where = get_sync_where_incr(ftab, config)
            v_where_mysql = get_sync_where_incr_mysql(ftab, config)
            i_counter = 0
            n_tab_total_rows = get_sync_table_total_rows(config, tab, v_where)
            ins_sql_header = get_tab_header(config, tab)
            n_batch_size = int(config['batch_size'])
            v_pk_names = get_sync_table_pk_names(config, tab)
            v_pk_cols_mysql = get_sync_table_pk_vals_mysql(config, tab)
            db_source = config['db_sqlserver']
            cr_source = db_source.cursor()
            db_desc = config['db_mysql']
            cr_desc = db_desc.cursor()

            if ftab.split(':')[1] == '':
                print("Full Sync Table :{0} ...".format(ftab.split(':')[0]))
                print('DB:{0},delete {1} table data,please wait...'.
                      format(config['db_mysql_string'],get_mapping_tname(tab)))
                config['run_sql'] = 'delete from {0}'.format(get_mapping_tname(tab))
                st_desc = """select {0} as 'pk' from {1}""".format(v_pk_cols_mysql, get_mapping_tname(tab))
                cr_desc.execute(st_desc)
                rs_desc = cr_desc.fetchall()
                for r in list(rs_desc):
                    v_del = get_sync_where(v_pk_names, r[0])
                    cr_desc.execute('delete from {0} where {1}'.format(get_mapping_tname(tab), v_del))
                print('delete table:{0} all data ok!'.format(get_mapping_tname(tab)))

            else:
                print("Increment Sync Table  :{0} for In recent {1} {2}...".
                      format(ftab.split(':')[0], ftab.split(':')[2], config['sync_time_type']))
                print('DB:{0},delete {1} table recent {2} {3} data please wait...'.
                      format(config['db_mysql_string'], tab,
                             ftab.split(':')[2],
                             config['sync_time_type']))
                cr_desc.execute('delete from {0} {1}'.format(get_mapping_tname(tab), v_where_mysql))


            v_sql = """select {0} from {1} with(nolock) {2}""".format(get_tab_columns(config, tab), tab, v_where)
            n_rows = 0
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            start_time = datetime.datetime.now()
            while rs_source:
                v_sql = ''
                n_rows = n_rows + len(rs_source)
                print("\r{0},Scanning table:{1},{2}/{3} rows,elapsed time:{4}s..."
                      .format(get_time(), get_mapping_tname(tab), str(n_rows), str(n_tab_total_rows),
                              str(get_seconds(start_time))), end='')
                rs_source_desc = cr_source.description
                if len(rs_source) > 0:
                    for r in list(rs_source):
                        ins_val = ""
                        for j in range(len(r)):
                            col_type = str(rs_source_desc[j][1])
                            if r[j] is None:
                                ins_val = ins_val + "null,"
                            elif col_type == "1":  # varchar,date
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                            elif col_type == "5":  # int,decimal
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == "4":  # datetime
                                ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                            elif col_type == "3":  # bit
                                if str(r[j]) == "True":  # bit
                                    ins_val = ins_val + "'" + "1" + "',"
                                elif str(r[j]) == "False":  # bit
                                    ins_val = ins_val + "'" + "0" + "',"
                                else:  # bigint ,int
                                    ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == "2":  # timestamp
                                ins_val = ins_val + "null,"
                            else:
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                        ins_val = ins_val + config['sync_col_val']
                        v_sql = v_sql + '(' + ins_val + '),'
                    batch_sql = ins_sql_header + v_sql[0:-1]
                    config['run_sql'] = batch_sql
                    cr_desc.execute(batch_sql)
                    i_counter = i_counter + len(rs_source)

                    if n_tab_total_rows == 0:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                              .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2),
                                      str(get_seconds(start_time))), end='')
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                              .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2),
                                      str(get_seconds(start_time))), end='')
                rs_source = cr_source.fetchmany(n_batch_size)
            db_desc.commit()

            if config['run_mode'] == 'remote':
                config['sync_duration'] = str(get_seconds(start_time))
                config['sync_table_inteface'] = tab
                config['sync_amount'] = str(n_rows)
                write_sync_log_detail(config,ftab)
    except Exception as e:
        print('sync_sqlserver_data_nopk exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def get_nday_list(n):
    before_n_days = []
    for i in range(0, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days

def sync_sqlserver_data_pk_7(config, ftab):
    try:
        for day in get_nday_list(config['sync_repair_day']):
            tab = ftab.split(':')[0]
            v_where            = get_sync_where_day(ftab, day)
            i_counter          = 0
            n_tab_total_rows   = get_sync_table_total_rows(config, tab, v_where)
            n_tab_total_rows2  = get_sync_table_total_rows_mysql(config, get_mapping_tname(tab), v_where)

            ins_sql_header     = get_tab_header(config, tab)
            v_pk_names         = get_sync_table_pk_names(config, tab)
            v_pk_cols          = get_sync_table_pk_vals(config, tab)
            n_batch_size       = int(config['batch_size_incr'])
            db_source          = config['db_sqlserver']
            db_source2         = config['db_sqlserver2']
            cr_source          = db_source.cursor()
            db_desc            = config['db_mysql']
            cr_desc            = db_desc.cursor()
            v_sql              = """select {0} as 'pk',{1} from {2} with(nolock) {3}
                                 """.format(v_pk_cols, get_tab_columns(config, tab), tab, v_where)
            n_rows             = 0

            if ftab.split(':')[1] == '':
                print("Sync Table {} have no increment column ,skipping...".format(ftab.split(':')[0]))
                continue

            # 检测同一天SQLServer和MySQL表中数据量是否一致，一致则跳过本次循环，不一致则同步当天数据
            if n_tab_total_rows == n_tab_total_rows2 :
               print('{}：`{}`表数据相等，跳过同步({})!'.format(day,tab,n_tab_total_rows))
               continue
            elif n_tab_total_rows< n_tab_total_rows2:
                print('{}:`{}`表SQLServer与MySQL数据不一致({}/{})，同步中...'.format(day, tab, n_tab_total_rows, n_tab_total_rows2))
                cr_desc.execute('delete from {0} {1}'.format(get_mapping_tname(tab), v_where))
                print('DB:{0},delete {1} table {2} data ok!'.format(config['db_mysql_string'], get_mapping_tname(tab),v_where))
            else:
                print('{}:`{}`表SQLServer与MySQL数据不一致({}/{})，同步中...'.format(day,tab,n_tab_total_rows,n_tab_total_rows2))
                cr_desc.execute('delete from {0} {1}'.format(get_mapping_tname(tab), v_where))

            cr_source.execute(v_sql)
            rs_source  = cr_source.fetchmany(n_batch_size)
            start_time = datetime.datetime.now()

            while rs_source:
                batch_sql = ""
                batch_sql_del = ""
                v_sql = ''
                v_sql_del = ''
                n_rows = n_rows + len(rs_source)
                print("\r{0},Scanning table:{1},{2}/{3} rows,elapsed time:{4}s..."
                      .format(get_time(),get_mapping_tname(tab),str(n_rows),str(n_tab_total_rows),
                              str(get_seconds(start_time))),end='')
                rs_source_desc = cr_source.description
                if len(rs_source) > 0:
                    for r in list(rs_source):
                        ins_val = ""
                        for j in range(1, len(r)):
                            col_type = str(rs_source_desc[j][1])
                            if r[j] is None:
                                ins_val = ins_val + "null,"
                            elif col_type == "1":  # varchar,date
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                            elif col_type == "5":  # int,decimal
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == "4":  # datetime
                                ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                            elif col_type == "3":  # bit
                                if str(r[j]) == "True":  # bit
                                    ins_val = ins_val + "'" + "1" + "',"
                                elif str(r[j]) == "False":  # bit
                                    ins_val = ins_val + "'" + "0" + "',"
                                else:  # bigint ,int
                                    ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == "2":  # timestamp
                                ins_val = ins_val + "null,"
                            else:
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"

                        ins_val = ins_val + config['sync_col_val'].replace('$PK$',r[0].replace('^^^','_'))
                        v_sql = v_sql + '(' + ins_val + '),'
                        v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0]) + "$$$"
                    batch_sql = ins_sql_header + v_sql[0:-1]

                    for d in v_sql_del[0:-3].split('$$$'):
                        cr_desc.execute('delete from {0} where {1}'.format(get_mapping_tname(tab), d))
                    config['run_sql'] = batch_sql
                    cr_desc.execute(batch_sql)
                    i_counter = i_counter + len(rs_source)

                    print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                          .format(tab, n_tab_total_rows,
                                  i_counter, round(i_counter / n_tab_total_rows * 100, 2),
                                  str(get_seconds(start_time))), end='')
                    if n_tab_total_rows == 0:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                              .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2),
                                      str(get_seconds(start_time))), end='')
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%,elapsed time:{4}s"
                              .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2),
                                      str(get_seconds(start_time))), end='')
                rs_source = cr_source.fetchmany(n_batch_size)
                print('')

            if tab == 'tc.recordarchive' and len(config['sync_col_val'].split(',')) > 1:
                print('update table tc_recordarchive es_id...'.format(tab))
                sql = """UPDATE tc_recordarchive
                           SET es_id=CONCAT(market_id,'_',sid,'_',parkid,'_',inwid,'_',inid,'_',outwid,'_',outid),
                               doc_id=MD5(CONCAT(market_id,'_',sid,'_',parkid,'_',inwid,'_',inid,'_',outwid,'_',outid))
                             where es_id ='{0}'""".format(config['sync_col_val'].split(',')[0] + '_$PK$')
                print(sql)
                cr_desc.execute(sql)

            db_desc.commit()

    except Exception as e:
        print('sync_sqlserver_data_pk exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def sync_sqlserver_data_pk_7_nopk(config, ftab):
    try:
        for day in get_nday_list(config['sync_repair_day']):
            tab = ftab.split(':')[0]
            i_counter = 0
            start_time = datetime.datetime.now()
            v_where     = get_sync_where_day(ftab, day)
            n_tab_total_rows = get_sync_table_total_rows(config, tab, v_where)
            n_tab_total_rows2  = get_sync_table_total_rows_mysql(config, get_mapping_tname(tab), v_where)

            ins_sql_header = get_tab_header(config, tab)
            v_tab_cols = get_tab_columns(config, tab)
            n_batch_size = int(config['batch_size'])
            db_source = config['db_sqlserver']
            cr_source = db_source.cursor()
            db_desc = config['db_mysql']
            cr_desc = db_desc.cursor()

            if ftab.split(':')[1] == '':
                print("Sync Table {} have no increment column ,skipping...".format(ftab.split(':')[0]))
                continue

            # 检测同一天SQLServer和MySQL表中数据量是否一致，一致则跳过本次循环，不一致则同步当天数据
            if n_tab_total_rows == n_tab_total_rows2:
                print('{}：`{}`表数据相等，跳过同步({})!'.format(day, tab, n_tab_total_rows))
                continue
            else:
                print('{}:`{}`表SQLServer与MySQL数据不一致({}/{})，同步中...'
                      .format(day, tab, n_tab_total_rows,n_tab_total_rows2))
                cr_desc.execute('delete from {0} {1}'.format(get_mapping_tname(tab), v_where))
                print('DB:{0},delete {1} table {2} data ok!'
                      .format(config['db_mysql_string'], get_mapping_tname(tab),v_where))

            v_sql = "select {0} from {1} with(nolock) {2}".format(v_tab_cols, tab,v_where)
            #print(v_sql)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                batch_sql = ""
                v_sql = ''
                for i in range(len(rs_source)):
                    rs_source_desc = cr_source.description
                    ins_val = ""
                    for j in range(len(rs_source[i])):
                        col_type = str(rs_source_desc[j][1])
                        if rs_source[i][j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == "1":  # varchar,date
                            ins_val = ins_val + "'" + format_sql(str(rs_source[i][j])) + "',"
                        elif col_type == "5":  # int,decimal
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == "4":  # datetime
                            ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                        elif col_type == "3":  # bit
                            if str(rs_source[i][j]) == "True":  # bit
                                ins_val = ins_val + "'" + "1" + "',"
                            elif str(rs_source[i][j]) == "False":  # bit
                                ins_val = ins_val + "'" + "0" + "',"
                            else:  # bigint ,int
                                ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        elif col_type == "2":  # timestamp
                            ins_val = ins_val + "null,"
                        else:
                            ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                    v_sql = v_sql + '(' + ins_val + config['sync_col_val'] + '),'
                batch_sql = ins_sql_header + v_sql[0:-1]

                config['run_sql'] = convert(batch_sql)
                cr_desc.execute(batch_sql)
                i_counter = i_counter + len(rs_source)

                if n_tab_total_rows == 0:
                    print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                          format(tab, n_tab_total_rows, round(i_counter / 1 * 100, 2)))
                else:
                    print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                          .format(get_time(), tab, n_tab_total_rows, i_counter,
                                  round(i_counter / n_tab_total_rows * 100, 2), str(get_seconds(start_time))),
                          end='')
                rs_source = cr_source.fetchmany(n_batch_size)
            print('')

        if tab == 'tc.recordarchive' and len(config['sync_col_val'].split(',')) > 1:
            print('update table tc_recordarchive es_id...'.format(tab))
            sql = """UPDATE tc_recordarchive
                            SET es_id=CONCAT(market_id,'_',sid,'_',parkid,'_',inwid,'_',inid,'_',outwid,'_',outid),
                                doc_id=MD5(CONCAT(market_id,'_',sid,'_',parkid,'_',inwid,'_',inid,'_',outwid,'_',outid))
                               where es_id ='{0}'""".format(config['sync_col_val'].split(',')[0] + '_$PK$')
            #print(sql)
            cr_desc.execute(sql)
        db_desc.commit()

    except Exception as e:
        print('sync_sqlserver_data_pk_7_nopk exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def sync_sqlserver_data(config, config_init):
    start_time = datetime.datetime.now()
    amount = 0
    for v in config['sync_table'].split(","):
        tab = v.split(':')[0].lower()
        if check_full_sync(config, tab):
            sync_sqlserver_data_nopk(config, v, config_init)
        else:
            sync_sqlserver_data_pk(config, v, config_init)

        if config['run_mode'] == 'remote':
           amount = amount + int(config['sync_amount'])

    if config['run_mode'] == 'remote':
        config['sync_amount'] = str(amount)
        config['sync_duration'] = str(get_seconds(start_time))
        write_sync_log(config)

def check_sqlserver_data(config):
    print('check recent 7 day data...')
    for v in config['sync_table'].split(","):
        tab = v.split(':')[0].lower()
        if not check_full_sync(config, tab):
           sync_sqlserver_data_pk_7(config, v)
        else:
           sync_sqlserver_data_pk_7_nopk(config, v)

def cleaning_table(config):
    print('starting cleaning_table please wait...')
    db = config['db_mysql']
    cr = db.cursor()
    desc = config['db_mysql_string']
    start_time = datetime.datetime.now()
    # 如果索引不存在，则建立索引
    v_chk_idx_sql = "SELECT count(0) FROM information_schema.innodb_sys_indexes WHERE NAME='idx_tc_recordarchive_n1'"
    v_cre_idx_sql = "CREATE INDEX idx_tc_recordarchive_n1 ON tc_recordarchive(intime,carno,pkid)"

    # 表数据去重
    n_cnt_rep_sql = 0
    v_cnt_rep_sql = """select count(0) from tc_recordarchive
                          where pkid in(select pkid from (select  max(pkid) AS pkid from tc_recordarchive 
                                         where indeviceentrytype=1
                                           group by carno,intime having count(0)>1) t)"""

    v_del_rep_sql = """delete from tc_recordarchive
                        where pkid in(select pkid from (select  max(pkid) AS pkid from tc_recordarchive 
                                       where indeviceentrytype=1
                                         group by carno,intime having count(0)>1) t)"""
    for i in config['sync_table'].split(","):
        tab = get_mapping_tname(i.split(':')[0])
        if tab == "tc_recordarchive" and check_mysql_col_exists(config, tab, 'pkid') > 0:
            # 创建索引
            print('DB:{0} cleaning table tc_recordarchive create index...'.format(desc))
            cr.execute(v_chk_idx_sql)
            rs = cr.fetchone()
            if rs[0] == 0:
                print('DB:{0},createing index idx_tc_recordarchive_n1 for {1} please wait...'.format(desc, tab))
                cr.execute(v_cre_idx_sql)
                print('DB:{0},Table:{1} index idx_tc_recordarchive_n1 create complete!'.format(desc, tab))
            else:
                print('DB:{0} cleaning table tc_recordarchive index idx_tc_recordarchive_n1 already exists!')
            # 删除重复数据
            print('DB:{0} cleaning table tc_recordarchive delete repeat data...'.format(desc))
            cr.execute(v_cnt_rep_sql)
            rs = cr.fetchone()
            if rs[0] > 0:
                print('DB:{0},deleting table {1} repeat data please wait...'.format(desc, tab))
                cr.execute(v_del_rep_sql)
                print('DB:{0},Table:{1} delete repeat data {2} rows!'.format(desc, tab, rs[0]))
            else:
                print('DB:{0} cleaning table tc_recordarchive no repeat data!'.format(desc))

    db.commit()
    cr.close()
    print('complete cleaning_table,elaspse:{0}s'.format(str(get_seconds(start_time))))

def check_full_sync(config, tab):
    if check_sqlserver_tab_exists_pk(config, tab) == 0:
        return True
    else:
        return False

def write_local_config_file_json(config):
    file_name = config['script_path'] + '/config/' + config['sync_tag'] + '.json'
    with open(file_name, 'w') as f:
        f.write(json.dumps(config, ensure_ascii=False, indent=4, separators=(',', ':')))

def write_local_config_file(config):
    sync_db_sour = config['db_sqlserver_ip'] + ':' + \
                   config['db_sqlserver_port'] + ':' + \
                   config['db_sqlserver_service'] + ':' + \
                   config['db_sqlserver_user'] + ':' + \
                   config['db_sqlserver_pass']

    sync_db_dest = config['db_mysql_ip'] + ':' + \
                   config['db_mysql_port'] + ':' + \
                   config['db_mysql_service'] + ':' + \
                   config['db_mysql_user'] + ':' + \
                   config['db_mysql_pass']

    file_name   = config['script_path'] + '/config/' + config['sync_tag'] + '.ini'
    file_handle = open(file_name, 'w')
    file_handle.write('[sync]' + '\n')
    file_handle.write('sync_tag={0}\n'.format(config['sync_tag']))
    file_handle.write('sync_ywlx={0}\n'.format(config['sync_ywlx']))
    file_handle.write('sync_ywlx_name={0}\n'.format(config['sync_ywlx_name']))
    file_handle.write('sync_type={0}\n'.format(config['sync_type']))
    file_handle.write('sync_type_name={0}\n'.format(config['sync_type_name']))
    file_handle.write('sync_db_server={0}\n'.format(sync_db_sour))
    file_handle.write('sync_db_mysql={0}\n'.format(sync_db_dest))
    file_handle.write('server_id={0}\n'.format(config['server_id']))
    file_handle.write('server_desc={0}\n'.format(config['server_desc']))
    file_handle.write('run_time={0}\n'.format(config['run_time']))
    file_handle.write('api_server={0}\n'.format(config['api_server']))
    file_handle.write('sync_table={0}\n'.format(config['sync_table']))
    file_handle.write('batch_size={0}\n'.format(config['batch_size']))
    file_handle.write('batch_size_incr={0}\n'.format(config['batch_size_incr']))
    file_handle.write('sync_gap={0}\n'.format(config['sync_gap']))
    file_handle.write('sync_col_name={0}\n'.format(config['sync_col_name']))
    file_handle.write('sync_repair_day={0}\n'.format(config['sync_repair_day']))
    file_handle.write('sync_col_val={0}\n'.format(config['sync_col_val']))
    file_handle.write('sync_time_type={0}\n'.format(config['sync_time_type']))
    file_handle.write('script_path={0}\n'.format(config['script_path']))
    file_handle.write('script_file={0}\n'.format(config['script_file']))
    file_handle.write('comments={0}\n'.format(config['comments']))
    file_handle.write('python3_home={0}\n'.format(config['python3_home']))
    file_handle.write('status={0}\n'.format(config['status']))
    file_handle.write('server_ip={0}\n'.format(config['server_ip']))
    file_handle.write('server_port={0}\n'.format(config['server_port']))
    file_handle.write('server_user={0}\n'.format(config['server_user']))
    file_handle.write('server_pass={0}\n'.format(config['server_pass']))
    file_handle.write('proxy_server={0}\n'.format(config['proxy_server']))
    file_handle.write('proxy_local_port={0}\n'.format(config['proxy_local_port']))
    file_handle.write('cols={0}\n'.format(config['cols']))
    file_handle.write('sync_time_type_name={0}\n'.format(config['sync_time_type_name']))
    file_handle.write('db_sqlserver_ip={0}\n'.format(config['db_sqlserver_ip']))
    file_handle.write('db_sqlserver_port={0}\n'.format(config['db_sqlserver_port']))
    file_handle.write('db_sqlserver_service={0}\n'.format(config['db_sqlserver_service']))
    file_handle.write('db_sqlserver_user={0}\n'.format(config['db_sqlserver_user']))
    file_handle.write('db_sqlserver_pass={0}\n'.format(config['db_sqlserver_pass']))
    file_handle.write('db_mysql_ip={0}\n'.format(config['db_mysql_ip']))
    file_handle.write('db_mysql_port={0}\n'.format(config['db_mysql_port']))
    file_handle.write('db_mysql_service={0}\n'.format(config['db_mysql_service']))
    file_handle.write('db_mysql_user={0}\n'.format(config['db_mysql_user']))
    file_handle.write('db_mysql_pass={0}\n'.format(config['db_mysql_pass']))
    file_handle.close()
    print("{0} export complete!".format(file_name))

def exception_connect_db(config, p_error):
    v_templete = '''
        <html>
           <head>
              <style type="text/css">
                  .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                  .xwtable thead td {font-size: 12px;color: #333333;
                                     text-align: center;background: url(table_top.jpg) repeat-x top center;
                                     border: 1px solid #ccc; font-weight:bold;}
                  .xwtable thead th {font-size: 12px;color: #333333;
                                     text-align: center;background: url(table_top.jpg) repeat-x top center;
                                     border: 1px solid #ccc; font-weight:bold;}
                  .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                  .xwtable tbody tr.alt-row {background: #f2f7fc;}
                  .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
              </style>
           </head>
           <body>             
              <table class='xwtable'>
                  <tr><td  width="30%">任务描述</td><td  width="70%">$$task_desc$$</td></tr>
                  <tr><td>任务标识</td><td>$$sync_tag$$</td></tr>                 
                  <tr><td>业务类型</td><td>$$sync_ywlx$$</td></tr>
                  <tr><td>同步方向</td><td>$$sync_type$$</td></tr>
                  <tr><td>同步服务器</td><td>$$server_id$$</td></tr>
                  <tr><td>源数据源</td><td>$$sync_db_sour$$</td></tr>
                  <tr><td>目标数据源</td><td>$$sync_db_dest$$</td></tr>
                  <tr><td>同步表名</td><td>$$sync_table$$</td></tr>
                  <tr><td>时间类型</td><td>$$sync_time_type$$</td></tr>
                  <tr><td>同步脚本</td><td>$$script_file$$</td></tr>
                  <tr><td>运行时间</td><td>$$run_time$$</td></tr>                
                  <tr><td>异常信息</td><td>$$run_error$$</td></tr>
              </table>                
           </body>
        </html>
       '''
    v_title = config.get('comments') + '数据同步数据库异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$sync_tag$$', config.get('sync_tag'))
    v_content = v_content.replace('$$sync_ywlx$$', config.get('sync_ywlx_name'))
    v_content = v_content.replace('$$sync_type$$', config.get('sync_type_name'))
    v_content = v_content.replace('$$server_id$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$sync_db_sour$$', config.get('sync_db_sour'))
    v_content = v_content.replace('$$sync_db_dest$$', config.get('sync_db_dest'))
    v_content = v_content.replace('$$sync_table$$', config.get('sync_table'))
    v_content = v_content.replace('$$sync_time_type$$', config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_time$$', config.get('run_time'))
    v_content = v_content.replace('$$run_error$$', str(p_error))
    #send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn', v_title, v_content)
    #send_mail465(config.get('send_server'),'190343@lifeat.cn', config.get('sendpass'), '190343@lifeat.cn', v_title, v_content)
    send_mail_param(config.get('send_server'),config.get('sender'),config.get('sendpass'),config.get('receiver'),v_title, v_content)


def exception_interface(config,v_title, v_content):
    v_templete = '''
           <html>
              <head>
                 <style type="text/css">
                     .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                     .xwtable thead td {font-size: 12px;color: #333333;
                                        text-align: center;background: url(table_top.jpg) repeat-x top center;
                                        border: 1px solid #ccc; font-weight:bold;}
                     .xwtable thead th {font-size: 12px;color: #333333;
                                        text-align: center;background: url(table_top.jpg) repeat-x top center;
                                        border: 1px solid #ccc; font-weight:bold;}
                     .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                     .xwtable tbody tr.alt-row {background: #f2f7fc;}
                     .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                 </style>
              </head>
              <body>             
                 $$TABLE$$           
              </body>
           </html>
          '''
    v_templete = v_templete.replace('$$TABLE$$', v_content)
    #send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn', v_title, v_templete)
    #send_mail465(config.get('send_server'),'190343@lifeat.cn', 'R86hyfjobMBYR76h', '190343@lifeat.cn', v_title, v_templete)
    send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'), config.get('receiver'),v_title, v_templete)

def exception_running(config, p_error):
    v_templete = '''
     <html>
        <head>
           <style type="text/css">
               .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
               .xwtable thead td {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
               .xwtable thead th {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
               .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
               .xwtable tbody tr.alt-row {background: #f2f7fc;}
               .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
           </style>
        </head>
        <body>             
           <table class='xwtable'>
               <tr><td  width="30%">任务描述</td><td  width="70%">$$task_desc$$</td></tr>
               <tr><td>任务标识</td><td>$$sync_tag$$</td></tr>
               <tr><td>业务类型</td><td>$$sync_ywlx$$</td></tr>
               <tr><td>同步方向</td><td>$$sync_type$$</td></tr>
               <tr><td>同步服务器</td><td>$$server_id$$</td></tr>
               <tr><td>源数据源</td><td>$$sync_db_sour$$</td></tr>
               <tr><td>目标数据源</td><td>$$sync_db_dest$$</td></tr>
               <tr><td>同步表名</td><td>$$sync_table$$</td></tr>
               <tr><td>时间类型</td><td>$$sync_time_type$$</td></tr>
               <tr><td>同步脚本</td><td>$$script_file$$</td></tr>
               <tr><td>运行时间</td><td>$$run_time$$</td></tr>
               <tr><td>运行语句</td><td>$$run_sql$$</td></tr>
               <tr><td>异常信息</td><td>$$run_error$$</td></tr>
           </table>                
        </body>
     </html>
    '''

    v_title   = config.get('comments') + '数据同步运行异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$sync_tag$$', config.get('sync_tag'))
    v_content = v_content.replace('$$sync_ywlx$$', config.get('sync_ywlx_name'))
    v_content = v_content.replace('$$sync_type$$', config.get('sync_type_name'))
    v_content = v_content.replace('$$server_id$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$sync_db_sour$$', config.get('sync_db_sour'))
    v_content = v_content.replace('$$sync_db_dest$$', config.get('sync_db_dest'))
    v_content = v_content.replace('$$sync_table$$', config.get('sync_table'))
    v_content = v_content.replace('$$sync_time_type$$', config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_time$$', config.get('run_time'))
    v_content = v_content.replace('$$run_sql$$', config.get('run_sql'))
    v_content = v_content.replace('$$run_error$$', str(p_error))
    #send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn', v_title, v_content)
    send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'), config.get('receiver'),v_title, v_content)

def recover_running(config):
    v_templete = '''
     <html>
        <head>
           <style type="text/css">
               .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
               .xwtable thead td {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
               .xwtable thead th {font-size: 12px;color: #333333;
                                  text-align: center;background: url(table_top.jpg) repeat-x top center;
                                  border: 1px solid #ccc; font-weight:bold;}
               .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
               .xwtable tbody tr.alt-row {background: #f2f7fc;}
               .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
           </style>
        </head>
        <body>             
           <table class='xwtable'>
               <tr><td  width="30%">任务描述</td><td  width="70%">$$task_desc$$</td></tr>
               <tr><td>任务标识</td><td>$$sync_tag$$</td></tr>
               <tr><td>业务类型</td><td>$$sync_ywlx$$</td></tr>
               <tr><td>同步方向</td><td>$$sync_type$$</td></tr>
               <tr><td>同步服务器</td><td>$$server_id$$</td></tr>
               <tr><td>源数据源</td><td>$$sync_db_sour$$</td></tr>
               <tr><td>目标数据源</td><td>$$sync_db_dest$$</td></tr>
               <tr><td>同步表名</td><td>$$sync_table$$</td></tr>
               <tr><td>时间类型</td><td>$$sync_time_type$$</td></tr>
               <tr><td>同步脚本</td><td>$$script_file$$</td></tr>
               <tr><td>运行时间</td><td>$$run_time$$</td></tr>
           </table>                
        </body>
     </html>
    '''
    v_title = config.get('comments') + '任务恢复为远程模式[★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$sync_tag$$', config.get('sync_tag'))
    v_content = v_content.replace('$$sync_ywlx$$', config.get('sync_ywlx_name'))
    v_content = v_content.replace('$$sync_type$$', config.get('sync_type_name'))
    v_content = v_content.replace('$$server_id$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$sync_db_sour$$', config.get('sync_db_sour'))
    v_content = v_content.replace('$$sync_db_dest$$', config.get('sync_db_dest'))
    v_content = v_content.replace('$$sync_table$$', config.get('sync_table').replace(',','<br>'))
    v_content = v_content.replace('$$sync_time_type$$', config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_time$$', config.get('run_time'))
    #send_mail25('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn', v_title, v_content)
    send_mail_param(config.get('send_server'),config.get('sender'),config.get('sendpass'),config.get('receiver'),v_title, v_content)

def disconnect(config):
    config['db_sqlserver'].close()
    config['db_sqlserver2'].close()
    config['db_sqlserver3'].close()
    config['db_mysql'].close()
    config['db_mysql3'].close()

def sync(config, debug, workdir):
    # init dict
    config = get_config_from_db(config, workdir)

    if config is None:
       exit(0)

    # print dict
    if debug:
        print_dict(config)

    # update sync task status is running
    update_sync_status(config, '1')

    # sync table ddl
    sync_sqlserver_ddl(config, debug)

    # init sync table
    config_init = sync_sqlserver_init(config)

    # sync data
    while True:
        # sync increment data
        sync_sqlserver_data(config, config_init)

        # check  recently 7 day data
        check_sqlserver_data(config)

        disconnect(config)

        # update sync task status is complete
        update_sync_status(config, '0')

        sys.exit(0)


def main():
    # init variable
    config = ''
    debug = False
    warnings.filterwarnings("ignore")
    # get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-workdir":
            workdir = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True
    print('config=', config)
    if config == '':
        print('Please input tag value!')
        sys.exit(0)

    # process
    sync(config, debug, workdir)


if __name__ == "__main__":
    main()
