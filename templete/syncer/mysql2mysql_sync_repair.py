#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Func : MySQL->MySQL增量同步（实时车流）
# @Software: PyCharm

import sys,time
import traceback
import configparser
import warnings
import pymssql
import pymysql
import datetime
import hashlib
import smtplib
from email.mime.text import MIMEText
import json
import urllib.parse
import urllib.request
import ssl
import requests,os
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
    for p in [25,465]:
        if socket_port("smtp.exmail.qq.com",p):
            return p

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail25(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        #server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server = smtplib.SMTP("smtp.exmail.qq.com", get_available_port())
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_sqlserver(ip,port,service,user,password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service,charset='utf8')
    return conn

def get_db_mysql_sour(config):
    return get_ds_mysql(config['db_mysql_sour_ip'],config['db_mysql_sour_port'],config['db_mysql_sour_service'],\
                        config['db_mysql_sour_user'],config['db_mysql_sour_pass'])

def get_db_mysql_desc(config):
    return get_ds_mysql(config['db_mysql_desc_ip'],config['db_mysql_desc_port'],config['db_mysql_desc_service'],\
                        config['db_mysql_desc_user'],config['db_mysql_desc_pass'])


def get_sync_time_type_name(sync_time_type):
    if sync_time_type=="day":
       return '天'
    elif sync_time_type=="hour":
       return '小时'
    elif sync_time_type=="min":
       return '分'
    else:
       return ''

def aes_decrypt(p_cfg,p_password,p_key):
    values = {
        'password': p_password,
        'key':p_key
    }
    url = 'http://$$API_SERVER$$/read_db_decrypt'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    try:
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
    except:
        api = query_health_api(p_cfg)
        url = 'http://{}/read_db_decrypt'.format(api)
        print('aes_decrypt=>query_health_api=', url)
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

def write_local_config_file_json(config):
    file_name = config['script_path'] + '/config/' + config['sync_tag'] + '.json'
    with open(file_name, 'w') as f:
        f.write(json.dumps(config, ensure_ascii=False, indent=4, separators=(',', ':')))


def get_local_config_json(fname):
    with open(fname, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def get_config_json(fname):
    with open(fname, 'r') as f:
         cfg = json.loads(f.read())

    cfg['run_mode'] = 'local'
    cfg['counter']  = cfg['counter'] + 1
    write_local_config_file_json(cfg)


    cfg['db_mysql_sour']  = get_ds_mysql(cfg['db_mysql_sour_ip'],
                                         cfg['db_mysql_sour_port'],
                                         cfg['db_mysql_sour_service'],
                                         cfg['db_mysql_sour_user'],
                                         cfg['db_mysql_sour_pass'])

    cfg['db_mysql_sour2'] = get_ds_mysql(cfg['db_mysql_sour_ip'],
                                         cfg['db_mysql_sour_port'],
                                         cfg['db_mysql_sour_service'],
                                         cfg['db_mysql_sour_user'],
                                         cfg['db_mysql_sour_pass'])

    cfg['db_mysql_sour3'] = get_ds_mysql(cfg['db_mysql_sour_ip'],
                                         cfg['db_mysql_sour_port'],
                                         cfg['db_mysql_sour_service'],
                                         cfg['db_mysql_sour_user'],
                                         cfg['db_mysql_sour_pass'])

    cfg['db_mysql_desc']  = get_ds_mysql(cfg['db_mysql_desc_ip'],
                                         cfg['db_mysql_desc_port'],
                                         cfg['db_mysql_desc_service'],
                                         cfg['db_mysql_desc_user'],
                                         cfg['db_mysql_desc_pass'])

    cfg['db_mysql_desc3'] = get_ds_mysql(cfg['db_mysql_desc_ip'],
                                         cfg['db_mysql_desc_port'],
                                         cfg['db_mysql_desc_service'],
                                         cfg['db_mysql_desc_user'],
                                         cfg['db_mysql_desc_pass'])

    return cfg


def check_api_server_status(cfg):
    print('check_api_server_status=',cfg['api_server'])
    api_status  = {}
    for api in cfg['api_server'].split(','):
        req = 'http://{}/health'.format(api)
        try:
          res = requests.head(req)
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

def get_config_from_db(tag,workdir):
    file_name = workdir + '/config/' + tag + '.json'
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
        values = {
            'tag': tag
        }
        url = 'http://$$API_SERVER$$/read_config_sync'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        try:
            req = urllib.request.Request(url, data=data)
            res = urllib.request.urlopen(req, context=context)
        except:
            api = query_health_api(cfg)
            url = 'http://{}/read_config_sync'.format(api)
            print('query_health_api=', url)
            req = urllib.request.Request(url, data=data)
            res = urllib.request.urlopen(req, context=context)

        res = json.loads(res.read())
        print(res, res['code'])
        if res['code'] == 200:
            print('接口调用成功!')

            try:
                tmp = get_local_config_json(file_name)
                if tmp.get('counter') is None or tmp.get('counter') > 0:
                    recover_running(tmp)
            except:
                pass

            try:
                print(res['msg'])
                config = res['msg']
                config['sync_time_type_name']   = get_sync_time_type_name(config['sync_time_type'])
                db_sour_ip                      = config['sync_db_sour'].split(':')[0]
                db_sour_port                    = config['sync_db_sour'].split(':')[1]
                db_sour_service                 = config['sync_db_sour'].split(':')[2]
                db_sour_user                    = config['sync_db_sour'].split(':')[3]
                db_sour_pass                    = aes_decrypt(cfg,config['sync_db_sour'].split(':')[4],db_sour_user)
                db_dest_ip                      = config['sync_db_dest'].split(':')[0]
                db_dest_port                    = config['sync_db_dest'].split(':')[1]
                db_dest_service                 = config['sync_db_dest'].split(':')[2]
                db_dest_user                    = config['sync_db_dest'].split(':')[3]
                db_dest_pass                    = aes_decrypt(cfg,config['sync_db_dest'].split(':')[4],db_dest_user)
                config['db_mysql_sour_ip']      = db_sour_ip
                config['db_mysql_sour_port']    = db_sour_port
                config['db_mysql_sour_service'] = db_sour_service
                config['db_mysql_sour_user']    = db_sour_user
                config['db_mysql_sour_pass']    = db_sour_pass
                config['db_mysql_desc_ip']      = db_dest_ip
                config['db_mysql_desc_port']    = db_dest_port
                config['db_mysql_desc_service'] = db_dest_service
                config['db_mysql_desc_user']    = db_dest_user
                config['db_mysql_desc_pass']    = db_dest_pass
                config['db_mysql_sour_string']  = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
                config['db_mysql_desc_string']  = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
                config['run_mode']              = 'remote'
                config['counter']               = 0
                config['api_status']            = cfg['api_status']

                write_local_config_file_json(config)

                config['db_mysql_sour']         = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
                config['db_mysql_sour2']        = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
                config['db_mysql_sour3']        = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
                config['db_mysql_desc']         = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
                config['db_mysql_desc3']        = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)

                return config
            except Exception as e:
                v_error = '从接口配置获取数据库连接对象时出现异常:{0}'.format(traceback.format_exc())
                print(v_error)
                exception_connect_db(config, v_error)
                return None
        else:
            print('DBAPI接口调用失败!,{0}'.format(res['msg']))  # 发异常邮件
            v_title = '数据同步接口异常[★]'
            v_content = '''<table class='xwtable'>
                                           <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                                           <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                                           <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr>            
                                       </table>'''
            v_content = v_content.replace('$$interface$$', url)
            v_content = v_content.replace('$$parameter$$', json.dumps(values))
            v_content = v_content.replace('$$error$$', res['msg'])
            if res['code'] != -3:
                exception_interface(v_title, v_content)
            else:
                print(res['msg'])
            return None

    except :
        file_name = workdir + '/config/' + tag + '.json'
        config    = get_config_json(file_name)
        if config['counter'] <= 3:
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
           v_content = v_content.replace('$$parameter$$',  json.dumps(values))
           v_content = v_content.replace('$$error$$', traceback.format_exc())
           v_content = v_content.replace('$$desc$$', v_desc)
           exception_interface(v_title, v_content)
        return config

def write_log(msg):
    file_name   = '/tmp/mysql_sync.log'
    file_handle = open(file_name, 'a+')
    file_handle.write(msg + '\n')
    file_handle.close()

def write_sync_log(config):
    v_tag = {
        'sync_tag'        : config['sync_tag'],
        'create_date'     : get_time(),
        'duration'        : config['sync_duration'],
        'amount'          : config['sync_amount']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    url = 'http://$$API_SERVER$$/write_sync_log'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    try:
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
    except:
        api = query_health_api(config)
        url = 'http://{}/write_sync_log'.format(api)
        print('query_health_api=', url)
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        write_log('Interface write_sync_log call successful!')
    else:
        write_log('Interface write_sync_log call failed!')

def write_sync_log_detail(config,ftab):
    v_tag = {
        'sync_tag'        : config['sync_tag'],
        'create_date'     : get_time(),
        'sync_table'      : config['sync_table_inteface'],
        'sync_amount'     : config['sync_amount'],
        'duration'        : config['sync_duration'],
        'db_name'         : config['db_mysql_sour_service'].lower(),
        'schema_name'     : '',
        'tab_name'        : ftab.split(':')[0].lower(),
        'sync_cols'       : get_sync_table_cols(config, ftab.split(':')[0]).replace('`','').replace(',','#').lower(),
        'sync_incr_col'   : ftab.split(':')[1].lower(),
        'sync_time'       : ftab.split(':')[2]
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('write_sync_log_detail=', values)
    url     = 'http://$$API_SERVER$$/write_sync_log_detail'
    context = ssl._create_unverified_context()
    data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    try:
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
    except:
        api = query_health_api(config)
        url = 'http://{}/write_sync_log_detail'.format(api)
        print('query_health_api=', url)
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
    res     = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        write_log('Interface write_sync_log_detail call successful!')
    else:
        write_log('Interface write_sync_log_detail call failed!')

def get_config(fname,tag):
    try:
        config = {}
        cfg=configparser.ConfigParser()
        cfg.read(fname,encoding="utf-8-sig")
        sync_server_sour                  = cfg.get("sync","sync_db_mysql_sour")
        sync_server_dest                  = cfg.get("sync","sync_db_mysql_desc")
        config['sync_table']              = cfg.get("sync", "sync_table").lower()
        config['batch_size']              = cfg.get("sync", "batch_size")
        config['batch_size_incr']         = cfg.get("sync", "batch_size_incr")
        config['sync_gap']                = cfg.get("sync", "sync_gap")
        config['sync_col_name']           = cfg.get("sync", "sync_col_name")
        config['sync_col_val']            = cfg.get("sync", "sync_col_val")
        config['sync_time_type']          = cfg.get("sync","sync_time_type")
        config['sync_time_type_name']     = get_sync_time_type_name(config['sync_time_type'])
        db_sour_ip                        = sync_server_sour.split(':')[0]
        db_sour_port                      = sync_server_sour.split(':')[1]
        db_sour_service                   = sync_server_sour.split(':')[2]
        db_sour_user                      = sync_server_sour.split(':')[3]
        db_sour_pass                      = sync_server_sour.split(':')[4]
        db_dest_ip                        = sync_server_dest.split(':')[0]
        db_dest_port                      = sync_server_dest.split(':')[1]
        db_dest_service                   = sync_server_dest.split(':')[2]
        db_dest_user                      = sync_server_dest.split(':')[3]
        db_dest_pass                      = sync_server_dest.split(':')[4]
        config['db_mysql_sour_ip']        = db_sour_ip
        config['db_mysql_sour_port']      = db_sour_port
        config['db_mysql_sour_service']   = db_sour_service
        config['db_mysql_sour_user']      = db_sour_user
        config['db_mysql_sour_pass']      = db_sour_pass
        config['db_mysql_desc_ip']        = db_dest_ip
        config['db_mysql_desc_port']      = db_dest_port
        config['db_mysql_desc_service']   = db_dest_service
        config['db_mysql_desc_user']      = db_dest_user
        config['db_mysql_desc_pass']      = db_dest_pass
        config['db_mysql_sour_string']    = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
        config['db_mysql_desc_string']    = db_dest_ip+':'+db_dest_port+'/'+db_dest_service
        config['db_mysql_sour']           = get_ds_mysql(db_sour_ip, db_sour_port ,db_sour_service, db_sour_user, db_sour_pass)
        config['db_mysql_sour2']          = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
        config['db_mysql_sour3']          = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
        config['db_mysql_desc']           = get_ds_mysql(db_dest_ip, db_dest_port ,db_dest_service, db_dest_user, db_dest_pass)
        config['db_mysql_desc3']          = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
        config['run_mode']                = 'local'
        config['sync_tag']                = tag
        return config
    except Exception as e:
        v_error = '从本地读取最近一次本地配置文件获取数据源时出现异常:{0}'.format(traceback.format_exc())
        print(v_error)
        exception_connect_db(config, v_error)
        exit(0)

def check_mysql_tab_exists(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   config['run_sql'] = sql
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_rows(config,tab):
   db=config['db_mysql_desc3']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab)
   config['run_sql'] = sql
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_incr_rows(config,tab):
   db=config['db_mysql_desc3']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab )
   config['run_sql'] = sql
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_sync(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tab)
   config['run_sql'] = sql
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_exists_pk(config,tab):
   db=config['db_mysql_sour']
   cr=db.cursor()
   sql = """select count(0) from information_schema.columns
              where table_schema=database() and table_name='{0}' and column_key='PRI'""".format(tab)
   config['run_sql'] = sql
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_tab_header_pkid(config,tab):
    db=config['db_mysql_sour']
    cr=db.cursor()
    cols=get_sync_table_cols_pkid(db,tab)
    sql="select {0} from {1} limit 1".format(cols,tab)
    config['run_sql'] = sql
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+cols+")"
    cr.close()
    return s1+s2

def get_tab_header(config,tab):
    db=config['db_mysql_sour']
    cr=db.cursor()
    sql="select * from {0} limit 1".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+get_sync_table_cols(db,tab)+")"
    cr.close()
    return s1+s2

def f_get_table_ddl(config,tab):
    db_source = config['db_mysql_sour']
    cr_source = db_source.cursor()
    v_sql     ="""show create table {0}""".format(tab)
    config['run_sql'] = v_sql
    cr_source.execute(v_sql)
    rs=cr_source.fetchone()
    return rs[1]

def sync_mysql_ddl(config,debug):
    try:
        db_source = config['db_mysql_sour']
        cr_source = db_source.cursor()
        db_desc   = config['db_mysql_desc']
        cr_desc   = db_desc.cursor()
        for i in config['sync_table'].split(","):
            tab=i.split(':')[0]
            sql = """SELECT table_schema,table_name 
                                  FROM information_schema.tables
                                 WHERE table_schema=database() and table_name='{0}' order by table_name
                              """.format(tab)
            config['run_sql'] = sql
            cr_source.execute(sql)
            rs_source = cr_source.fetchall()
            for j in range(len(rs_source)):
                tab_name = rs_source[j][1].lower()
                if check_mysql_tab_exists_pk(config,tab)==0:
                   print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_mysql_sour_string'],tab_name))
                   v_cre_sql = f_get_table_ddl(config, tab_name)
                   if check_mysql_tab_exists(config, tab_name) > 0:
                       print("DB:{0},Table :{1} already exists!".format(config['db_mysql_sour_string'],tab_name))
                   else:
                       config['run_sql'] = v_cre_sql
                       cr_desc.execute(v_cre_sql)
                       print("Table:{0} creating success!".format(tab_name))
                       v_pk_sql="""ALTER TABLE {0} ADD COLUMN pkid INT(11) NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (pkid)
                                """.format(tab_name)
                       print( "Table:{0} add primary key pkid success!".format(tab_name))
                       config['run_sql'] = v_pk_sql
                       cr_desc.execute(v_pk_sql)
                       db_desc.commit()
                else:
                   #编写函数完成生成创表语句
                   v_cre_sql = f_get_table_ddl(config,tab_name)
                   if check_mysql_tab_exists(config,tab_name)>0:
                       print("DB:{0},Table :{1} already exists!".format(config['db_mysql_desc_string'],tab_name))
                   else:
                      config['run_sql'] = v_cre_sql
                      cr_desc.execute(v_cre_sql)
                      print("Table:{0} creating success!".format(tab_name))
                      db_desc.commit()
        cr_source.close()
        cr_desc.close()
    except Exception as e:
        print('sync_mysql_ddl exceptiion:'+traceback.format_exc())
        exception_running(config,traceback.format_exc())
        exit(0)

def get_sync_table_total_rows(db,tab,v_where):
    cr_source = db.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_pk_names(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema=database() 
                and table_name='{0}' and column_key='PRI' order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_cols(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select concat('`',column_name,'`') from information_schema.columns
              where table_schema=database() and table_name='{0}'  order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

#获取非主键列以外的列名列表
def get_sync_table_cols_pkid(db,tab):
    return ','.join(get_sync_table_cols(db, tab).split(',')[1:])

def get_sync_table_pk_vals(db,tab):
    cr_source  = db.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CAST(" + i[0] + " as char)," + "\'^^^\'" + ","
    cr_source.close()
    return 'CONCAT('+v_col[0:-7]+')'

def get_sync_table_pk_vals2(db,tab):
    cr_source  = db.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col +  i[0] + ","
    cr_source.close()
    return v_col[0:-1]

def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def get_sync_incr_max_rq(db,tab):
    cr_desc = db.cursor()
    v_tab   = tab.split(':')[0]
    v_rq    = tab.split(':')[1]
    if v_rq=='':
       return ''
    v_sql   = "select max({0}) from {1}".format(v_rq,v_tab)
    cr_desc.execute(v_sql)
    rs=cr_desc.fetchone()
    db.commit()
    cr_desc.close()
    return rs[0]

def get_sync_where_incr_mysql(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = 'where {0} ={1}'.format(v_rq_col, v_expire_time)
    return v

def get_sync_where_incr_mysql_rq(tab,config,currq):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} DAY)".format(v_rq_col,currq, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} HOUR)".format(v_rq_col,currq, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} MINUTE)".format(v_rq_col,currq, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_md5(str):
    hash = hashlib.md5()
    hash.update(str.encode('utf-8'))
    return (hash.hexdigest())

def get_pk_vals_mysql(db,ftab,v_where):
    cr        = db.cursor()
    tab       = ftab.split(':')[0]
    v_pk_cols = get_sync_table_pk_vals(db, tab)
    v_sql     = "select {0} from {1} {2}".format(v_pk_cols, tab,v_where)
    cr.execute(v_sql)
    rs        = cr.fetchall()
    l_pk_vals = []
    for i in list(rs):
        l_pk_vals.append(i[0])
    cr.close()
    return l_pk_vals


def get_tjd_exception(config):
    db  = config['db_mysql_sour']
    cr  = db.cursor()
    st  = """select  val from hopsonone_park_tjd_real_time_exception.t_tjd_excep_meta 
              where  db='{0}' and tab='dbo_park_paiddetails' and col='carlogid' 
                 AND create_time >= DATE_SUB(NOW(),INTERVAL 3 DAY)
          """.format(config['db_mysql_sour_service'].replace('_repair',''))
    cr.execute(st)
    rs=cr.fetchall()
    if len(rs) == 0:
       print('Not found exception data!')
    else:
       print('get_tjd_exception=>', st)
    return list(rs)


def sync_mysql_data(config):
    # 遍历每一个异常值，生成数据同步表清单
    db = config['db_mysql_sour2']
    for v in get_tjd_exception(config):
        print('{}Process {}=>dbo_park_paiddetails.carlogid={}'.format('>'.ljust(10, '>'),config['db_mysql_sour_service'].replace('_repair',''),v[0]))
        start_time = datetime.datetime.now()
        amount=0
        for t in config['sync_table'].replace('$$val$$',v[0]).split(","):
            sync_mysql_data_no_pkid(config,t)
            if config['run_mode'] == 'remote':
               amount = amount + int(config['sync_amount'])

        # 更新停简单异常元数据表状态为已处理
        st_repair = """
update hopsonone_park_tjd_real_time_exception.t_tjd_excep_meta set flag='1',repair_time=now() where db='{}' and tab='dbo_park_paiddetails'  and col='carlogid'  and val='{}' 
        """.format(config['db_mysql_sour_service'].replace('_repair', ''), v[0])
        print('>>>>>>>>aliyun Repair tjd:{}.dbo_park_paiddetails[carlogid]={}'.format(config['db_mysql_sour_service'].replace('_repair', ''), v[0]))
        print(st_repair)
        cr = db.cursor()
        cr.execute(st_repair)
        db.commit()

        if config['run_mode'] == 'remote':
           config['sync_amount']   = str(amount)
           config['sync_duration'] = str(get_seconds(start_time))
           write_sync_log(config)


def sync_mysql_data_no_pkid(config, ftab):
    try:
        #start sync dml data
        config['sync_amount'] = 0
        i_counter        = 0
        tab              = ftab.split(':')[0]

        v_where          = get_sync_where_incr_mysql(ftab, config)
        ins_sql_header   = get_tab_header(config, tab)
        n_batch_size     = int(config['batch_size_incr'])
        db_source        = config['db_mysql_sour']
        db_source2       = config['db_mysql_sour2']
        cr_source        = db_source.cursor()
        db_desc          = config['db_mysql_desc']
        v_pk_names       = get_sync_table_pk_names(db_source, tab)
        v_pk_cols        = get_sync_table_pk_vals(db_source, tab)
        cr_desc          = db_desc.cursor()
        n_tab_total_rows = get_sync_table_total_rows(db_source, tab, v_where)
        v_sql            = """select {0} as 'pk',{1} from {2} {3}""".format(v_pk_cols, get_sync_table_cols(db_source, tab), tab, v_where)
        n_rows           = 0
        cr_source.execute(v_sql)
        rs_source  = cr_source.fetchmany(n_batch_size)
        start_time = datetime.datetime.now()

        if ftab.split(':')[1] == '':
            print( "Sync Table increment :{0} ...".format(ftab.split(':')[0]))
        else:
            print( "Sync Table increment :{0} for carlogid={1}...".format(ftab.split(':')[0],ftab.split(':')[2]))

        while rs_source:
            batch_sql = ""
            batch_sql_del=""
            v_sql = ''
            v_sql_del = ''
            for r in list(rs_source):
                rs_source_desc = cr_source.description
                ins_val = ""
                for j in range(1,len(r)):
                    col_type = str(rs_source_desc[j][1])
                    if r[j] is None:
                        ins_val = ins_val + "null,"
                    elif col_type == '253':  # varchar,date
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                    elif col_type in ('1', '3', '8', '246'):  # int,decimal
                        ins_val = ins_val + "'" + str(r[j]) + "',"
                    elif col_type == '12':  # datetime
                        ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                    else:
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0])+ ","
            batch_sql = ins_sql_header + v_sql[0:-1]

            if ftab.split(':')[1] == '':
                config['run_sql'] =batch_sql
                cr_desc.execute(batch_sql)
            else:
                for d in v_sql_del[0:-1].split(','):
                    config['run_sql'] = 'delete from {0} where {1}'.format(tab, d)
                    cr_desc.execute('delete from {0} where {1}'.format(tab, d))
                config['run_sql'] = batch_sql
                cr_desc.execute(batch_sql)
            i_counter = i_counter + len(rs_source)


            print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                  .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
            if n_tab_total_rows == 0:
                print( "\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                      .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2)), end='')
            else:
                print( "\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                       .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
            rs_source = cr_source.fetchmany(n_batch_size)
        print('')

        db_desc.commit()

        if config['run_mode'] == 'remote':
           config['sync_duration'] = str(get_seconds(start_time))
           config['sync_table_inteface'] = tab
           config['sync_amount'] = str(i_counter)
           write_sync_log_detail(config)
    except Exception as e:
        print('sync_sqlserver_data_pk exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)


def write_local_config_file(config):
    sync_db_sour=config['db_mysql_sour_ip']+':'+\
                 config['db_mysql_sour_port']+':'+\
                 config['db_mysql_sour_service']+':'+\
                 config['db_mysql_sour_user']+':'+\
                 config['db_mysql_sour_pass']

    sync_db_dest=config['db_mysql_desc_ip']+':'+\
                 config['db_mysql_desc_port']+':'+\
                 config['db_mysql_desc_service']+':'+\
                 config['db_mysql_desc_user']+':'+\
                 config['db_mysql_desc_pass']

    file_name   = config['script_path']+'/config/'+config['sync_tag']+'.ini'
    file_handle = open(file_name, 'w')
    file_handle.write('[sync]' + '\n')
    file_handle.write('sync_db_mysql_sour={0}\n'.format(sync_db_sour))
    file_handle.write('sync_db_mysql_desc={0}\n'.format(sync_db_dest))
    file_handle.write('sync_table={0}\n'.format(config['sync_table']))
    file_handle.write('sync_col_name={0}\n'.format(config['sync_col_name']))
    file_handle.write('sync_col_val={0}\n'.format(config['sync_col_val']))
    file_handle.write('batch_size={0}\n'.format(config['batch_size']))
    file_handle.write('batch_size_incr={0}\n'.format(config['batch_size_incr']))
    file_handle.write('sync_time_type={0}\n'.format(config['sync_time_type']))
    file_handle.write('sync_gap={0}\n'.format(config['sync_gap']))
    file_handle.close()
    print("{0} export complete!".format(file_name))


def init(config,debug,workdir):
    config = get_config_from_db(config,workdir)

    if config is None:
       exit(0)

    if debug:
       print_dict(config)
    return config

def exception_connect_db(config,p_error):
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
    v_title   = config.get('comments')+'数据同步数据库异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$sync_tag$$',   config.get('sync_tag'))
    v_content = v_content.replace('$$sync_ywlx$$' , config.get('sync_ywlx_name'))
    v_content = v_content.replace('$$sync_type$$' , config.get('sync_type_name'))
    v_content = v_content.replace('$$server_id$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$sync_db_sour$$', config.get('sync_db_sour'))
    v_content = v_content.replace('$$sync_db_dest$$', config.get('sync_db_dest'))
    v_content = v_content.replace('$$sync_table$$', config.get('sync_table'))
    v_content = v_content.replace('$$sync_time_type$$', config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_time$$', config.get('run_time'))
    v_content = v_content.replace('$$run_error$$', str(p_error))
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

def exception_interface(v_title,v_content):
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
    v_templete = v_templete.replace('$$TABLE$$',v_content)
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_templete)

def exception_running(config,p_error):
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

    v_title   = config.get('comments')+'数据同步运行异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$'    ,config.get('comments'))
    v_content = v_content.replace('$$sync_tag$$'      ,config.get('sync_tag'))
    v_content = v_content.replace('$$sync_ywlx$$'     ,config.get('sync_ywlx_name'))
    v_content = v_content.replace('$$sync_type$$'     ,config.get('sync_type_name'))
    v_content = v_content.replace('$$server_id$$'     ,str(config.get('server_desc')))
    v_content = v_content.replace('$$sync_db_sour$$'  ,config.get('sync_db_sour'))
    v_content = v_content.replace('$$sync_db_dest$$'  ,config.get('sync_db_dest'))
    v_content = v_content.replace('$$sync_table$$'    ,config.get('sync_table'))
    v_content = v_content.replace('$$sync_time_type$$',config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$'  , config.get('script_file'))
    v_content = v_content.replace('$$run_time$$'      ,config.get('run_time'))
    v_content = v_content.replace('$$run_sql$$'       ,config.get('run_sql'))
    v_content = v_content.replace('$$run_error$$'     ,str(p_error))

    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

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
    v_title   = config.get('comments')+'任务恢复为远程模式[★]'
    v_content = v_templete.replace('$$task_desc$$'    ,config.get('comments'))
    v_content = v_content.replace('$$sync_tag$$'      ,config.get('sync_tag'))
    v_content = v_content.replace('$$sync_ywlx$$'     ,config.get('sync_ywlx_name'))
    v_content = v_content.replace('$$sync_type$$'     ,config.get('sync_type_name'))
    v_content = v_content.replace('$$server_id$$'     ,str(config.get('server_desc')))
    v_content = v_content.replace('$$sync_db_sour$$'  ,config.get('sync_db_sour'))
    v_content = v_content.replace('$$sync_db_dest$$'  ,config.get('sync_db_dest'))
    v_content = v_content.replace('$$sync_table$$'    ,config.get('sync_table').replace(',','<br>'))
    v_content = v_content.replace('$$sync_time_type$$',config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$'  , config.get('script_file'))
    v_content = v_content.replace('$$run_time$$'      ,config.get('run_time'))
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

def sync(config,debug):

    #sync table ddl
    sync_mysql_ddl(config, debug)

    #sync data
    while True:
      #sync increment data
      sync_mysql_data(config)

      sys.exit(0)

def main():
    #init variable
    config = ""
    debug = False
    check = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-workdir":
            workdir = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #初始化
    config=init(config,debug,workdir)

    #数据同步
    sync(config, debug)

if __name__ == "__main__":
     main()
