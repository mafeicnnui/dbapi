#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : ma.fei
# @File : sync_mysql2mongo.py
# @Func : MySQL->MySQL increment sync
# @Software: PyCharm

import sys
import traceback
import configparser
import warnings
import psycopg2
from psycopg2 import extras
import pymysql
import datetime
import smtplib
from email.mime.text import MIMEText
import json
import requests
import os
import socket


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


def socket_port(ip, port):
    try:
        socket.setdefaulttimeout(1)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex((ip, port))
        if result == 0:
            return True
        else:
            return False
    except:
        return False


def get_available_port():
    for p in [465, 25]:
        if socket_port("smtp.exmail.qq.com", p):
            return p


def send_mail(p_from_user, p_from_pass, p_to_user, p_title, p_content):
    to_user = p_to_user.split(",")
    try:
        msg = MIMEText(p_content, 'html', 'utf-8')
        msg["Subject"] = p_title
        msg["From"] = p_from_user
        msg["To"] = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)


def send_mail25(p_sendserver, p_from_user, p_from_pass, p_to_user, p_title, p_content):
    to_user = p_to_user.split(",")
    try:
        msg = MIMEText(p_content, 'html', 'utf-8')
        msg["Subject"] = p_title
        msg["From"] = p_from_user
        msg["To"] = ",".join(to_user)
        server = smtplib.SMTP(p_sendserver, 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)


def send_mail465(p_sendserver, p_from_user, p_from_pass, p_to_user, p_title, p_content):
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
        server.quit()
    except smtplib.SMTPException as e:
        print(e)


def send_mail_param(p_sendserver, p_from_user, p_from_pass, p_to_user, p_title, p_content):
    try:
        port = get_available_port()
        if port == 465:
            print('send_mail465')
            send_mail465(p_sendserver, p_from_user, p_from_pass, p_to_user, p_title, p_content)
        else:
            print('send_mail25')
            send_mail25(p_sendserver, p_from_user, p_from_pass, p_to_user, p_title, p_content)
        print('send_mail_param send success!')
    except:
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


def get_ds_mysql(ip, port, service, user, password):
    print(ip, port, service, user, password)
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8mb4')
    return conn


def get_ds_pg(ip, port, service, user, password):
    conn = psycopg2.connect(database=service, user=user, password=password, host=ip, port=port,
                            cursor_factory=psycopg2.extras.DictCursor)
    return conn


def get_db_mysql_sour(config):
    return get_ds_mysql(config['db_mysql_sour_ip'], config['db_mysql_sour_port'], config['db_mysql_sour_service'], \
                        config['db_mysql_sour_user'], config['db_mysql_sour_pass'])


def get_db_mysql_desc(config):
    return get_ds_mysql(config['db_mysql_desc_ip'], config['db_mysql_desc_port'], config['db_mysql_desc_service'], \
                        config['db_mysql_desc_user'], config['db_mysql_desc_pass'])


def get_sync_time_type_name(sync_time_type):
    if sync_time_type == "day":
        return '天'
    elif sync_time_type == "hour":
        return '小时'
    elif sync_time_type == "min":
        return '分'
    else:
        return ''


def aes_decrypt(p_cfg, p_password, p_key):
    par = {'password': p_password, 'key': p_key}
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par, timeout=1).json()
        if res['code'] == 200:
            print('接口read_db_decrypt 调用成功!')
            config = res['msg']
            return config
        else:
            print('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        try:
            url = 'http://{}/read_db_decrypt'.format(query_health_api(p_cfg))
            res = requests.post(url, data=par, timeout=1).json()
            if res['code'] == 200:
                print('Api read_db_decrypt call success!')
                config = res['msg']
                return config
            else:
                print('Api read_db_decrypt call failure! {0}'.format(res['msg']))
        except:
            print('aes_decrypt api not available!')


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
    cfg['counter'] = cfg['counter'] + 1
    write_local_config_file_json(cfg)

    cfg['db_mysql_sour'] = get_ds_mysql(cfg['db_mysql_sour_ip'],
                                        cfg['db_mysql_sour_port'],
                                        cfg['db_mysql_sour_service'],
                                        cfg['db_mysql_sour_user'],
                                        cfg['db_mysql_sour_pass'])

    cfg['db_mysql_desc'] = get_ds_mysql(cfg['db_mysql_desc_ip'],
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
    print('check_api_server_status=', cfg['api_server'])
    api_status = {}
    for api in cfg['api_server'].split(','):
        req = 'http://{}/health'.format(api)
        try:
            res = requests.head(req, timeout=1)
            api_status[api] = res.status_code
        except:
            api_status[api] = 500
    cfg['api_status'] = api_status
    print(cfg['api_status'])
    return cfg


def query_health_api(cfg):
    apis = cfg['api_status']
    for key in apis:
        if apis[key] == 200:
            return key


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
            res = requests.post(url, data=par, timeout=1).json()
        except:
            url = 'http://{}/read_config_sync'.format(query_health_api(cfg))
            res = requests.post(url, data=par, timeout=1).json()

        if res['code'] == 200:
            print('read_config_sync 接口调用成功!')
            config = res['msg']
            try:
                tmp = get_local_config_json(file_name)
                if tmp.get('counter') is None or tmp.get('counter') > 0:
                    recover_running(tmp)
            except:
                pass

            try:
                config['sync_time_type_name'] = get_sync_time_type_name(config['sync_time_type'])
                db_sour_ip = config['sync_db_sour'].split(':')[0]
                db_sour_port = config['sync_db_sour'].split(':')[1]
                db_sour_service = config['sync_db_sour'].split(':')[2]
                db_sour_user = config['sync_db_sour'].split(':')[3]
                db_sour_pass = aes_decrypt(cfg, config['sync_db_sour'].split(':')[4], db_sour_user)
                db_dest_ip = config['sync_db_dest'].split(':')[0]
                db_dest_port = config['sync_db_dest'].split(':')[1]
                db_dest_service = config['sync_db_dest'].split(':')[2]
                db_dest_user = config['sync_db_dest'].split(':')[3]
                db_dest_pass = aes_decrypt(cfg, config['sync_db_dest'].split(':')[4], db_dest_user)

                config['db_pg_sour_ip'] = db_sour_ip
                config['db_pg_sour_port'] = db_sour_port
                config['db_pg_sour_service'] = db_sour_service
                config['db_pg_sour_user'] = db_sour_user
                config['db_pg_sour_pass'] = db_sour_pass

                config['db_mysql_desc_ip'] = db_dest_ip
                config['db_mysql_desc_port'] = db_dest_port
                config['db_mysql_desc_service'] = db_dest_service
                config['db_mysql_desc_user'] = db_dest_user
                config['db_mysql_desc_pass'] = db_dest_pass

                config['db_pg_sour_string'] = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
                config['db_mysql_desc_string'] = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
                config['run_mode'] = 'remote'
                config['counter'] = 0
                config['api_status'] = cfg['api_status']

                write_local_config_file_json(config)

                config['db_pg_sour'] = get_ds_pg(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
                config['db_mysql_desc'] = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user,
                                                       db_dest_pass)
                config['db_mysql_desc2'] = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user,
                                                        db_dest_pass)
                return config
            except Exception as e:
                v_error = '从接口配置获取数据库连接对象时出现异常:{0}'.format(traceback.format_exc())
                print(v_error)
                exception_connect_db(config, v_error)
                return None
        else:
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
                exception_interface(cfg, v_title, v_content)
            else:
                print(res['msg'])
            return None

    except:
        file_name = workdir + '/config/' + tag + '.json'
        config = get_config_json(file_name)
        if config['counter'] <= 3:
            print('接口调用失败,从本地读取最近一次配置文件：{0}'.format(file_name))
            v_title = '{}任务切换为本地模式[★★]'.format(config['comments'])
            v_desc = '同步任务运行于本地配置文件模式中，请尽快处理! 自动从本地读取最近一次配置文件进行同步：{0}'.format(
                file_name)
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
            exception_interface(cfg, v_title, v_content)
        return config


def write_sync_log(config):
    par = {
        'sync_tag': config['sync_tag'],
        'create_date': get_time(),
        'duration': config['sync_duration'],
        'amount': config['sync_amount']
    }
    try:
        url = 'http://$$API_SERVER$$/write_sync_log'
        res = requests.post(url, data={'tag': json.dumps(par)}, timeout=1)
        if res.status_code == 200:
            print('Interface write_sync_log call successful!')
        else:
            print('Interface write_sync_log call failed!')
    except:
        try:
            api = query_health_api(config)
            url = 'http://{}/write_sync_log'.format(api)
            res = requests.post(url, data={'tag': json.dumps(par)}, timeout=1)
            if res.status_code == 200:
                print('Interface write_sync_log call successful!')
            else:
                print('Interface write_sync_log call failed!')
        except:
            print('Interface Api  not available,skip write_sync_log!')


def write_sync_log_detail(config, ftab):
    schema, tab = ftab.split(':')[0].split('.')
    par = {
        'sync_tag': config['sync_tag'],
        'create_date': get_time(),
        'sync_table': config['sync_table_inteface'],
        'sync_amount': config['sync_amount'],
        'duration': config['sync_duration'],
        'db_name': config['db_mysql_desc_service'].lower(),
        'schema_name': '',
        'tab_name': ftab.split(':')[0].lower(),
        'sync_cols': get_sync_table_cols(config, schema, tab).replace(',', '#'),
        'sync_incr_col': ftab.split(':')[1].lower(),
        'sync_time': ftab.split(':')[2]
    }

    try:
        url = 'http://$$API_SERVER$$/write_sync_log_detail'
        res = requests.post(url, data={'tag': json.dumps(par)}, timeout=1)
        if res.status_code == 200:
            print('Interface write_sync_log_detail call successful!')
        else:
            print('Interface write_sync_log_detail call failed!')
    except:
        try:
            api = query_health_api(config)
            url = 'http://{}/write_sync_log_detail'.format(api)
            res = requests.post(url, data={'tag': json.dumps(par)}, timeout=1)
            if res.status_code == 200:
                print('Interface write_sync_log_detail call successful!')
            else:
                print('Interface write_sync_log_detail call failed!')
        except:
            print('Interface Api  not available,skip write_sync_log_detail!')


def update_sync_status(config, status):
    data = {
        'tag': config['sync_tag'],
        'status': status
    }
    try:
        url = 'http://$$API_SERVER$$/update_sync_status'
        res = requests.post(url, data=data, timeout=1)
        if res.status_code == 200:
            print('call interface update_sync_status :{}!'.format('running' if status == '1' else 'complete'))
        else:
            print('call interface update_sync_status error :{}'.format(res['msg']))
    except:
        try:
            api = query_health_api(config)
            url = 'http://{}/update_sync_status'.format(api)
            res = requests.post(url, data=data, timeout=1).json()
            if res.status_code == 200:
                print('call interface update_sync_status :{}!'.format('running' if status == '1' else 'complete'))
            else:
                print('call interface update_sync_status error :{}'.format(res['msg']))
        except:
            print('Interface API not available,skip update_sync_status!')


def get_config(fname, tag):
    try:
        config = {}
        cfg = configparser.ConfigParser()
        cfg.read(fname, encoding="utf-8-sig")
        sync_server_sour = cfg.get("sync", "sync_db_mysql_sour")
        sync_server_dest = cfg.get("sync", "sync_db_mysql_desc")
        config['sync_table'] = cfg.get("sync", "sync_table").lower()
        config['batch_size'] = cfg.get("sync", "batch_size")
        config['batch_size_incr'] = cfg.get("sync", "batch_size_incr")
        config['sync_gap'] = cfg.get("sync", "sync_gap")
        config['sync_col_name'] = cfg.get("sync", "sync_col_name")
        config['sync_col_val'] = cfg.get("sync", "sync_col_val")
        config['sync_time_type'] = cfg.get("sync", "sync_time_type")
        config['sync_time_type_name'] = get_sync_time_type_name(config['sync_time_type'])
        db_sour_ip = sync_server_sour.split(':')[0]
        db_sour_port = sync_server_sour.split(':')[1]
        db_sour_service = sync_server_sour.split(':')[2]
        db_sour_user = sync_server_sour.split(':')[3]
        db_sour_pass = sync_server_sour.split(':')[4]
        db_dest_ip = sync_server_dest.split(':')[0]
        db_dest_port = sync_server_dest.split(':')[1]
        db_dest_service = sync_server_dest.split(':')[2]
        db_dest_user = sync_server_dest.split(':')[3]
        db_dest_pass = sync_server_dest.split(':')[4]
        config['db_mysql_sour_ip'] = db_sour_ip
        config['db_mysql_sour_port'] = db_sour_port
        config['db_mysql_sour_service'] = db_sour_service
        config['db_mysql_sour_user'] = db_sour_user
        config['db_mysql_sour_pass'] = db_sour_pass
        config['db_mysql_desc_ip'] = db_dest_ip
        config['db_mysql_desc_port'] = db_dest_port
        config['db_mysql_desc_service'] = db_dest_service
        config['db_mysql_desc_user'] = db_dest_user
        config['db_mysql_desc_pass'] = db_dest_pass
        config['db_mysql_sour_string'] = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
        config['db_mysql_desc_string'] = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
        config['db_mysql_sour'] = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
        config['db_mysql_desc'] = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
        config['db_mysql_desc3'] = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
        config['run_mode'] = 'local'
        config['sync_tag'] = tag
        return config
    except Exception as e:
        v_error = '从本地读取最近一次本地配置文件获取数据源时出现异常!'
        traceback.print_exc()
        exception_connect_db(config, v_error)
        exit(0)


def get_tab_header_pkid(config, tab):
    db = config['db_mysql_sour']
    cr = db.cursor()
    cols = get_sync_table_cols_pkid(config, tab)
    sql = "select {0} from `{1}` limit 1".format(cols, tab)
    config['run_sql'] = sql
    cr.execute(sql)
    desc = cr.description
    s1 = "insert into `" + tab.lower() + "` ("
    s2 = " values "
    s1 = s1 + cols + ',' + config['sync_col_name'] + ")"
    cr.close()
    return s1 + s2


def get_tab_header(config, schema, tab):
    s1 = "insert into `" + tab.lower() + "` ("
    s2 = " values "
    s1 = s1 + get_sync_table_cols(config, schema, tab) + ',' + config['sync_col_name'] + ")"
    return s1 + s2


def check_mysql_tab_exists(config, tab):
    db = config['db_mysql_desc']
    cr = db.cursor()
    sql = """select count(0) from information_schema.tables
            where table_schema='{0}' and table_name='{1}'""".format(config['db_mysql_desc_service'], tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def check_pg_tab_exists(config, schema, tab):
    db = config['db_pg_sour']
    cr = db.cursor()
    sql = """select count(0) from pg_tables  where schemaname='{}' and tablename='{}'""".format(schema, tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def get_mysql_tab_rows(config, tab):
    db = config['db_mysql_desc3']
    cr = db.cursor()
    sql = """select count(0) from {0}""".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def get_mysql_tab_incr_rows(config, tab):
    db = config['db_mysql_desc3']
    cr = db.cursor()
    sql = """select count(0) from {0}""".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def check_mysql_tab_sync(config, tab):
    db = config['db_mysql_desc']
    cr = db.cursor()
    sql = "select count(0) from `{0}`".format(tab)
    config['run_sql'] = sql
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def check_pg_tab_exists_pk(db, schema, tab):
    cr = db.cursor()
    st = """select count(0) from  pg_constraint 
             where  conrelid in(select oid from  pg_class where relname='{}') 
	          and connamespace=(select oid from pg_namespace where nspname='{}') and  contype='p' """.format(tab,
                                                                                                             schema)
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def check_pg_tab_exists_uk(db, schema, tab):
    cr = db.cursor()
    st = """select count(0) from  pg_constraint 
             where  conrelid in(select oid from  pg_class where relname='{}') 
	          and connamespace=(select oid from pg_namespace where nspname='{}') and  contype='u' """.format(tab,
                                                                                                             schema)
    cr.execute(st)
    rs = cr.fetchone()
    cr.close()
    db.commit()
    return rs[0]


def f_get_table_cons_cols(db, schema, tab, cids):
    cr = db.cursor()
    st = """select column_name 
             from  information_schema.columns 
               where table_schema='{}' and table_name='{}' 
                and ordinal_position in ({}) order by ordinal_position
            """.format(schema, tab, ','.join([str(i) for i in cids]))
    v_col = ''
    cr.execute(st)
    rs = cr.fetchall()
    for i in range(len(rs)):
        v_col = v_col + '{},'.format(rs[i][0])
    return v_col[0:-1]


def f_get_table_cons(db, schema, tab):
    cr = db.cursor()
    st = """select  conname,contype,conkey from pg_constraint 
             where  conrelid in(select oid from  pg_class where relname='{}' ) 
	             and connamespace = (select oid from pg_namespace where nspname='{}') 
        """.format(tab, schema)
    v_con = ''
    cr.execute(st)
    rs = cr.fetchall()
    for i in range(len(rs)):
        if rs[i][1] == 'u':
            v_con = ' UNIQUE KEY `{}` ({}),'.format(rs[i][0], f_get_table_cons_cols(db, schema, tab, rs[i][2]))
        elif rs[i][1] == 'p':
            v_con = ' PRIMARY KEY ({}),'.format(f_get_table_cons_cols(db, schema, tab, rs[i][2]))
    return v_con[0:-1]


def f_get_table_cons_pk_names(db, schema, tab):
    cr = db.cursor()
    st = """select  conkey from pg_constraint 
             where  conrelid in(select oid from  pg_class where relname='{}' ) 
	             and connamespace = (select oid from pg_namespace where nspname='{}')  and contype='p'
        """.format(tab, schema)
    cr.execute(st)
    rs = cr.fetchone()
    col = f_get_table_cons_cols(db, schema, tab, rs[0])
    return col


def f_get_table_cons_uk_names(db, schema, tab):
    cr = db.cursor()
    st = """select  conkey from pg_constraint 
             where  conrelid in(select oid from  pg_class where relname='{}' ) 
	             and connamespace = (select oid from pg_namespace where nspname='{}')  and contype='u' limit 1
        """.format(tab, schema)
    cr.execute(st)
    rs = cr.fetchone()
    col = f_get_table_cons_cols(db, schema, tab, rs[0])
    return col


def f_get_table_ddl(config, schema, tab):
    db = config['db_pg_sour']
    cr = db.cursor()
    st = """select column_name,data_type,is_nullable 
                 from  information_schema.columns a
                 where table_schema='{}' and table_name='{}'
                  order by a.ordinal_position""".format(schema, tab)
    config['run_sql'] = st
    cr.execute(st)
    rs = cr.fetchall()
    v_cre_tab = 'create table ' + tab + '(\n'
    for i in range(len(rs)):
        v_name = rs[i][0]
        v_type = rs[i][1]
        if v_type in ('integer'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    int,\n'
        elif v_type in ('real'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    numeric(20,2) ,\n'
        elif v_type in ('date'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    date ,\n'
        elif v_type in ('timestamp without time zone'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    datetime ,\n'
        elif v_type in ('text'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    varchar(100) ,\n'
        elif v_type in ('uuid'):
            v_cre_tab = v_cre_tab + '   ' + v_name + '    varchar(50) ,\n'
        else:
            v_cre_tab = v_cre_tab + '   ' + v_name + '    varchar(500) ,\n'
    v_cre_tab = v_cre_tab + f_get_table_cons(db, schema, tab)
    return v_cre_tab + ')'


def sync_mysql_ddl(config, debug):
    try:
        db_source = config['db_pg_sour']
        cr_source = db_source.cursor()
        db_desc = config['db_mysql_desc']
        cr_desc = db_desc.cursor()
        for i in config['sync_table'].split(","):
            schema, tab = i.split(':')[0].split('.')
            sql = """select schemaname as  table_schema,
                             tablename  as  table_name 
                          from pg_tables where schemaname='{}' and tablename='{}' order by table_name
                  """.format(schema, tab)
            config['run_sql'] = sql
            cr_source.execute(sql)
            rs_source = cr_source.fetchall()
            for j in range(len(rs_source)):
                tab_name = rs_source[j][1].lower()
                if check_pg_tab_exists_pk(db_source, schema, tab) == 0:
                    print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_pg_sour_string'], tab_name))
                    v_cre_sql = f_get_table_ddl(config, schema, tab_name)
                    if check_mysql_tab_exists(config, tab_name) > 0:
                        print("DB:{0},Table :{1} already exists!".format(config['db_mysql_desc_string'], tab_name))
                    else:
                        config['run_sql'] = v_cre_sql
                        cr_desc.execute(v_cre_sql)
                        print("Table:{0} creating success!".format(tab_name))

                        for ac in config['sync_col_name'].split(','):
                            config['run_sql'] = 'alter table {0} add {1} varchar(50)'.format(tab_name, ac)
                            cr_desc.execute('alter table {0} add {1} varchar(50)'.format(tab_name, ac))
                            print("Table:{0} add column {1} success!".format(tab_name, ac))

                        v_pk_sql = """ALTER TABLE {0} ADD COLUMN pkid INT(11) NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (pkid)
                                                       """.format(tab_name)
                        cr_desc.execute(v_pk_sql)
                        print("Table:{0} add primary key pkid success!".format(tab_name))
                        db_desc.commit()
                else:
                    v_cre_sql = f_get_table_ddl(config, schema, tab_name)
                    if check_mysql_tab_exists(config, tab_name) > 0:
                        print("DB:{0},Table :{1} already exists!".format(config['db_mysql_desc_string'], tab_name))
                    else:
                        config['run_sql'] = v_cre_sql
                        cr_desc.execute(v_cre_sql)
                        print("Table:{0} creating success!".format(tab_name))

                        for ac in config['sync_col_name'].split(','):
                            config['run_sql'] = 'alter table {0} add {1} varchar(50)'.format(tab_name, ac)
                            cr_desc.execute('alter table {0} add {1} varchar(50)'.format(tab_name, ac))
                            print("Table:{0} add column {1} success!".format(tab_name, ac))
                        db_desc.commit()

        cr_source.close()
        cr_desc.close()
    except Exception as e:
        print('sync_mysql_ddl exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)


def get_sync_table_total_rows(db, schema, tab, v_where):
    cr_source = db.cursor()
    v_sql = """select count(0) from {}.{} {}""".format(schema, tab, v_where)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchone()
    db.commit()
    cr_source.close()
    return rs_source[0]


def get_sync_table_pk_names(db, schema, tab):
    if check_pg_tab_exists_pk(db, schema, tab) == 0:
        if check_pg_tab_exists_uk(db, schema, tab) == 0:
            return None
        else:
            return f_get_table_cons_uk_names(db, schema, tab)
    else:
        return f_get_table_cons_pk_names(db, schema, tab)


def get_sync_table_pk_vals(db, schema, tab):
    pk_cols = get_sync_table_pk_names(db, schema, tab)
    v_col = ''
    for col in pk_cols.split(','):
        v_col = v_col + "CAST(" + col + " as varchar)," + "\'^^^\'" + ","
    return 'CONCAT(' + v_col[0:-7] + ')'


def get_sync_cols(config, tab):
    for dic in config['cols']:
        if dic['sync_tag'] == config['sync_tag'] \
                and dic['db_name'] == config['db_mysql_desc_service'].lower() \
                and dic['tab_name'] == tab.split(':')[0].lower():
            return dic['sync_cols'].split('#')
    return None


def get_sync_table_cols(config, schema, tab):
    db = config['db_pg_sour']
    cr_source = db.cursor()
    v_col = ''
    v_sql = """select concat(column_name) from information_schema.columns
              where table_schema='{}' and table_name='{}'  order by ordinal_position
          """.format(schema, tab)
    v_sync_cols = get_sync_cols(config, tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        if v_sync_cols is None:
            v_col = v_col + i[0] + ','
        else:
            if i[0].lower().replace('`', '') in v_sync_cols:
                v_col = v_col + i[0] + ','
    db.commit()
    cr_source.close()
    return v_col[0:-1]


def get_sync_table_cols_pkid(config, tab):
    '''
    :param config:
    :param tab:
    :return: 非主键列以外的列名列表
    '''
    return ','.join(get_sync_table_cols(config, tab).split(',')[1:])


def get_sync_where(pk_cols, pk_vals):
    v_where = ''
    for i in range(len(pk_cols.split(','))):
        v_where = v_where + pk_cols.split(',')[i] + "='" + pk_vals.split('^^^')[i] + "' and "
    return v_where[0:-4]


def get_sync_where_incr_pg(tab, config):
    v_rq_col = tab.split(':')[1]
    v_expire_time = tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {} >= CURRENT_TIMESTAMP + INTERVAL '8 HOUR'- INTERVAL '{} DAY'".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {} >= CURRENT_TIMESTAMP + INTERVAL '8 HOUR'- INTERVAL '{} HOUR'".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {} >= CURRENT_TIMESTAMP + INTERVAL '8 HOUR'- INTERVAL '{} MINUTE'".format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v


def get_sync_where_incr_pg_rq(tab, config, currq):
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


def get_pk_vals_mysql(db, ftab, v_where):
    cr = db.cursor()
    schema, tab = ftab.split(':')[0].split('.')
    v_pk_cols = get_sync_table_pk_vals(db, schema, tab)
    v_sql = "select {0} from {1} {2}".format(v_pk_cols, tab, v_where)
    print('v_pk_cols=', v_pk_cols)
    cr.execute(v_sql)
    rs = cr.fetchall()
    l_pk_vals = []
    for i in list(rs):
        l_pk_vals.append(i[0])
    cr.close()
    return l_pk_vals


def sync_mysql_init(config, debug):
    try:
        config_init = {}
        for i in config['sync_table'].split(","):
            schema, tab = i.split(':')[0].split('.')
            config_init[tab] = False
            if (check_mysql_tab_exists(config, tab) == 0 \
                    or (check_mysql_tab_exists(config, tab) > 0 and check_mysql_tab_sync(config, tab) == 0)):

                config_init[tab] = True
                i_counter = 0
                ins_sql_header = get_tab_header(config, schema, tab)
                n_batch_size = int(config['batch_size'])
                db_source = config['db_pg_sour']
                db_desc = config['db_mysql_desc']
                v_pk_names = get_sync_table_pk_names(db_source, schema, tab)
                v_pk_cols = get_sync_table_pk_vals(db_source, schema, tab)
                cr_source = db_source.cursor()
                cr_desc = db_desc.cursor()

                print('delete table:{0} all data!'.format(tab))
                cr_desc.execute('delete from `{0}`'.format(tab))
                print('delete table:{0} all data ok!'.format(tab))

                n_tab_total_rows = get_sync_table_total_rows(db_source, schema, tab, '')
                v_sql = "select  {} as pk,{} from {}.{}".format(v_pk_cols, get_sync_table_cols(config, schema, tab),
                                                                schema, tab)
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
                            elif col_type in ('23', '700'):  # int,decimal
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == '1114':  # datetime
                                ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                            else:
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                        v_sql = v_sql + '(' + ins_val + config['sync_col_val'].replace('$PK$',
                                                                                       r[0].replace('^^^', '_')) + '),'

                    batch_sql = ins_sql_header + v_sql[0:-1]

                    #noinspection PyBroadException
                    config['run_sql'] = batch_sql
                    cr_desc.execute(batch_sql)
                    i_counter = i_counter + len(rs_source)

                    print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                          format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)),
                          end='')
                    if n_tab_total_rows == 0:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2)), end='')
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)),
                              end='')

                    rs_source = cr_source.fetchmany(n_batch_size)
                db_desc.commit()
                print('')
        return config_init
    except Exception as e:
        print('sync_mysql_init exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)


def sync_mysql_data(config, config_init):
    start_time = datetime.datetime.now()
    db = config['db_pg_sour']
    amount = 0
    for v in config['sync_table'].split(","):
        schema, tab = v.split(':')[0].split('.')
        if get_sync_table_pk_names(db, schema, tab) is not None:
            sync_mysql_data_pk(config, v, config_init)
            if config['run_mode'] == 'remote':
                amount = amount + int(config['sync_amount'])
        else:
            print('Tabke {}.{} no primary or unique key,skip sync!'.format(schema, tab))

    if config['run_mode'] == 'remote':
        config['sync_amount'] = str(amount)
        config['sync_duration'] = str(get_seconds(start_time))
        write_sync_log(config)


def get_nday_list(n):
    before_n_days = []
    for i in range(0, n + 1)[::-1]:
        before_n_days.append(str(datetime.date.today() - datetime.timedelta(days=i)))
    return before_n_days


def get_sync_where_day(tab, day):
    v_rq_col = tab.split(':')[1]
    v_rq_rqq = day + ' 0:0:0'
    v_rq_rqz = day + ' 23:59:59'
    v = "where {0} between '{1}' and '{2}'".format(v_rq_col, v_rq_rqq, v_rq_rqz)
    if tab.split(':')[1] == '':
        return ''
    else:
        return v


def sync_mysql_data_pk(config, ftab, config_init):
    try:
        config['sync_amount'] = 0
        i_counter = 0
        schema, tab = ftab.split(':')[0].split('.')
        v_where = get_sync_where_incr_pg(ftab, config)
        ins_sql_header = get_tab_header(config, schema, tab)
        n_batch_size = int(config['batch_size_incr'])
        db_source = config['db_pg_sour']
        cr_source = db_source.cursor()
        db_desc = config['db_mysql_desc']
        v_pk_names = get_sync_table_pk_names(db_source, schema, tab)
        v_pk_cols = get_sync_table_pk_vals(db_source, schema, tab)
        cr_desc = db_desc.cursor()
        n_tab_total_rows = get_sync_table_total_rows(db_source, schema, tab, v_where)
        v_sql = """select {} as pk,{} from {}.{} {}""". \
            format(v_pk_cols, get_sync_table_cols(config, schema, tab), schema, tab, v_where)
        cr_source.execute(v_sql)
        rs_source = cr_source.fetchmany(n_batch_size)
        start_time = datetime.datetime.now()

        if ftab.split(':')[1] == '':
            print("Sync Table increment :{0} ...".format(ftab.split(':')[0]))
        else:
            print("Sync Table increment :{0} for In recent {1} {2}..."
                  .format(ftab.split(':')[0], ftab.split(':')[2], config['sync_time_type']))

        if ftab.split(':')[1] == '':
            print('DB:{0},delete `{1}` table data please wait...'.format(config['db_mysql_desc_string'], tab,
                                                                         ftab.split(':')[2]))
            cr_desc.execute('delete from `{0}`'.format(tab))
            print('DB:{0},delete {1} table data ok!'.format(config['db_mysql_desc_string'], tab))

        while rs_source:
            v_sql = ''
            v_sql_del = ''
            for r in list(rs_source):
                rs_source_desc = cr_source.description
                ins_val = ""
                for j in range(1, len(r)):
                    col_type = str(rs_source_desc[j][1])
                    if r[j] is None:
                        ins_val = ins_val + "null,"
                    elif col_type in ('23', '700'):  # int,decimal
                        ins_val = ins_val + "'" + str(r[j]) + "',"
                    elif col_type in ('1114'):  # datetime
                        ins_val = ins_val + "'" + str(r[j]) + "',"
                    else:
                        ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                v_sql = v_sql + '(' + ins_val + config['sync_col_val'].replace('$PK$', r[0].replace('^^^', '_')) + '),'
                v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0]) + ","
            batch_sql = ins_sql_header + v_sql[0:-1]

            if ftab.split(':')[1] == '':
                config['run_sql'] = batch_sql
                cr_desc.execute(batch_sql)
            else:
                for d in v_sql_del[0:-1].split(','):
                    config['run_sql'] = 'delete from `{0}` where {1}'.format(tab, d)
                    cr_desc.execute('delete from `{0}` where {1}'.format(tab, d))
                config['run_sql'] = batch_sql
                cr_desc.execute(batch_sql)
            i_counter = i_counter + len(rs_source)

            if n_tab_total_rows == 0:
                print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                      .format(tab, n_tab_total_rows, i_counter, round(i_counter / 1 * 100, 2)), end='')
            else:
                print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%"
                      .format(tab, n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
            rs_source = cr_source.fetchmany(n_batch_size)
        print('')
        db_desc.commit()
        if config['run_mode'] == 'remote':
            config['sync_duration'] = str(get_seconds(start_time))
            config['sync_table_inteface'] = tab
            config['sync_amount'] = str(i_counter)
            write_sync_log_detail(config, ftab)
    except Exception as e:
        print('sync_mysql_data_pk exception:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)


def write_local_config_file(config):
    sync_db_sour = config['db_mysql_sour_ip'] + ':' + \
                   config['db_mysql_sour_port'] + ':' + \
                   config['db_mysql_sour_service'] + ':' + \
                   config['db_mysql_sour_user'] + ':' + \
                   config['db_mysql_sour_pass']

    sync_db_dest = config['db_mysql_desc_ip'] + ':' + \
                   config['db_mysql_desc_port'] + ':' + \
                   config['db_mysql_desc_service'] + ':' + \
                   config['db_mysql_desc_user'] + ':' + \
                   config['db_mysql_desc_pass']

    file_name = config['script_path'] + '/config/' + config['sync_tag'] + '.ini'
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


def init(config, debug, workdir):
    config = get_config_from_db(config, workdir)

    if config is None:
        exit(0)

    if debug:
        print_dict(config)

    return config


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
    #send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content
    send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'), config.get('receiver'),
                    v_title, v_content)


def exception_interface(config, v_title, v_content):
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
    #send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_templete)
    send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'), config.get('receiver'),
                    v_title, v_templete)


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
    v_title = config.get('comments') + '数据同步运行异常[★★★]'
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
    #send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)
    send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'), config.get('receiver'),
                    v_title, v_content)


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
    v_content = v_content.replace('$$sync_table$$', config.get('sync_table').replace(',', '<br>'))
    v_content = v_content.replace('$$sync_time_type$$', config.get('sync_time_type_name'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_time$$', config.get('run_time'))
    #send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content
    send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'), config.get('receiver'),
                    v_title, v_content)


def disconnect(config):
    config['db_pg_sour']
    config['db_mysql_desc']
    config['db_mysql_desc2']


def sync(config, debug):
    # sync table ddl
    sync_mysql_ddl(config, debug)

    # init sync table
    config_init = sync_mysql_init(config, debug)

    # sync data
    while True:
        # sync increment data
        sync_mysql_data(config, config_init)
        sys.exit(0)

        disconnect(config)

        update_sync_status(config, '0')

        sys.exit(0)


def pg_test(cfg):
    db = cfg['db_pg_sour']
    cr = db.cursor()
    cr.execute('select * from guangzhou_hopson_lane.building_kpis')
    rs = cr.fetchall()
    for i in rs:
        print(i)
    cr.close()


def main():
    # init variable
    config = ""
    debug = False
    check = False
    warnings.filterwarnings("ignore")
    # get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-workdir":
            workdir = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    # 初始化
    config = init(config, debug, workdir)

    # 数据同步
    update_sync_status(config, '1')
    sync(config, debug)


if __name__ == "__main__":
    main()
