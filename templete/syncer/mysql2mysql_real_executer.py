#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/8/6 10:54
# @Author : ma.fei
# @File : schedule.py.py
# @Software: PyCharm

import os
import sys
import time
import json
import requests
import pymysql
import logging
import datetime
import warnings
import traceback
import smtplib
import socket
import psutil
from email.mime.text import MIMEText
from clickhouse_driver import Client
from concurrent.futures import ProcessPoolExecutor,wait,as_completed
from logging.handlers import TimedRotatingFileHandler

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

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
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

def send_mail25(p_sendserver,p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP(p_sendserver, 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail465(p_sendserver,p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL(p_sendserver, 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
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

def exception_executer(config,p_error):
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
    #send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content
    send_mail_param(config.get('send_server'),config.get('sender'),config.get('sendpass'),config.get('receiver'),v_title, v_content)

def log(msg,pos='l'):
    tm = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    if pos == 'l':
       print("""{} : {}""".format(tm,msg))
    else:
       print("""{} : {}""".format(msg,tm))

def log2(msg):
    tm = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    print("""\r{} : {}""".format(tm,msg),end='')

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    logger.info('-'.ljust(85, '-'))
    logger.info(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    logger.info('-'.ljust(85, '-'))
    for key in config:
        logger.info(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=' + str(config[key]))
    logger.info('-'.ljust(85, '-'))


def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par,timeout=60).json()
        if res['code'] == 200:
            config = res['msg']
            return config
        else:
            logger.info('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
            return None
    except:
        logger.info('aes_decrypt api not available!')
        return None

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           autocommit=True)
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip,
                           port=int(port),
                           user=user,
                           passwd=password,
                           db=service,
                           charset='utf8mb4',
                           cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn

def get_ds_ck(ip,port,service ,user,password):
    return  Client(host=ip,
                   port=port,
                   user=user,
                   password=password,
                   database=service,
                   send_receive_timeout=600000)

def check_tab_exists_pk(cfg,event):
   db = get_ds_mysql(cfg['db_mysql_ip'],
                     cfg['db_mysql_port'],
                     cfg['db_mysql_service'],
                     cfg['db_mysql_user'],
                     cfg['db_mysql_pass'])

   cr = db.cursor()
   st = """select count(0) from information_schema.columns
              where table_schema='{}' and table_name='{}' and column_key='PRI'""".format(event['schema'],event['table'])
   cr.execute(st)
   rs=cr.fetchone()
   cr.close()
   return rs[0]

def get_tables(cfg,o):
    db = get_ds_mysql(cfg['db_mysql_ip'],
                      cfg['db_mysql_port'],
                      cfg['db_mysql_service'],
                      cfg['db_mysql_user'],
                      cfg['db_mysql_pass'])
    cr  = db.cursor()
    sdb = o.split('$')[0].split('.')[0]
    tab = o.split('$')[0].split('.')[1]
    ddb = o.split('$')[1]
    if tab.count('*') > 0:
       tab = tab.replace('*','')
       st = """select table_name from information_schema.tables
                  where table_schema='{}' and instr(table_name,'{}')>0 order by table_name""".format(sdb,tab)
       cr.execute(st)
       rs = cr.fetchall()
       vv = ''
       for i in list(rs):
           evt = {'schema': o.split('$')[0].split('.')[0], 'table': i[0]}
           if check_tab_exists_pk(cfg,evt)>0:
              vv = vv + '{}.{}${},'.format(sdb,i[0],ddb)
       cr.close()
       return vv[0:-1]
    else:
       return o

def get_sync_tables(cfg):
    v = ''
    for o in cfg['sync_table'].split(','):
        if o !='':
           v = v + get_tables(cfg,o)+','
    cfg['sync_table'] = v[0:-1]
    return cfg



def upd_cfg(config):
    db_mysql_ip = config['sync_db_sour'].split(':')[0]
    db_mysql_port = config['sync_db_sour'].split(':')[1]
    db_mysql_service = config['sync_db_sour'].split(':')[2]
    db_mysql_user = config['sync_db_sour'].split(':')[3]
    db_mysql_pass = aes_decrypt(config['sync_db_sour'].split(':')[4], db_mysql_user)
    db_mysql_ip_dest = config['sync_db_dest'].split(':')[0]
    db_mysql_port_dest = config['sync_db_dest'].split(':')[1]
    db_mysql_service_dest = config['sync_db_dest'].split(':')[2]
    db_mysql_user_dest = config['sync_db_dest'].split(':')[3]
    db_mysql_pass_dest = aes_decrypt(config['sync_db_dest'].split(':')[4], db_mysql_user_dest)
    config['db_mysql_ip'] = db_mysql_ip
    config['db_mysql_port'] = db_mysql_port
    config['db_mysql_service'] = db_mysql_service
    config['db_mysql_user'] = db_mysql_user
    config['db_mysql_pass'] = db_mysql_pass
    config['db_mysql_ip_dest'] = db_mysql_ip_dest
    config['db_mysql_port_dest'] = db_mysql_port_dest
    config['db_mysql_service_dest'] = db_mysql_service_dest
    config['db_mysql_user_dest'] = db_mysql_user_dest
    config['db_mysql_pass_dest'] = db_mysql_pass_dest

    config['db_mysql_string'] = config['db_mysql_ip'] + ':' + config['db_mysql_port'] + '/' + config['db_mysql_service']
    config['db_mysql_dest_string'] = config['db_mysql_ip_dest'] + ':' + config['db_mysql_port_dest'] + '/' + config['db_mysql_service_dest']
    config['exec_tag'] = config['sync_tag'].replace('_executer', '_logger')
    config['sleep_time'] = float(config['sync_gap'])

    if config.get('ds_ro') is not None and config.get('ds_ro') != '':
        config['db_mysql_ip_ro'] = config['ds_ro']['ip']
        config['db_mysql_port_ro'] = config['ds_ro']['port']
        config['db_mysql_service_ro'] = config['ds_ro']['service']
        config['db_mysql_user_ro'] = config['ds_ro']['user']
        config['db_mysql_pass_ro'] = aes_decrypt(config['ds_ro']['password'], config['ds_ro']['user'])

    if config.get('ds_log') is not None and config.get('ds_log') != '':
        config['db_mysql_ip_log'] = config['ds_log']['ip']
        config['db_mysql_port_log'] = config['ds_log']['port']
        config['db_mysql_service_log'] = config['ds_log']['service']
        config['db_mysql_user_log'] = config['ds_log']['user']
        config['db_mysql_pass_log'] = aes_decrypt(config['ds_log']['password'], config['ds_log']['user'])
    else:
        print('mysql日志库不能为空!')
        logger.info('mysql日志库不能为空!')
        sys.exit(0)
    config = get_sync_tables(config)
    return config

def get_config_from_db(tag,workdir):
    url = 'http://$$API_SERVER$$/read_config_sync'
    res = None
    try:
      res = requests.post(url, data= { 'tag': tag},timeout=30).json()
    except:
      pass

    if res is not None and res['code'] == 200:
        config = upd_cfg(res['msg'])
        write_local_config(res['msg'])
        return config
    else:
        lname = '{}/{}_cfg.json'.format(workdir, tag)
        logger.info('Load interface `$$API_SERVER$$` failure!')
        logger.info('Read local config file `{}`.'.format(lname))
        config = get_local_config(lname)
        if config is None:
            logger.info('Load local config failure!')
            return None
        else:
            #config = upd_cfg(config)
            return config


def get_mysql_schema(cfg,event):
    for o in cfg['sync_table'].split(','):
      mysql_schema = o.split('.')[0]
      mysql_table  = o.split('$')[0].split('.')[1]
      dest_schema  = o.split('$')[1] if o.split('$')[1] !='auto' else mysql_schema
      if event['schema'] == mysql_schema and event['table'] == mysql_table:
         return  cfg['desc_db_prefix']+dest_schema
    return 'test'

def check_mysql_database(cfg,event):
    db = cfg['db_mysql_dest']
    cr = db.cursor()
    st = """select count(0) from information_schema.`SCHEMATA` d 
                  where schema_name ='{}'""".format(get_mysql_schema(cfg, event))
    cr.execute(st)
    rs = cr.fetchone()
    return rs[0]

def check_mysql_tab_exists(cr,cfg,event):
   st="""select count(0) from information_schema.tables
            where table_schema='{}' and table_name='{}'""".format(get_mysql_schema(cfg,event),event['table'])
   cr.execute(st)
   rs = cr.fetchone()
   return rs[0]

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def write_sync_log(config):
    par = {
            'sync_tag'       : config['sync_tag'],
            'event_amount'   : config['event_amount'],
            'insert_amount'  : config['insert_amount'],
            'update_amount'  : config['update_amount'],
            'delete_amount'  : config['delete_amount'],
            'ddl_amount'     : config['ddl_amount'],
            'binlogfile'     : '',
            'binlogpos'      : '',
            'c_binlogfile'   : '',
            'c_binlogpos'    : '',
            'create_date'    : get_time()
    }
    try:
        url = 'http://$$API_SERVER$$/write_sync_real_log'
        res = requests.post(url, data={'tag': json.dumps(par)},timeout=30)
        if res.status_code != 200:
           logger.info('write_sync_log failure1!')
    except:
       logger.info('write_sync_log failure2!')

def write_mysql(cfg,tab):
    db_dest = get_ds_mysql(cfg['db_mysql_ip_dest'],
                           cfg['db_mysql_port_dest'],
                           cfg['db_mysql_service_dest'],
                           cfg['db_mysql_user_dest'],
                           cfg['db_mysql_pass_dest'])
    cr_dest = db_dest.cursor()
    db_log = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                               cfg['db_mysql_port_log'],
                               cfg['log_db_name'],
                               cfg['db_mysql_user_log'],
                               cfg['db_mysql_pass_log'])
    cr_log = db_log.cursor()
    st_log = """select id,sync_table,statement,type
                   from t_db_sync_log where  status='0' and sync_tag='{}' and sync_table='{}' 
                       order by id limit {}""".format(cfg['exec_tag'],tab,cfg['batch_size_incr'])
    cr_log.execute(st_log)
    rs_log = cr_log.fetchall()
    cfg['event_amount']  = len(rs_log)
    cfg['insert_amount'] = 0
    cfg['update_amount'] = 0
    cfg['delete_amount'] = 0
    cfg['ddl_amount'] = 0
    write_sync_log(cfg)
    logger.info("\033[1;37;40m[{}] write sync log to db!\033[0m".format(cfg['sync_tag'].split('_')[0]))
    ids=''
    for r in rs_log:
        event = {'schema': r['sync_table'].split('.')[0], 'table': r['sync_table'].split('.')[1]}
        if check_mysql_tab_exists(cr_dest,cfg,event) == 0:
           logger.info('Table:{}.{} not exists,skip incr sync!'.format(get_mysql_schema(cfg,event),event['table']))
           time.sleep(1)
           continue
        try:
            cr_dest.execute(r['statement'])
            ids = ids + '{},'.format(r['id'])
        except:
            if traceback.format_exc().count('Duplicate entry') > 0:
                ids = ids + '{},'.format(r['id'])
            else:
                logger.info('execute statement error!!!')
                logger.info('\033[0;36;40m' + r['statement'] + '\033[0m')
                logger.info(traceback.print_exc())

                # send mail
                v_title = 'mysql->mysql实时同步任务执行失败告警[e]'
                v_error = 'execute statement error!\n' + traceback.print_exc() + '\n' + 'rs_log=' + rs_log + '\n' + 'statement=' + \
                          r['statement']
                v_templete = exception_executer(cfg, v_error)
                send_mail_param(cfg.get('send_server'),
                                cfg.get('sender'),
                                cfg.get('sendpass'),
                                cfg.get('receiver'),
                                v_title, v_templete)

    if ids != '':
        upd = "update t_db_sync_log set status='1' where id in({})".format(ids[0:-1])
        try:
          cr_log.execute(upd)
          logger.info('Task {} execute complete!'.format(tab))
        except:
          logger.info('update t_db_sync_log error!')
          logger.info(traceback.print_exc())
          # send mail
          v_title = 'mysql->mysql实时同步任务执行失败告警[u]'
          v_error = 'update t_db_sync_log error!\n' + traceback.print_exc() + '\n' + 'upd=' + upd
          v_templete = exception_executer(cfg, v_error)
          send_mail_param(cfg.get('send_server'),
                          cfg.get('sender'),
                          cfg.get('sendpass'),
                          cfg.get('receiver'),
                          v_title, v_templete)
          # sys.exit(0)

def get_tasks(cfg):
    db = get_ds_mysql_dict(cfg['db_mysql_ip_log'],
                           cfg['db_mysql_port_log'],
                           cfg['log_db_name'],
                           cfg['db_mysql_user_log'],
                           cfg['db_mysql_pass_log'])
    cr = db.cursor()
    st = """SELECT sync_table,count(0) as amount FROM t_db_sync_log 
               WHERE  status='0' and sync_tag='{}' GROUP BY sync_table limit {}""".format(cfg['exec_tag'],cfg['process_num'])
    cr.execute(st)
    rs = cr.fetchall()
    return  rs

def read_real_sync_status(p_tag):
    try:
        par = {'tag': p_tag}
        url = 'http://$$API_SERVER$$/get_real_sync_status'
        res = requests.post(url,data=par,timeout=30).json()
        return res
    except:
        logger.info('read_real_sync_status failure!')
        return None

def start_syncer(cfg,workdir):
    apply_time = datetime.datetime.now()
    sleep_time = datetime.datetime.now()
    sync_time  = datetime.datetime.now()
    with ProcessPoolExecutor(max_workers=cfg['process_num']) as executor:
        while True:
            if get_seconds(apply_time) >= cfg['apply_timeout']:
               apply_time = datetime.datetime.now()
               cfg = get_config_from_db(cfg['sync_tag'],workdir)
               logger.info("\033[1;36;40\nmapply config success\033[0m")

            tasks = get_tasks(cfg)
            if tasks!=():
                logger.info('\n检测到新事件：')
                logger.info('-'.ljust(85, '-'))
                for task in tasks:
                    logger.info(' '.ljust(3, ' ') + task['sync_table'].ljust(50, ' ') + ' = '+ str(task['amount']))
                logger.info('-'.ljust(85, '-'))
                logger.info('\n')

                sleep_time = datetime.datetime.now()
                all_task = [executor.submit(write_mysql,cfg,t['sync_table']) for t in tasks]

                for future in as_completed(all_task):
                    res = future.result()
                    if res is not None:
                       log(res)
            else:
               if get_seconds(sleep_time) % 60 == 0:
                  logger.info('未检测到任务，休眠中:{}s ...'.format(str(get_seconds(sleep_time))))
                  time.sleep(1)

               if get_seconds(sync_time) >= 30:
                   sync_time = datetime.datetime.now()
                   sync_status = read_real_sync_status(cfg['sync_tag'])
                   if sync_status is not None:
                       if sync_status['msg']['real_sync_status'] == 'PAUSE':
                           while True:
                               time.sleep(1)
                               if sync_status['msg']['real_sync_status'] == 'PAUSE':
                                   logger.info("\033[1;37;40msync task {} suspended!\033[0m".format(cfg['sync_tag']))
                                   continue
                               elif sync_status['msg']['real_sync_status']== 'STOP':
                                   logger.info("\033[1;37;40msync task {} terminate!\033[0m".format(cfg['sync_tag']))
                                   break
                               else:
                                   break
                       elif sync_status['msg']['real_sync_status'] == 'STOP':
                           logger.info("\033[1;37;40msync task {} terminate!\033[0m".format(cfg['sync_tag']))
                           break



def write_pid(cfg):
    ckpt = {
       'pid':os.getpid()
    }
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'w') as f:
        f.write(json.dumps(ckpt, ensure_ascii=False, indent=4, separators=(',', ':')))
    logger.info('write pid:{}'.format(ckpt['pid']))


def check_pid(cfg):
    return os.path.isfile('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']))

def read_pid(cfg):
    with open('{}/{}.json'.format(cfg['script_path'],cfg['sync_tag']), 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        ckpt = json.loads(contents)
        return ckpt

# def get_task_status(cfg):
#     c = 'ps -ef|grep {} |grep {} | grep -v grep |  wc -l'.format(cfg['sync_tag'],cfg['script_file'])
#     r = os.popen(c).read()
#     if len(r.split('\n')) >= 3 :
#        logger.info('Executer Task already running!')
#        return True
#     else:
#        return False

def get_task_status(cfg):
    if check_pid(cfg):
        pid = read_pid(cfg).get('pid')
        if pid :
            if pid == os.getpid():
                return False

            if int(pid) in psutil.pids():
              return True
            else:
              return False
        else:
            return False
    else:
        return False

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)

def get_local_config(lname):
    try:
        with open(lname, 'r') as f:
             cfg = json.loads(f.read())
        return cfg
    except:
        return None

def write_local_config(config):
    file_name = '{}/{}_cfg.json'.format(config['script_path'],config['sync_tag'])
    with open(file_name, 'w') as f:
        f.write(json.dumps(config, ensure_ascii=False, indent=4, separators=(',', ':')))


if __name__=="__main__":
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]
        elif sys.argv[p] == "-workdir":
            workdir = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    # init logger
    # logging.basicConfig(filename='/tmp/{}.{}.log'.format(tag,datetime.datetime.now().strftime("%Y-%m-%d")),
    #                     format='[%(asctime)s-%(levelname)s:%(message)s]',
    #                     level=logger.info, filemode='a', datefmt='%Y-%m-%d %I:%M:%S')

    # 配置日志
    logging.basicConfig()
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.INFO)
    # 创建TimedRotatingFileHandler，按天切割日志
    handler = TimedRotatingFileHandler(filename='/tmp/{}.log'.format(tag), when='D', interval=1, backupCount=7)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    # 添加到logger
    logger.addHandler(handler)

    # call api get config
    cfg = get_config_from_db(tag,workdir)

    if cfg is None:
        logger.info('load config failure,exit sync!')
        sys.exit(0)

    # query system parameters to determine whether to run the  program
    if read_real_sync_status(tag) is not None and read_real_sync_status(tag)['msg']['real_sync_status'] == 'STOP':
        logger.info("\033[1;37;40mTask `{}` terminate!\033[0m".format(cfg['sync_tag']))
        sys.exit(0)

    # check task
    if not get_task_status(cfg):
       write_pid(cfg)
       print_dict(cfg)
       start_syncer(cfg,workdir)
    # else:
    #    logger.info('sync program:{} is running!'.format(tag))
