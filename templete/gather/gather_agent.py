#!/usr/bin/env python3
import urllib.parse
import urllib.request
import traceback
import pymysql
import pymssql
import cx_Oracle
import sys
import os.path
import warnings
import psutil
import datetime
import time
import json
import ssl
import smtplib
import redis
import pymongo
from email.mime.text import MIMEText
from elasticsearch import Elasticsearch


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

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def send_mail25(p_from_user,p_from_pass,p_to_user,p_title,p_content):
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
                  <tr><td>任务标识</td><td>$$task_tag$$</td></tr>    
                  <tr><td>采集服务器</td><td>$$server_desc$$</td></tr>
                  <tr><td>数据源</td><td>$$db_sour$$</td></tr>             
                  <tr><td>同步脚本</td><td>$$script_file$$</td></tr>
                  <tr><td>异常信息</td><td>$$run_error$$</td></tr>
              </table>                
           </body>
        </html>
       '''
    v_title   = '数据同步数据库异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$task_tag$$' , config.get('task_tag'))
    v_content = v_content.replace('$$server_desc$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$db_sour$$', config.get('db_string'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_error$$', str(p_error))
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

def get_ds_mysql(ip,port,service ,user,password):
    try:
        conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',connect_timeout=3)
        return conn
    except  Exception as e:
        print('get_ds_mysql exceptiion:' + traceback.format_exc())
        return None

def get_ds_mysql_test(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',read_timeout=3)
    return conn

def get_ds_sqlserver(ip, port, service, user, password):
    try:
        conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service, charset='utf8')
        return conn
    except  Exception as e:
        print('get_ds_sqlserver exceptiion:' + traceback.format_exc())
        return None

def get_ds_sqlserver_test(ip, port, service, user, password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service, charset='utf8',timeout=3)
    return conn

def get_ds_oracle(ip,port,instance,user,password):
    print(ip,port,instance,user,password)
    tns          = cx_Oracle.makedsn(ip,int(port),instance)
    db           = cx_Oracle.connect(user,password,tns)
    return db

def get_ds_redis(ip,port,password):
    try:
        if password is None or password=='':
           conn =redis.Redis(host=ip, port=int(port), db=0)
        else:
           conn = redis.StrictRedis(host=ip, port=int(port), password=password, db=0)
        return conn
    except  Exception as e:
        print('get_ds_redis exceptiion:' + traceback.format_exc())
        return None

def get_ds_mongo(ip, port):
    try:
        conn = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip, int(port)))
        return conn
    except  Exception as e:
        print('get_ds_mongo exceptiion:' + traceback.format_exc())
        return None

def get_ds_mongo_auth(ip, port,user,password,service):
    try:
        conn          = pymongo.MongoClient('mongodb://{0}:{1}/'.format(ip,int(port)))
        db            = conn[service]
        db.authenticate(user, password)
        return conn
    except  Exception as e:
        print('get_ds_mongo_auth exceptiion:' + traceback.format_exc())
        return None

def get_ds_es(p_ip,p_port):
    try:
        conn = Elasticsearch([p_ip],port=p_port)
        return conn
    except  Exception as e:
        print('get_ds_es exceptiion:' + traceback.format_exc())
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

def get_config_from_db(tag):
    values = {
        'tag': tag
    }
    print('values=', values)
    url = 'http://$$API_SERVER$$/read_config_monitor'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    print('data=', data)
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        print('接口调用成功!')
        try:
            print(res['msg'])
            config = res['msg']
            if config['db_ip'] is not None:
               db_ip      = config['db_ip']
               db_port    = config['db_port']
               db_service = config['db_service']
               db_user    = config['db_user']
               db_type    = config['db_type']
               if db_type=='0':
                   db_pass = aes_decrypt(config['db_pass'], db_user)
                   config['db_string'] = 'msyql://'+db_ip + ':' + db_port + '/' + db_service
                   config['db_mysql']  = get_ds_mysql(db_ip, db_port, db_service, db_user, db_pass)
                   print('get_config_from_db=', db_ip, db_port, db_service, db_user, db_type, db_pass)
               if db_type=='1':
                   db_pass = aes_decrypt(config['db_pass'], db_user)
                   config['db_string'] = 'msyql://'+db_ip + ':' + db_port + '/' + db_service
                   config['db_oracle']  = get_ds_oracle(db_ip, db_port, db_service, db_user, db_pass)
                   print('get_config_from_db=', db_ip, db_port, db_service, db_user, db_type, db_pass)
               elif db_type=='2':
                   db_pass = aes_decrypt(config['db_pass'], db_user)
                   config['db_string'] = 'mssql://'+db_ip + ':' + db_port + '/' + db_service
                   config['db_mssql']  = get_ds_sqlserver(db_ip, db_port, db_service, db_user, db_pass)
                   print('get_config_from_db=', db_ip, db_port, db_service, db_user, db_type, db_pass)
               elif db_type=='4' :
                   config['db_string'] = 'elasticsearch://' + db_ip + ':' + db_port
                   config['db_elasticsearch'] =  get_ds_es(db_ip, db_port)
               elif db_type == '5':
                   db_pass=''
                   if config['db_pass'] !='':
                      db_pass = aes_decrypt(config['db_pass'], db_user)
                   config['db_string'] = 'redis://' + db_ip + ':' + db_port
                   config['db_redis']  = get_ds_redis(db_ip, db_port,db_pass)
               elif db_type == '6':
                   db_pass = ''
                   config['db_string'] = 'mongo://' + db_ip + ':' + db_port
                   if config['db_pass'] != '':
                       db_pass = aes_decrypt(config['db_pass'], db_user)
                       config['db_mongo'] = get_ds_mongo_auth(db_ip, db_port, db_user, db_pass, db_service)
                   else:
                       config['db_mongo'] = get_ds_mongo(db_ip, db_port)
               else:
                   pass

            return config
        except Exception as e:
            v_error = '从接口配置获取数据库连接对象时出现异常:{0}'.format(traceback.format_exc())
            print(v_error)
            exception_connect_db(config, v_error)
            exit(0)
    else:
        print('接口调用失败!,{0}'.format(res['msg']))  # 发异常邮件
        v_title = '数据同步接口异常[★]'
        v_content = '''<table class='xwtable'>
                                       <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                                       <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                                       <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr>            
                                   </table>'''
        v_content = v_content.replace('$$interface$$', url)
        v_content = v_content.replace('$$parameter$$', json.dumps(values))
        v_content = v_content.replace('$$error$$', res['msg'])
        exception_interface(v_title, v_content)
        sys.exit(0)

def init(config):
    config = get_config_from_db(config)
    #init disk I/O info
    if not os.path.isfile(os.getcwd()+'/disk_io.ini'):
       write_disk_init_info(config)
    #init net i/o info
    if not os.path.isfile(os.getcwd()+ '/net_io.ini'):
       write_net_init_info(config)
    print_dict(config)
    return config

def get_disk_usage():
    partitions = {}
    for partition in psutil.disk_partitions(all=True):
        if partition.mountpoint in('/','/home/hopson/apps','/home','/home/hopson/data','/db-backup','/file-backup','C:\\','D:\\'):
           partitions[partition.mountpoint.replace(':\\','')] = psutil.disk_usage(partition.mountpoint).percent
    return partitions

def get_tbs_usage(config):
    tbs_usage = {}
    db = config['db_oracle']
    cr = db.cursor()
    st = '''
     SELECT a.tablespace_name ,
             round((total - free) / total, 4) * 100 as usage_rate
      FROM (SELECT tablespace_name, SUM(bytes) free
      FROM dba_free_space
      GROUP BY tablespace_name) a,
     (SELECT tablespace_name, SUM(bytes) total
      FROM dba_data_files 
       where tablespace_name not in('SYSAUX')
     GROUP BY tablespace_name)b 
     WHERE a.tablespace_name = b.tablespace_name
    '''
    cr.execute(st)
    rs = cr.fetchall()
    for r in rs:
        tbs_usage[r[0]]=r[1]
    return  tbs_usage

def write_disk_init_info(config):
    d_disk_io  ={}
    try:
        d_disk_io['read_count']  = psutil.disk_io_counters().read_count
        d_disk_io['write_count'] = psutil.disk_io_counters().write_count
        d_disk_io['read_bytes']  = psutil.disk_io_counters().read_bytes
        d_disk_io['write_bytes'] = psutil.disk_io_counters().write_bytes
        d_disk_io['read_time']   = int(time.time())
        d_disk_io['write_time']  = int(time.time())
    except:
        d_disk_io['read_count'] = 0
        d_disk_io['write_count'] = 0
        d_disk_io['read_bytes'] = 0
        d_disk_io['write_bytes'] = 0
        d_disk_io['read_time'] = int(time.time())
        d_disk_io['write_time'] = int(time.time())
    with open(os.getcwd()+'/disk_io.ini', 'w') as f:
        f.write(json.dumps(d_disk_io, ensure_ascii=False, indent=4, separators=(',', ':')))

def write_net_init_info(config):
    d_net_io = {}
    try:
        d_net_io['sent_bytes'] = psutil.net_io_counters().bytes_sent
        d_net_io['recv_bytes'] = psutil.net_io_counters().bytes_recv
        d_net_io['sent_time']  = int(time.time())
        d_net_io['recv_time']  = int(time.time())
    except:
        d_net_io['sent_bytes'] = 0
        d_net_io['recv_bytes'] = 0
        d_net_io['sent_time'] = int(time.time())
        d_net_io['recv_time'] = int(time.time())
    with open('net_io.ini', 'w') as f:
        f.write(json.dumps(d_net_io, ensure_ascii=False, indent=4, separators=(',', ':')))

def get_disk_io_info(config):
    d_disk_io  = {}
    d_prev_disk_io = {}
    disk_stats = {}
    # with open(os.getcwd()+'/disk_io.ini', 'r') as f:
    #     prev_disk_io = f.read()
    # d_prev_disk_io=json.loads(prev_disk_io)
    try:
        d_prev_disk_io['read_bytes']  = psutil.disk_io_counters().read_bytes
        d_prev_disk_io['write_bytes'] = psutil.disk_io_counters().write_bytes
    except:
        d_prev_disk_io['read_bytes'] = 0
        d_prev_disk_io['write_bytes'] = 0
    d_prev_disk_io['read_time']   = int(time.time())
    d_prev_disk_io['write_time']  = int(time.time())
    time.sleep(1)
    try:
        d_disk_io['read_bytes']       = psutil.disk_io_counters().read_bytes
        d_disk_io['write_bytes']      = psutil.disk_io_counters().write_bytes
    except:
        d_disk_io['read_bytes']       = 0
        d_disk_io['write_bytes']      = 0
    d_disk_io['read_time']        = int(time.time())
    d_disk_io['write_time']       = int(time.time())
    disk_stats['read_bytes']      = int((d_disk_io['read_bytes']  * 1.0 - d_prev_disk_io['read_bytes']) / (d_disk_io['read_time'] - d_prev_disk_io['read_time']))
    disk_stats['write_bytes']     = int((d_disk_io['write_bytes'] * 1.0 - d_prev_disk_io['write_bytes']) / (d_disk_io['write_time']- d_prev_disk_io['write_time']))
    return disk_stats

def get_net_io_info(config):
    d_net_io  = {}
    net_stats = {}
    d_prev_disk_io = {}
    # with open('net_io.ini', 'r') as f:
    #     prev_net_io = f.read()
    # d_prev_disk_io=json.loads(prev_net_io)
    d_prev_disk_io['sent_bytes']   = psutil.net_io_counters().bytes_sent
    d_prev_disk_io['recv_bytes']   = psutil.net_io_counters().bytes_recv
    d_prev_disk_io['sent_time']    = int(time.time())
    d_prev_disk_io['recv_time']    = int(time.time())
    time.sleep(1)
    d_net_io['sent_bytes']         = psutil.net_io_counters().bytes_sent
    d_net_io['recv_bytes']         = psutil.net_io_counters().bytes_recv
    d_net_io['sent_time']          = int(time.time())
    d_net_io['recv_time']          = int(time.time())
    net_stats['sent_bytes']        = int((d_net_io['sent_bytes'] - d_prev_disk_io['sent_bytes']) / (d_net_io['sent_time'] - d_prev_disk_io['sent_time']))
    net_stats['recv_bytes']        = int((d_net_io['recv_bytes'] - d_prev_disk_io['recv_bytes']) / (d_net_io['recv_time']- d_prev_disk_io['recv_time']))
    return net_stats

'''
  功能: mysql 数据库监控指标
'''
def get_mysql_total_connect(config):
    try:
        db = config['db_mysql']
        cr = db.cursor()
        cr.execute("SELECT count(0) FROM information_schema.processlist")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_mysql_active_connect(config):
    try:
        db = config['db_mysql']
        cr = db.cursor()
        cr.execute("SELECT count(0) FROM information_schema.processlist where command <>'Sleep'")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_mysql_available(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = ''
        db_user    = config['db_user']
        db_pass    = aes_decrypt(config['db_pass'], db_user)
        db         = get_ds_mysql_test(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        cr.execute("SELECT 1")
        db.commit()
        cr.close()
        return 1
    except Exception as e:
        print('get_mysql_available exceptiion:' + traceback.format_exc())
        return 0

def mysql_qps(config):
    try:
        db_ip   = config['db_ip']
        db_port = config['db_port']
        db_service = ''
        db_user = config['db_user']
        db_pass = aes_decrypt(config['db_pass'], db_user)
        db = get_ds_mysql_test(db_ip, db_port, db_service, db_user, db_pass)
        cr = db.cursor()
        cr.execute("SHOW GLOBAL STATUS LIKE 'Questions'")
        rs=cr.fetchone()
        questions=int(rs[1])
        cr.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
        rs = cr.fetchone()
        uptime = int(rs[1])
        qps = round(questions / uptime,2)
        print('mysql_qps=',questions,uptime)
        cr.close()
        return qps
    except Exception as e:
        print('mysql_qps exceptiion:' + traceback.format_exc())
        return 0

def mysql_tps(config):
    try:
        db_ip   = config['db_ip']
        db_port = config['db_port']
        db_service = ''
        db_user = config['db_user']
        db_pass = aes_decrypt(config['db_pass'], db_user)
        db = get_ds_mysql_test(db_ip, db_port, db_service, db_user, db_pass)
        cr = db.cursor()
        cr.execute("SHOW GLOBAL STATUS LIKE 'Com_commit'")
        rs=cr.fetchone()
        com_commit=int(rs[1])
        cr.execute("SHOW GLOBAL STATUS LIKE 'Com_rollback'")
        rs = cr.fetchone()
        com_rollback = int(rs[1])
        cr.execute("SHOW GLOBAL STATUS LIKE 'Uptime'")
        rs = cr.fetchone()
        uptime = int(rs[1])
        tps = round((com_commit+com_rollback)/uptime ,2)
        print(com_commit,com_rollback,uptime,tps)
        cr.close()
        return tps
    except Exception as e:
        print('mysql_tps exceptiion:' + traceback.format_exc())
        return 0

'''
  功能:Oracle数据库监控指标
'''
def get_oracle_total_connect(config):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute("SELECT count(0) FROM v$session")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_oracle_active_connect(config):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute("SELECT count(0) FROM v$session where status='ACTIVE'")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_oracle_available(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = config['db_service']
        db_user    = config['db_user']
        db_pass    = aes_decrypt(config['db_pass'], db_user)
        db         = get_ds_oracle(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        cr.execute("SELECT 1 from dual")
        db.commit()
        cr.close()
        return 1
    except Exception as e:
        print('get_oracle_available exceptiion:' + traceback.format_exc())
        return 0

def get_oracle_qps(config):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute("select round(sum(value),2)  from gv$sysmetric where metric_name ='I/O Requests per Second'")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_oracle_tps(config):
    try:
        db = config['db_oracle']
        cr = db.cursor()
        cr.execute("select round(sum(value),2)  from gv$sysmetric where metric_name = 'User Transaction Per Sec'")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

'''
  功能:SQLServer 数据库监控指标
'''
def get_mssql_total_connect(config):
    try:
        db = config['db_mssql']
        cr = db.cursor()
        cr.execute("select count(0) from sys.sysprocesses  where spid>50  and spid<>@@SPID")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_mssql_active_connect(config):
    try:
        db = config['db_mssql']
        cr = db.cursor()
        cr.execute("select count(0) from sys.sysprocesses  where spid>50 and status<>'sleeping' and spid<>@@SPID")
        rs=cr.fetchone()
        cr.close()
        return rs[0]
    except:
        return 0

def get_mssql_available(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = config['db_service']
        db_user    = config['db_user']
        db_pass    = aes_decrypt(config['db_pass'], db_user)
        db         = get_ds_sqlserver_test(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        cr.execute("SELECT 1")
        db.commit()
        cr.close()
        return 1
    except Exception as e:
        print('get_mssql_available exceptiion:' + traceback.format_exc())
        return 0

def get_mssql_qps(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = config['db_service']
        db_user    = config['db_user']
        db_pass    = aes_decrypt(config['db_pass'], db_user)
        db         = get_ds_sqlserver_test(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        st         = "select cntr_value from sys.dm_os_performance_counters where counter_name = 'Lock Requests/sec' and instance_name = 'Database'"
        cr.execute(st)
        rs=cr.fetchone()
        cntr_value1=rs[0]
        time.sleep(3)
        cr.execute(st)
        rs = cr.fetchone()
        cntr_value2 = rs[0]
        db.commit()
        cr.close()
        return round((cntr_value2-cntr_value1)/3,2)
    except Exception as e:
        print('get_mssql_qps exceptiion:' + traceback.format_exc())
        return 0

def get_mssql_tps(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = config['db_service']
        db_user    = config['db_user']
        db_pass    = aes_decrypt(config['db_pass'], db_user)
        db         = get_ds_sqlserver_test(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        st         = "select cntr_value from sys.dm_os_performance_counters where counter_name = 'Transactions/sec' and instance_name = '_Total'"
        cr.execute(st)
        rs=cr.fetchone()
        cntr_value1=rs[0]
        time.sleep(3)
        cr.execute(st)
        rs = cr.fetchone()
        cntr_value2 = rs[0]
        db.commit()
        cr.close()
        return round((cntr_value2-cntr_value1)/3,2)
    except Exception as e:
        print('get_mssql_tps exceptiion:' + traceback.format_exc())
        return 0

def getUniqueNumber():
    import datetime
    import random;
    for i in range(0, 10):
        nowTime = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    randomNum = random.randint(0, 100)
    if randomNum <= 10:
        randomNum = str(0) + str(randomNum)
    uniqueNum = str(nowTime) + str(randomNum)
    return  uniqueNum

def get_es_available(config):
    db = get_ds_es(config['db_ip'], config['db_port'])
    if db :
       return 1
    else:
       return 0

def get_redis_available(config):
    try:
        r= config['db_redis']
        key = 'dbapi_' + getUniqueNumber()
        r.set(key,'0')
        r.delete(key)
        return 1
    except Exception as e:
        print('get_redis_available exceptiion:' + traceback.format_exc())
        return 0

def get_mongo_available(config):
    try:
        mongo = config['db_mongo']
        db_names = mongo.list_database_names()
        print('get_mongo_available=',db_names)
        return 1
    except Exception as e:
        print('get_mongo_available exceptiion:' + traceback.format_exc())
        return 0

def get_mongo_total_connect(config):
    mongo = config['db_mongo']
    db    = mongo.admin
    rs    = db.command('serverStatus')['connections']['totalCreated']
    return rs

def get_mongo_active_connect(config):
    mongo = config['db_mongo']
    db    = mongo.admin
    rs    = db.command('serverStatus')['connections']['current']
    return rs

def get_mongo_qps(config):
    mongo  = config['db_mongo']
    db     = mongo.admin
    query  = db.command('serverStatus')['opcounters']['query']
    uptime = db.command('serverStatus')['uptime']
    return round(query/uptime,2)

def get_mongo_tps(config):
    mongo  = config['db_mongo']
    db     = mongo.admin
    insert = db.command('serverStatus')['opcounters']['insert']
    update = db.command('serverStatus')['opcounters']['update']
    delete = db.command('serverStatus')['opcounters']['delete']
    uptime = db.command('serverStatus')['uptime']
    return round((insert+update+delete) / uptime, 2)


def gather(config):
    d_item = {}
    for idx in config['templete_indexes'].split(','):
        if idx == 'cpu_total_usage':
           d_item['cpu_total_usage'] = psutil.cpu_percent(interval=3, percpu=False)
        elif idx == 'cpu_core_usage':
           d_item['cpu_core_usage'] = psutil.cpu_percent(interval=3, percpu=True)
        elif idx == 'mem_usage':
           d_item['mem_usage']      = psutil.virtual_memory().percent
        elif idx == 'disk_usage':
           d_item['disk_usage']     = json.dumps(get_disk_usage())
        elif idx == 'disk_read':
           d_item['disk_read']      = get_disk_io_info(config)['read_bytes']
        elif idx == 'disk_write':
           d_item['disk_write']     = get_disk_io_info(config)['write_bytes']
        elif idx == 'net_out':
           d_item['net_out']        = get_net_io_info(config)['sent_bytes']
        elif idx == 'net_in':
           d_item['net_in']         = get_net_io_info(config)['recv_bytes']
        elif idx == 'mysql_total_connect':
           d_item['total_connect']  = get_mysql_total_connect(config)
        elif idx == 'mysql_active_connect':
           d_item['active_connect'] = get_mysql_active_connect(config)
        elif idx == 'mysql_available':
           d_item['db_available']   = get_mysql_available(config)
        elif idx == 'mysql_qps':
            d_item['db_qps']        = mysql_qps(config)
        elif idx == 'mysql_tps':
            d_item['db_tps']        = mysql_tps(config)
        elif idx == 'mssql_total_connect':
           d_item['total_connect']  = get_mssql_total_connect(config)
        elif idx == 'mssql_active_connect':
           d_item['active_connect'] = get_mssql_active_connect(config)
        elif idx == 'mssql_available':
           d_item['db_available']   =  get_mssql_available(config)
        elif idx == 'mssql_qps':
            d_item['db_qps']        = get_mssql_qps(config)
        elif idx == 'mssql_tps':
            d_item['db_tps']        = get_mssql_tps(config)
        elif idx == 'oracle_total_connect':
           d_item['total_connect']  = get_oracle_total_connect(config)
        elif idx == 'oracle_active_connect':
           d_item['active_connect'] = get_oracle_active_connect(config)
        elif idx == 'oracle_available':
           d_item['db_available']   = get_oracle_available(config)
        elif idx == 'oracle_qps':
            d_item['db_qps']        = get_oracle_qps(config)
        elif idx == 'oracle_tps':
            d_item['db_tps']        = get_oracle_tps(config)
        elif idx == 'es_available':
           d_item['db_available']   = get_es_available(config)
        elif idx == 'redis_available':
           d_item['db_available']   = get_redis_available(config)
        elif idx == 'mongo_available':
           d_item['db_available']   = get_mongo_available(config)
        elif idx == 'mongo_total_connect':
           d_item['total_connect']   =get_mongo_total_connect(config)
        elif idx == 'mongo_active_connect':
           d_item['active_connect'] = get_mongo_active_connect(config)
        elif idx == 'mongo_qps':
           d_item['db_qps']        = get_mongo_qps(config)
        elif idx == 'mongo_tps':
           d_item['db_tps']        = get_mongo_tps(config)
        elif idx == 'oracle_tablespace':
           d_item['db_tbs_usage']  = json.dumps(get_tbs_usage(config))
        else:
           pass

    d_item['server_id'] = config['server_id']
    d_item['db_id']     = config['db_id']
    d_item['task_tag']  = config['task_tag']
    d_item['market_id'] = config['market_id']
    print('gather=', d_item)
    write_monitor_log(d_item)


def write_monitor_log(item):
    v_tag = {
        'task_tag'            : item.get('task_tag'),
        'server_id'           : item.get('server_id'),
        'db_id'               : item.get('db_id'),
        'cpu_total_usage'     : item.get('cpu_total_usage'),
        'cpu_core_usage'      : item.get('cpu_core_usage'),
        'mem_usage'           : item.get('mem_usage'),
        'disk_usage'          : item.get('disk_usage'),
        'disk_read'           : item.get('disk_read'),
        'disk_write'          : item.get('disk_write'),
        'net_out'             : item.get('net_out'),
        'net_in'              : item.get('net_in'),
        'total_connect'       : item.get('total_connect'),
        'active_connect'      : item.get('active_connect'),
        'db_available'        : item.get('db_available'),
        'db_tbs_usage'        : item.get('db_tbs_usage',''),
        'db_qps'              : item.get('db_qps',''),
        'db_tps'              : item.get('db_tps',''),
        'market_id'           : item.get('market_id')
    }
    print('write_monitor_log=',v_tag)
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    url = 'http://$$API_SERVER$$/write_monitor_log'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] != 200:
        print('Interface write_monitor_log call failed!')

def ping(cfg):
    ip = 'ping -w 10 {}'.format(cfg['server_ip'])
    print(ip)
    r = os.popen(ip)
    for i in r:
        print(i)
        if i.count('icmp_seq=10') > 0:
            return True
    return False


def main():
    config = ""
    warnings.filterwarnings("ignore")
    # get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]

    #初始化
    config=init(config)

    #数据同步
    # if ping(config):
    #    print('ping is ok!')
    #    gather(config)
    # else:
    #    print('ping is not ok,exit!')
    gather(config)



if __name__ == "__main__":
     main()
