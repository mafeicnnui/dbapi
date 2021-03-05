#!/usr/bin/env python3
import urllib.parse
import urllib.request
import traceback
import pymysql
import sys
import os.path
import warnings
import psutil
import datetime
import time
import json
import ssl
import smtplib
from email.mime.text import MIMEText

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
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8', cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mysql_test(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',read_timeout=3)
    return conn

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
    url = 'http://$$API_SERVER$$/read_config_db'
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
            config     = res['msg']
            db_ip      = config['db_ip']
            db_port    = config['db_port']
            db_service = config['db_service']
            db_user    = config['db_user']
            db_pass    = aes_decrypt(config['db_pass'], db_user)
            config['db_string'] = db_ip + ':' + db_port + '/' + db_service
            config['db_mysql']  = get_ds_mysql(db_ip, db_port, db_service, db_user, db_pass)
            config['db_mysql_dict'] = get_ds_mysql_dict(db_ip, db_port, db_service, db_user, db_pass)
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
    print_dict(config)
    return config

def get_monitor_indexes(config):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute("SELECT * FROM t_monitor_index where id=1 and status='1' order by id ")
    rs = cr.fetchall()
    cr.close()
    return rs

def get_gather_tasks(config):
    db = config['db_mysql_dict']
    cr = db.cursor()
    cr.execute("SELECT * FROM `t_monitor_task` WHERE STATUS='1' AND task_tag LIKE 'gather%' ORDER BY id")
    rs = cr.fetchall()
    cr.close()
    return rs

def monitor(config):
   for idx in get_monitor_indexes(config):
       print(idx)

   for idx in get_gather_tasks(config):
        print(idx)

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
    monitor(config)

if __name__ == "__main__":
     main()
