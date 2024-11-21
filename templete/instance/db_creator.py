#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/8/24 10:55
# @Author : 马飞
# @File : db_creator.py.py
# @Software: PyCharm

from email.mime.text import MIMEText
import urllib.parse
import urllib.request
import json
import smtplib
import traceback
import pymysql
import warnings
import sys
import ssl
import os
import datetime
import time

def get_day():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_time():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%Y-%m-%d %H:%M:%S')
    return time1_str

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(30,' ')+'=',config[key])
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

def get_config_from_db(inst_id):
    values = {
        'inst_id': inst_id
    }
    print('values=', values)
    url = 'http://$$API_SERVER$$/read_db_inst_config'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    print('data=', data)
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        print('接口调用成功!')
        config = res['msg']
        config['dpath']   = config['dpath'][0]['mysql_download_url']
        config['dfile']   = config['dpath'].split('/')[-1]
        config['dver']    = config['dfile'].split('-')[1]
        config['lpath']   = config['dfile'].replace('.tar.gz', '')
        config['db_pass'] = aes_decrypt(config['db_pass'], config['db_user'])
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        return None

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

def del_config(config):
    config_mysqld = '[mysqld]\n'
    config_mysql  = '[mysql]\n'
    config_client = '[client]\n'
    parameter     = {}

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
            else:
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'],c['value'])
                parameter[c['value'].split('=')[0]] = c['value'].split('=')[1]

        elif c['TYPE'] == 'mysql':
            config_mysql  = config_mysql + '#{}\n{}\n'.format(c['name'], c['value'])
            parameter[c['value'].split('=')[0]] = c['VALUE'].split('=')[1]
        elif c['TYPE'] == 'client':
            if c['value'].split('=')[0] == 'socket' :
                n_val = c['value'].format(config['dver'],config['db_port'])
                config_mysqld = config_mysqld + '#{}\n{}\n'.format(c['name'], n_val)
                parameter[n_val.split('=')[0]] = n_val.split('=')[1]
            else:
                config_client = config_client + '#{}\n{}\n'.format(c['name'], c['value'])
                parameter[c['value'].split('=')[0]] = c['value'].split('=')[1]
        else:
            pass

    filename = '/etc/mysql_{}.cnf'.format(config['db_port'])
    config['cfile'] = filename
    os.system('sudo rm -f {}'.format(filename))
    return parameter

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
            elif c['value'].split('=')[0] == 'slow_query_log_file':
                n_val = c['value'].format(parameter['datadir']).replace('YYYYMMDD', get_day())
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
    filename    = 'mysql_{}.cnf'.format(config['db_port'])
    fullname    = '/tmp/{}'.format(filename)
    with open(fullname, 'w') as f:
       f.write(config_file)
    os.system('sudo cp {} /etc/'.format(fullname))
    config['cfile']  = '/etc/'+filename
    return parameter

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

def update_inst_status(item,status):
    try:
        v_tag = {
            'inst_id'   : item.get('inst_id'),
            'status'    : status
        }
        v_msg = json.dumps(v_tag)
        values = {
            'tag': v_msg
        }
        url = 'http://$$API_SERVER$$/update_db_inst_status'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        if res['code'] != 200:
            print('Interface update_db_inst_status call failed!',res['msg'])
        else:
            print('Interface update_db_inst_status call success!')
    except:
        print(traceback.print_exc())

def update_inst_reboot_status(item,status):
    try:
        v_tag = {
            'inst_id'   : item.get('inst_id'),
            'reboot_status'    : status
        }
        v_msg = json.dumps(v_tag)
        values = {
            'tag': v_msg
        }
        print('update_inst_reboot_status=',values)
        url = 'http://$$API_SERVER$$/update_db_inst_reboot_status'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        if res['code'] != 200:
            print('Interface update_inst_reboot_status call failed!',res['msg'])
        else:
            print('Interface update_inst_reboot_status call success!')
    except:
        print(traceback.print_exc())

def creator(config):
    # write config file
    parameter = write_config(config)
    print_dict(config)
    print_dict(parameter)
    write_inst_log(config,'生成mysql配置文件:{}'.format(config['cfile']))

    # create db
    print('creating mysql instance....')
    for s in config['step_create']:
        if s['id'] == 1:
           s['cmd'] = s['cmd'].format(config['dfile'],config['dpath'])
        elif s['id'] == 2:
           s['cmd'] = s['cmd'].format(config['dfile'],parameter['basedir'],config['lpath'],config['lpath'],parameter['basedir'])
        elif s['id'] in(3,):
           pass
        elif s['id'] in(4,):
           s['cmd'] = s['cmd'].format(parameter['datadir'])
        elif s['id'] in(5,7):
           s['cmd'] = s['cmd'].format(parameter['datadir'])
        elif s['id'] == 6:
           s['cmd'] = s['cmd'].format(parameter['basedir'],parameter['basedir'],parameter['datadir'], parameter['user'],config['cfile'])
        elif s['id'] == 8:
           s['cmd'] = s['cmd'].format(parameter['basedir'],config['cfile'])
        elif s['id'] in (9,):
           s['cmd'] = s['cmd'].format(parameter['basedir'],parameter['port'],parameter['socket'],config['db_pass'])
        elif s['id'] in (10,11):
            s['cmd'] = s['cmd'].format(parameter['basedir'], config['db_pass'], parameter['port'], parameter['socket'],config['db_pass'])
        elif s['id'] in (12,):
           s['cmd'] = s['cmd'].format(parameter['basedir'],config['db_pass'],parameter['port'],parameter['socket'])
        elif s['id'] in (13,):
           s['cmd'] = s['cmd'].format(config['lpath'])
        elif s['id'] in (14,):
           s['cmd'] = s['cmd'].format(parameter['basedir'])
        else:
           pass
        try:
            print('{} Execute:{}'.format(get_time(),s['cmd']))
            if s['id'] == 6:
                print('{} Execute:{}', get_time(), 'Waiting database initilize...')
                os.system(s['cmd'])
                print('{} Execute:{}', get_time(), 'Waiting database initilize...ok')
            elif s['id'] == 8:
               print('{} Execute:{}', get_time(), 'Waiting database startup...')
               os.system(s['cmd'])
               time.sleep(3)
               print('{} Execute:{}', get_time(), 'Waiting database startup...ok')
            else:
               os.system(s['cmd'])
            write_inst_log(config, s['message'] +':'+ s['cmd'])
        except:
            write_inst_log(config, s['message']+'发生错误!')
            print(traceback.print_exc())
    update_inst_status(config, '2')
    write_inst_log(config, '实例[{}]状态已更改为已创建'.format(config['inst_name']))

def destroy(config):
    # write config file
    parameter = del_config(config)
    print_dict(config)
    write_inst_log(config,'删除mysql配置文件:/etc/{}'.format(config['cfile']))

    # destroy db
    print('creating mysql instance....')
    for s in config['step_destroy']:
        if s['id']   == 20:
            s['cmd'] = s['cmd'].format(parameter['basedir'], config['db_pass'], parameter['port'], parameter['socket'])
        elif s['id'] == 21:
           s['cmd'] = s['cmd'].format(parameter['datadir'],parameter['datadir'])
        elif s['id'] == 22:
            s['cmd'] = s['cmd'].format(parameter['basedir'], parameter['basedir'])
        else:
           pass
        try:
            print('{} Execute:{}'.format(get_time(), s['cmd']))
            os.system(s['cmd'])
            write_inst_log(config, s['message'] +':'+ s['cmd'])
        except:
            write_inst_log(config, s['message']+'发生错误!')
            print(traceback.print_exc())
    update_inst_status(config, '5')
    write_inst_log(config, '实例[{}]状态已更改为已销毁'.format(config['inst_name']))

def get_ds_mysql_test(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',read_timeout=3)
    return conn

def set_db_status(config):
    try:
        db_ip      = config['db_ip']
        db_port    = config['db_port']
        db_service = ''
        db_user    = config['db_user']
        db_pass    = config['db_pass']
        db         = get_ds_mysql_test(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        cr.execute("SELECT 1")
        db.commit()
        cr.close()
        update_inst_status(config,'3')
    except Exception as e:
        print('mysql_connect exception!')
        if config['inst_status']!='5':
           update_inst_status(config,'4')

def check_inst_reboot(config):
    try:
        r = os.popen('cat /etc/rc.d/rc.local | grep {} | wc -l'.format(config['inst_id'])).read()
        return int(r.split('\n')[0])
    except:
        return 0

def set_db_reboot_status(config):
    if check_inst_reboot(config)>0:
        update_inst_reboot_status(config,'Y')
    else:
        update_inst_reboot_status(config,'N')

def startup(config):
    parameter = write_config(config)
    print_dict(config)
    write_inst_log(config, '生成mysql配置文件:/etc/{}'.format(config['cfile']))
    print('starting mysql instance....')
    for s in config['step_start']:
       try:
            s['cmd'] = s['cmd'].format(parameter['basedir'], config['cfile'])
            print('{} Execute:{}'.format(get_time(), s['cmd']))
            os.system(s['cmd'])
            write_inst_log(config, s['message'] + ':' + s['cmd'])
            update_inst_status(config, '3')
            write_inst_log(config, '实例[{}]状态已更改为已启动'.format(config['inst_name']))
       except:
            write_inst_log(config, s['message'] + '时发生错误!')
            traceback.print_exc()


def stop(config):
    # write config file
    parameter = write_config(config)
    print_dict(config)
    write_inst_log(config, '生成mysql配置文件:/etc/{}'.format(config['cfile']))
    print('stopping mysql instance....')
    for s in config['step_stop']:
       try:
            s['cmd'] = s['cmd'].format(parameter['basedir'], config['db_pass'], parameter['port'],parameter['socket'])
            print('{} Execute:{}'.format(get_time(), s['cmd']))
            os.system(s['cmd'])
            write_inst_log(config, s['message'] + ':' + s['cmd'])
            update_inst_status(config, '4')
            write_inst_log(config, '实例[{}]状态已更改为已停止'.format(config['inst_name']))
       except:
            write_inst_log(config, s['message'] + '时发生错误!')
            print(traceback.print_exc())

def restart(config):
    stop(config)
    startup(config)
    
def autostart(config):
    # write config file
    parameter = write_config(config)
    print_dict(config)
    write_inst_log(config, '生成mysql配置文件:/etc/{}'.format(config['cfile']))
    print('Setting mysql auotstart task....')
    for s in config['step_auostart']:
        try:
            if s['id'] == 50:
                s['cmd'] = s['cmd'].format(config['inst_id'])
            elif s['id'] == 51:
                s['cmd'] = s['cmd'].format('#' + config['inst_name'] + '自启动脚本 inst_id='+config['inst_id'])
            elif s['id'] == 52:
                s['cmd'] = s['cmd'].format(parameter['user'],parameter['basedir'], config['cfile'],config['inst_id'])
            else:
                pass

            print('{} Execute:{}'.format(get_time(), s['cmd']))
            os.system(s['cmd'])
            write_inst_log(config, s['message'] + ':' + s['cmd'])
            set_db_reboot_status(config)
            write_inst_log(config, '实例[{}]已设置为已自启动'.format(config['inst_name']))
        except:
            write_inst_log(config, s['message'] + '时发生错误!')
            print(traceback.print_exc())

def  cancel_autostart(config):
    # write config file
    parameter = write_config(config)
    print_dict(config)
    write_inst_log(config, '生成mysql配置文件:/etc/{}'.format(config['cfile']))
    print('Setting mysql auotstart task....')
    for s in config['step_cancel_auostart']:
        try:
            if s['id'] == 60:
                s['cmd'] = s['cmd'].format(config['inst_id'])

            print('{} Execute:{}'.format(get_time(), s['cmd']))
            os.system(s['cmd'])
            write_inst_log(config, s['message'] + ':' + s['cmd'])
            set_db_reboot_status(config)
            write_inst_log(config, '实例[{}]已取消自启动'.format(config['inst_name']))
        except:
            write_inst_log(config, s['message'] + '时发生错误!')
            print(traceback.print_exc())

def main():
    config = ""
    mode   = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if  sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-mode":
            mode = sys.argv[p + 1]
        else:
            pass
    # init
    config = init(config)
    config['mode'] = mode

    # create or destroy db
    if mode =="" or mode not in ('create','destroy','status','start','stop','restart','autostart','cancel_autostart'):
       print('Usage ./db_creator.py -tag inst_id -mode create|destroy|status|start|stop|restart|autostart|cancel_autostart')
       sys.exit(0)
    elif mode == "create":
       creator(config)
    elif mode == "destroy":
       destroy(config)
    elif mode == "status":
       set_db_status(config)
    elif mode == "start":
       startup(config)
    elif mode == "stop":
       stop(config)
    elif mode == "autostart":
        autostart(config)
    elif mode == "cancel_autostart":
        cancel_autostart(config)
    elif mode == "restart":
       restart(config)
    else:
       pass

if __name__ == "__main__":
     main()
