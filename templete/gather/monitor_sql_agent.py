#!/usr/bin/env python3

import sys
import datetime
import warnings
import json
import pymysql
import requests
import traceback

ALERT_MESSAGE = '''监控项目：{}
监控结果：{}
告警时间：{}'''

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def send_message(toUser,title,message):
    msg = {
        "title":title,
        "toUser":toUser,
        "description":message,
        "url":'ops.zhitbar.cn:59521',
        "msgType":"textcard",
        "agentId":1000093
    }

    headers = {
        "User-Agent":"PostmanRuntime/7.26.8",
        "Accept":"*/*",
        "Accept-Encoding":"gzip, deflate, br",
        "Connection":"keep-alive",
        "Content-Type": "application/json"
    }

    try:
        print('send_message>>:',msg)
        chat_interface = "https://api.hopsontone.com/wxcp/message/template/send"
        r = requests.post(chat_interface, data=json.dumps(msg),headers=headers)
        print(r.text)
    except:
        print(traceback.print_exc())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8', cursorclass = pymysql.cursors.DictCursor)
    return conn

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par,timeout=1).json()
        if res['code'] == 200:
            print('接口read_db_decrypt 调用成功!')
            config = res['msg']
            return config
        else:
            print('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        print('aes_decrypt api not available!')

def get_config_from_db(tag):
    par = {'tag': tag}
    url = 'http://$$API_SERVER$$/read_config_monitor'
    res = requests.post(url, data=par, timeout=1).json()
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
            traceback.print_exc()
            exit(0)
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        sys.exit(0)

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

def get_results(cfg,sql):
    db = cfg['db_mysql']
    cr = db.cursor()
    cr.execute(sql)
    rs = cr.fetchall()
    v=''
    for r in rs:
        v=v+'{} {} {}\n'.format(r[0],r[1],r[2])
    return v[0:-1] if len(rs)!=0 else '无数据'

def monitor(cfg):
   for idx in cfg['templete_indexes_values']:
      MESSAGE = ALERT_MESSAGE. \
          format(idx['index_name'],
                 get_results(cfg,idx['index_threshold']),
                 get_time())
      send_message(cfg['receiver'], cfg['templete_name'], MESSAGE)

def main():
    config = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]

    #初始化
    config=init(config)

    #数据同步
    monitor(config)

if __name__ == "__main__":
     main()
