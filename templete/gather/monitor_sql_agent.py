#!/usr/bin/env python3

import sys
import datetime
import warnings
import json
import pymysql
import requests
import traceback

ALERT_MESSAGE = '''监控项目：{}
项目描述：{}
监控时间：{}
监控结果：{}'''

REPORT_MESSAGE = '''报表名称：{}
统计时间：{}
执行时长：{}秒
执行结果：{}'''

REPORT_MESSAGE_FAILURE = '''报表名称：{}
统计时间：{}
执行时长：{}秒
执行结果：{}
错误消息：{}'''

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

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

def get_row_info(row):
    s=''
    for r in row:
        s=s+str(r)+' | '
    return s[0:-2]

def get_col_info(desc):
    s = ''
    for r in desc:
        s = s + str(r[0]) + ' | '
    return s[0:-2]

def get_results(cfg,sql):
    db = cfg['db_mysql']
    cr = db.cursor()
    cr.execute(sql)
    desc = get_col_info(cr.description)
    rs = cr.fetchall()
    v = ''
    i = 0
    s = ' '*17
    for r in rs:
        print('s=',get_row_info(r))
        if i == 0:
           v=v+'{}\n'.format(get_row_info(r))
        else:
           v = v + s + '{}\n'.format(get_row_info(r))
        print('i=',i,'v=', v)
        i=i+1
    message = v[0:-3] if len(rs)!=0 else '无数据'
    return desc,message


def call_proc(cfg,sql):
    start_time = datetime.datetime.now()
    try:
        db = cfg['db_mysql']
        cr = db.cursor()
        cr.callproc(sql)
        print("call proc `{}` ok".format(sql))
        return get_seconds(start_time),{'status':True,'message':'成功'}
    except:
        return 0,{'status':False,'message':traceback.format_exc()}


def monitor(cfg):
   for idx in cfg['templete_indexes_values']:
      if idx['index_type'] == '6':
         elaspse,result = call_proc(cfg,idx['index_threshold'])
         if result['status'] == True:
             MESSAGE = REPORT_MESSAGE. \
                  format(idx['index_name'],
                         get_time(),
                         str(elaspse),
                         result['message'])
             send_message(cfg['receiver'], cfg['templete_name'], MESSAGE)
         else:
             MESSAGE = REPORT_MESSAGE_FAILURE. \
                 format(idx['index_name'],
                        get_time(),
                        str(elaspse),
                        '失败',
                        result['message'])
             send_message(cfg['receiver'], cfg['templete_name'], MESSAGE)
      else:
          desc, message = get_results(cfg,idx['index_threshold'])
          MESSAGE = ALERT_MESSAGE. \
              format(idx['index_name'],
                     desc,
                     get_time(),
                     message)
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
