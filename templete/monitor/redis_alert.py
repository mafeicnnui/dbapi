import os
import sys
import json
import psutil
import pymysql
import traceback
import requests
import datetime
import warnings
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)

MYSQL_SETTINGS = {
    "host": '10.2.39.18',
    "port": 3306,
    "user": "puppet",
    "passwd": "Puppet@123",
    "db": 'puppet'
}

SEND_USER='190343'
ALERT_TITLE = 'REDIS慢日志告警'
ALERT_MESSAGE = '''实例名称：{}
实例地址：{}
预警阀值：{}ms
日志数量：{}个
告警时间：{}'''

def get_db():
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',autocommit=True)
    return conn

def get_db_dict():
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)
    return conn

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_db_info(p_dbid):
    cr = get_db_dict().cursor()
    cr.execute("select * from t_db_source where id='{}'".format(p_dbid))
    rs = cr.fetchone()
    return rs

def send_message(toUser,title,message,batch_id):
    msg = {
        "title":title,
        "toUser":toUser,
        "description":message,
        "url":"ops.zhitbar.cn:59521/monitor/redis/slowlog?batch_id={}".format(batch_id),
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

def get_max_batch_id():
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT MAX(a.batch_id) as batch_id 
              FROM t_monitor_redis_log a
                where a.create_time between DATE_SUB(NOW(), INTERVAL 10 MINUTE) and now()
                  and  a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b WHERE a.dbid=b.dbid)"""
    cr.execute(st)
    rs=cr.fetchone()
    return rs['batch_id']

def get_dbid(batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = 'SELECT dbid FROM t_monitor_redis_hz_log where batch_id={} limit 1'.format(batch_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs['dbid']

def get_hz_info(batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT count(0) as slowlog_number 
            FROM t_monitor_redis_log a
            where a.batch_id={}
             AND  a.create_time BETWEEN DATE_SUB(NOW(), INTERVAL 10 MINUTE) AND NOW()
             AND  a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b WHERE a.dbid=b.dbid)            
            """.format(batch_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs

def get_index_threshold(batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT index_threshold FROM t_monitor_index \
          WHERE index_code =(SELECT index_code FROM t_monitor_redis_log WHERE batch_id ={} LIMIT 1)""".format(batch_id)
    cr.execute(st)
    rs = cr.fetchone()
    return rs['index_threshold']

def check_slowlog():
    db = get_db()
    cr = db.cursor()
    st = """SELECT count(0) FROM t_monitor_redis_log a
             WHERE a.create_time between DATE_SUB(NOW(), INTERVAL 10 MINUTE) and now()
               and a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b WHERE a.dbid=b.dbid)"""
    cr.execute(st)
    rs = cr.fetchone()
    if rs[0]>0:
        return True
    else:
        return False


def monitor():
    if check_slowlog():
        db = get_db_dict()
        cr = db.cursor()
        batch_id = get_max_batch_id()
        dbid= get_dbid(batch_id)
        dbinfo = get_db_info(dbid)
        # st ='SELECT * FROM t_monitor_redis_hz_log where batch_id={}'.format(batch_id)
        # cr.execute(st)
        # rs_hz = cr.fetchall()
        # st = 'SELECT * FROM t_monitor_redis_log where batch_id={}'.format(batch_id)
        # cr.execute(st)
        # rs_mx = cr.fetchall()
        # print('t_monitor_redis_hz_log...')
        # for h in rs_hz:
        #     print(h)
        # print('t_monitor_redis_log...')
        # for m in rs_mx:
        #     print(m)
        message = ALERT_MESSAGE. \
            format(dbinfo['db_desc'],
                   dbinfo['ip']+':'+str(dbinfo['port']),
                   get_index_threshold(batch_id),
                   get_hz_info(batch_id)['slowlog_number'],
                   get_time())
        print(message)
        send_message(SEND_USER, ALERT_TITLE, message,batch_id)
        # 发送邮件
    else:
        print('未找到10分钟内的慢日志!')

def main():
    monitor()


if __name__ == '__main__':
    main()
