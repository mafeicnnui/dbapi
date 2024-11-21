#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/6/8 11:10
# @Author : 马飞
# @File : webchat_service.py.py
# @Software: PyCharm
# @Function: EaseBase服务器监控微信推送

import sys
import warnings
import requests
import traceback
import pymysql
import smtplib
import datetime
from email.mime.text import MIMEText
import json

ALERT_TITLE = '停简单接口日志告警'
ALERT_MESSAGE = '''项目名称：{}
采集主机：{}
接口地址：{}
失败时间：{}
失败时长：{}
告警阀值：3分钟
失败原因：未收到接口日志！
'''

RECOVER_TITLE = "停简单接口日志恢复通知"
RECOVER_MESSAGE = '''项目名称：{}
采集主机：{}
接口地址：{}
响应内容：接口日志恢复正常!
恢复时间：{}'''


'''
  功能：全局配置
'''
config = {
    "chat_interface":"https://api.hopsontong.com/wxcp/message/template/send",
    "mysql":"10.2.39.18:3306:puppet:puppet:Puppet@123",
    "warn_level":"紧急"
}

'''
  功能：获取mysql连接，以元组返回
'''
def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',autocommit=True)
    return conn

'''
  功能：获取mysql连接，以字典返回
'''
def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8',cursorclass = pymysql.cursors.DictCursor,autocommit=True)
    return conn


'''
 功能：获取数据库连接
'''
def get_config(config):
    db_ip                   = config['mysql'].split(':')[0]
    db_port                 = config['mysql'].split(':')[1]
    db_service              = config['mysql'].split(':')[2]
    db_user                 = config['mysql'].split(':')[3]
    db_pass                 = config['mysql'].split(':')[4]
    config['db_mysql']      = get_ds_mysql(db_ip,db_port,db_service,db_user,db_pass)
    config['db_mysql_dict'] = get_ds_mysql_dict(db_ip, db_port, db_service, db_user, db_pass)
    return config

'''
  功能：调用接口发送消息
'''
def send_message(config,toUser,title,message):
    msg = {
        "title":title,
        "toUser":toUser,
        "description":message,
        "url":"ops.zhitbar.cn",
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
        r = requests.post(config['chat_interface'], data=json.dumps(msg),headers=headers)
        print(r.text)
    except:
        print(traceback.print_exc())

'''
  功能：发送邮件
  send_mail465('190343@lifeat.cn', 'Hhc5HBtAuYTPGHQ8', '190343@lifeat.cn', v_title, v_content)
'''
def send_mail465(p_send_server,p_sender_port,p_from_user,p_from_pass,p_to_user,p_title,p_content):
    print('send_mail465>>>',p_send_server,p_sender_port,p_from_user,p_from_pass,p_to_user,p_title)
    to_user=p_to_user.split(",")
    try:
        msg            = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server         = smtplib.SMTP_SSL(p_send_server, int(p_sender_port))
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def print_dict(p_cfg):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in p_cfg:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',p_cfg[key])
    print('-'.ljust(85,'-'))

'''
 功能：获取server信息
'''
def get_server_info(config,server_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = 'SELECT * FROM t_server where id={}'.format(server_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs

def get_api_interface(market_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = "SELECT index_threshold FROM t_monitor_index WHERE index_code='tjd_api_{}_1'".format(market_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs['index_threshold']


'''
 功能：写告警日志
'''
def write_warn_log(config,server_id,flag):
    db = config['db_mysql_dict']
    cr = db.cursor()
    if flag == 'failure':
        st ='''select count(0) as rec from t_monitor_api_warn_log where server_id={}'''.format(server_id)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec']==0:
            st = '''insert into t_monitor_api_warn_log(
                      server_id,server_desc,fail_times,succ_times,create_time,first_failure_time,is_send_rcv_mail,is_send_alt_mail) 
                     values({},'{}',{},{},'{}','{}','{}','{}')
                 '''.format(server_id,
                            get_server_info(config,server_id)['server_desc'],
                            1,
                            0,
                            get_time(),
                            get_time(),
                            'N',
                            'N'
                            )
        else:
            st = '''update  t_monitor_api_warn_log 
                       set fail_times=fail_times+1,
                           first_failure_time=case when first_failure_time is null  then now() else first_failure_time end,
                           succ_times = 0,
                           update_time=now() 
                      where server_id={}
                 '''.format(server_id)
        print('write_warn_log=>failure=',st)
        cr.execute(st)
        db.commit()

    if flag =='success':
        st ='''select count(0) as rec from t_monitor_api_warn_log 
                 where server_id={} '''.format(server_id)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_api_warn_log 
                               set succ_times=succ_times+1,
                                   fail_times=0,
                                   is_send_rcv_mail='N',
                                   update_time=now() ,
                                   first_failure_time=null,
                                   is_send_alt_mail_times=0
                              where server_id={}'''.format(server_id)
            print('write_warn_log=>success=', st)
            cr.execute(st)
            db.commit()

    if flag =='recover':
        st ='''select count(0) as rec from t_monitor_api_warn_log 
                  where server_id={} and succ_times=1 '''.format(server_id)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_api_warn_log 
                       set is_send_rcv_mail='Y',
                           is_send_alt_mail='N',
                           fail_times  = 0,
                           first_failure_time=null,
                           is_send_alt_mail_times=0,
                           update_time = now() 
                      where server_id = {}
                         '''.format(server_id)
            print('write_warn_log=>recover=', st)
            cr.execute(st)
            db.commit()

    if flag == 'warning':
        st = '''select count(0) as rec from t_monitor_api_warn_log 
                  where server_id={}'''.format(server_id)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_api_warn_log 
                       set is_send_alt_mail='Y',
                           is_send_alt_mail_times=is_send_alt_mail_times+1,
                           update_time = now() 
                      where server_id = {}'''.format(server_id)
            print('write_warn_log=>warning=', st)
            cr.execute(st)
            db.commit()

'''
 功能：统计某个服务失败次数
'''
def get_warn_info(config,server_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = '''select * from t_monitor_api_warn_log where server_id={}'''.format(server_id)
    cr.execute(st)
    rs = cr.fetchone()
    return rs

'''
 功能：获取某个指标阀值
'''
def get_index_threshold(config,p_index_code):
    try:
        db = config['db_mysql_dict']
        cr = db.cursor()
        st = "SELECT index_threshold*100 as index_threshold FROM t_monitor_index WHERE index_code='{}'".format(p_index_code)
        cr.execute(st)
        rs = cr.fetchone()
        return float(rs['index_threshold'])
    except:
        return 0

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def str2datetime(p_rq):
    return datetime.datetime.strptime(p_rq, '%Y-%m-%d %H:%M:%S')

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_market_mame(p_market_id):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    cr.execute("select dmmc from t_dmmx where dm='05' and dmm='{}'".format(p_market_id))
    rs = cr.fetchone()
    cr.close()
    return rs['dmmc']

def get_server_desc(p_server_id):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    cr.execute("select server_desc2 from t_server where  id='{}'".format(p_server_id))
    rs = cr.fetchone()
    cr.close()
    return rs['server_desc2']

def get_readable_time(seconds):
    m,s = divmod(seconds,60)
    h,m = divmod(m,60)
    return '{}时{}分{}秒'.format(h,m,s)

def server_warning(config):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    '''发送异常邮件或微信'''
    cr.execute('''SELECT 
                    a.server_id,
                    a.market_id,
                    c.dmmc AS market_name,
                    b.server_desc2 AS server_desc,
                    api_interface,
                    CASE WHEN TIMESTAMPDIFF(MINUTE,a.update_time,NOW())>=3 THEN '500' 
                         ELSE '200' END api_status,
                    response_time,
                    DATE_FORMAT(a.update_time,'%Y-%m-%d %H:%i:%s') AS update_time,
                    (SELECT dmmc FROM t_dmmx WHERE dm='48' AND dmm=a.market_id) AS request_body                    
                FROM t_monitor_api_log a,t_server b ,t_dmmx c
                WHERE a.server_id=b.id AND a.market_id=c.dmm AND c.dm='05'
                AND a.id IN(SELECT MAX(id) FROM t_monitor_api_log GROUP BY market_id,server_id,api_interface)
                -- and a.market_id=306
                ORDER BY a.id DESC''')
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        if r['api_status'] != '200':
            write_warn_log(cfg, r['server_id'], 'failure')
            warn_info = get_warn_info(cfg, r['server_id'])
            print('warn_info>>>:',warn_info)
            if (warn_info['is_send_alt_mail'] == 'N' and  warn_info['fail_times'] > 3  or \
                  warn_info['is_send_alt_mail'] == 'Y' and  warn_info['fail_times'] % 60 == 0) \
                    and warn_info['is_send_alt_mail_times'] <=5:
               write_warn_log(cfg, r['server_id'], 'warning')

               MESSAGE = ALERT_MESSAGE. \
                   format('{}({})'.format(get_market_mame(server_info['market_id']),r['market_id']),
                          get_server_desc(r['server_id']),
                          r['api_interface'],
                          warn_info['first_failure_time'],
                          get_readable_time(get_seconds(warn_info['first_failure_time']))
                          )

               send_message(cfg,'190343',ALERT_TITLE,MESSAGE)
               send_mail465(cfg.get('send_server'),
                            cfg.get('send_port'),
                            cfg.get('sender'),
                            cfg.get('sendpass'),
                            '190343@lifeat.cn',
                            ALERT_TITLE,
                            MESSAGE)
        else:
            # 写恢复告警日志
            write_warn_log(cfg, r['server_id'],'success')

    '''发送异常恢复邮件或微信'''
    cr.execute("""SELECT a.* FROM t_monitor_api_warn_log a,t_server b
                   where a.server_id=b.id 
                       AND a.succ_times=1 
                          AND a.is_send_alt_mail ='Y'
                            AND a.is_send_rcv_mail='N' order by server_id""")
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        write_warn_log(cfg, r['server_id'], 'recover')

        MESSAGE = RECOVER_MESSAGE. \
            format('{}({})'.format(get_market_mame(server_info['market_id']),server_info['market_id']),
                   get_server_desc(r['server_id']),
                   get_api_interface(server_info['market_id']),
                   r['update_time'])


        send_message(cfg,'190343',RECOVER_TITLE,MESSAGE)
        send_mail465(cfg.get('send_server'),
                     cfg.get('send_port'),
                     cfg.get('sender'),
                     cfg.get('sendpass'),
                     '190343@lifeat.cn',
                     RECOVER_TITLE,
                     MESSAGE)

    '''关送数据库连接'''
    cfg['db_mysql'].close()
    cfg['db_mysql_dict'].close()


def check_alert_config(p_tag):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    st = "select count(0) as rec from t_alert_task where task_tag='{0}'".format(p_tag)
    print(st)
    cr.execute(st)
    rs = cr.fetchone()
    return rs['rec']

def check_server_alert_status(p_tag):
    cfg = get_config(config)
    db  = cfg['db_mysql_dict']
    cr  = db.cursor()
    st = "select count(0) as rec from t_alert_task a,t_server b \
                  where a.server_id=b.id and a.task_tag='{0}' and b.status='0'".format(p_tag)
    cr.execute(st)
    rs = cr.fetchone()
    return rs['rec']

def get_itmes_from_monitor_templete(p_templete):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    st = "SELECT index_code,index_threshold FROM t_monitor_index \
              WHERE id IN(SELECT index_id FROM `t_monitor_templete_index` \
                           WHERE INSTR('{0}',templete_id)>0) AND STATUS='1'".format(p_templete)
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_db_alert_config(p_tag):
    cfg = get_config(config)
    print('cfg=',cfg)
    db = cfg['db_mysql_dict']
    cr = db.cursor()

    if check_server_alert_status(p_tag)>0:
       print('告警服务器已禁用!')
       sys.exit(0)

    if  check_alert_config(p_tag)==0:
       print('告警标识不存在!')
       sys.exit(0)

    st = '''SELECT a.task_tag,a.comments,a.templete_id,
                   a.server_id,a.run_time,
                   a.python3_home,a.api_server,a.script_path,
                   a.script_file,a.status,b.server_ip,
                   b.server_port,b.server_user,b.server_pass,
                   b.server_desc, b.market_id,
                   (select `value` from t_sys_settings where `key`='send_server') as send_server,
                   (select `value` from t_sys_settings where `key`='send_port') as send_port,
                   (select `value` from t_sys_settings where `key`='sender') as sender,
                   (select `value` from t_sys_settings where `key`='sendpass') as sendpass
        FROM t_alert_task a JOIN t_server b ON a.server_id=b.id 
        where a.task_tag ='{0}' ORDER BY a.id'''.format(p_tag)

    cr.execute(st)
    rs = cr.fetchone()
    cfg.update(rs)
    return cfg

if __name__ == "__main__":
    tag = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    cfg = get_db_alert_config(tag)

    print_dict(cfg)

    # 服务器是否可用告警
    server_warning(cfg)
