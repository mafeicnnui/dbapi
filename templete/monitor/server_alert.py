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

'''
  功能：全局配置
'''
config = {
    "chat_interface":"https://api.hopsontone.com/wxcp/message/template/send",
    "mysql":"10.2.39.59:3306:puppet:puppet:Puppet@123",
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

    # msg = {
    #     'title': '服务器告警测试',
    #     'toUser': '190343|190205',
    #     'description': '商管BI系统CPU使用率为97%',
    #     'url': ' ',
    #     'msgType': 'textcard',
    #     'agentId': '1000093'
    # }

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

'''
 功能：获取db信息
'''
def get_db_info(config,db_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = "SELECT a.*,b.dmmc as db_type_name FROM t_db_source a,t_dmmx b where a.db_type=b.dmm and b.dm='02' and a.id={}".format(db_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs

'''
 功能：获取项目收件人
'''
def get_proj_recever(config,market_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = "SELECT dmmc FROM t_dmmx WHERE dm='45' AND dmm='{}'".format(market_id)
    cr.execute(st)
    rs=cr.fetchone()
    return rs['dmmc']

'''
 功能：获取项目收件人微信
'''
def get_proj_recever_wx(config,market_id):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = "SELECT dmmc FROM t_dmmx WHERE dm='45' AND dmm='{}'".format(market_id)
    cr.execute(st)
    rs=cr.fetchone()
    return '|'.join([i.split('@')[0] for i in rs['dmmc'].split(',')])

'''
 功能：写告警日志
'''
def write_warn_log(config,server_id,index_code,index_name,index_value,flag):
    db = config['db_mysql_dict']
    cr = db.cursor()
    if flag == 'failure':
        st ='''select count(0) as rec from t_monitor_server_warn_log where server_id={} and index_code='{}' '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec']==0:
            st = '''insert into t_monitor_server_warn_log(
                      server_id,server_desc,fail_times,succ_times,create_time,first_failure_time,is_send_rcv_mail,is_send_alt_mail,index_code,index_name,index_value) 
                     values({},'{}',{},{},'{}','{}',
                           '{}','{}','{}','{}','{}')
                 '''.format(server_id,
                            get_server_info(config,server_id)['server_desc'],
                            1,
                            0,
                            get_time(),
                            get_time(),
                            'N',
                            'N',
                            index_code,
                            index_name,
                            index_value
                            )
        else:
            st = '''update  t_monitor_server_warn_log 
                       set 
                          index_value={},
                          fail_times=fail_times+1,
                          first_failure_time=case when first_failure_time is null  then now() else first_failure_time end,
                          succ_times = 0,
                          update_time=now() 
                      where server_id={} and index_code='{}'
                 '''.format(index_value,server_id,index_code)
        print('write_warn_log=>failure=',st)
        cr.execute(st)
        db.commit()

    if flag =='success':
        st ='''select count(0) as rec from t_monitor_server_warn_log 
                 where server_id={} and index_code='{}' '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_server_warn_log 
                               set 
                                  index_value={},
                                  succ_times=succ_times+1,
                                  fail_times=0,
                                  is_send_rcv_mail='N',
                                  update_time=now() ,
                                  first_failure_time=null,
                                  is_send_alt_mail_times=0
                              where server_id={} and index_code='{}'
                         '''.format(index_value,server_id,index_code)
            print('write_warn_log=>success=', st)
            cr.execute(st)
            db.commit()

    if flag =='recover':
        st ='''select count(0) as rec from t_monitor_server_warn_log 
                  where server_id={} and index_code='{}' and succ_times=1 '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_server_warn_log 
                       set 
                          is_send_rcv_mail='Y',
                          is_send_alt_mail='N',
                          fail_times  = 0,
                          first_failure_time=null,
                          is_send_alt_mail_times=0,
                          update_time = now() 
                      where server_id = {} and index_code='{}'
                         '''.format(server_id,index_code)
            print('write_warn_log=>recover=', st)
            cr.execute(st)
            db.commit()

    if flag == 'warning':
        st = '''select count(0) as rec from t_monitor_server_warn_log 
                  where server_id={} and index_code='{}' '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_server_warn_log 
                       set 
                          is_send_alt_mail='Y',
                          is_send_alt_mail_times=is_send_alt_mail_times+1,
                          update_time = now() 
                      where server_id = {} and index_code='{}'
                         '''.format(server_id, index_code)
            print('write_warn_log=>warning=', st)
            cr.execute(st)
            db.commit()


'''
 功能：统计某个服务失败次数
'''
def get_warn_info(config,server_id,index_code):
    db = config['db_mysql_dict']
    cr = db.cursor()
    st = '''select * from t_monitor_server_warn_log where server_id={} and index_code='{}' '''.format(server_id,index_code)
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

'''
   接收参数：从系统字典获取：发件服务器、发件人、发件人密码
            从项目配置中获取：不同项目收件人，项目ID从通过服务器信息获取
            第一次失败后达至10分钟以上，如果失败次数为3开始报警，之后每隔1小时后发送一封失败邮件，最多5封邮件，通过失败次数控制
            错误消息中增加首次出现问题时间
            接口通过查询一次全部提供，无需要多次调用接口
            dbapi:接口告警配置接口，提供以上参数，入口task_tag
            项目指标：开发不同项目可自定义指标，可从系统指标中选择，增加t_moitor_proj_index表
            cpu:100%,mem:90%,disk: 85%,load : val>cpu核数*1.7 
            
'''
def server_warning(config):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    ft = '''服务器名：{0}:{1}<br>
告警时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}<br>
失败次数：{5}<br>
失败时长：{6}s'''

    ft_wx= '''服务器名：{0}:{1}
告警时间：{2}
告警级别：{3}
异常描述：{4}
失败次数：{5}
失败时长：{6}s'''

    st = '''服务器名：{0}:{1}<br>
恢复时间：{2}<br>
告警级别：{3}<br>
恢复日志：{4}'''

    st_wx = '''服务器名：{0}:{1}
恢复时间：{2}
告警级别：{3}
恢复日志：{4}'''

    '''发送异常邮件或微信'''
    cr.execute('''SELECT 
                         a.server_id,
                         a.market_id,
                         'server_available'  AS index_code,
                         '服务器连接'         AS index_name,
                         a.create_date,
                         CASE WHEN TIMESTAMPDIFF(MINUTE, a.create_date, NOW())>3 THEN '0' ELSE '100' END  AS index_value
                      FROM t_monitor_task_server_log a ,t_server b
                       WHERE  a.server_id=b.id 
                           AND b.server_ip NOT LIKE '10.2.39.%' 
                           AND (a.server_id,a.create_date) IN( 
                                SELECT a.server_id, MAX(a.create_date) FROM t_monitor_task_server_log a GROUP BY server_id) 
                     ''')
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        if r['index_value'] == '0':
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'failure')
            warn_info = get_warn_info(cfg, r['server_id'],r['index_code'])

            v_title = '{}告警'.format(server_info['server_desc'])
            v_content = ft.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '服务器连接异常(1：网络异常 2:代理异常 3：服务器异常)',
                                  warn_info['fail_times'],
                                  get_seconds(warn_info['first_failure_time']))
            v_content_wx = ft_wx.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '服务器连接异常(1：网络异常 2:代理异常 3：服务器异常)',
                                  warn_info['fail_times'],
                                  get_seconds(warn_info['first_failure_time']))

            print('warn_info>>>:',warn_info)
            if (warn_info['is_send_alt_mail'] == 'N' and  warn_info['fail_times'] > 3  or \
                  warn_info['is_send_alt_mail'] == 'Y' and  warn_info['fail_times'] % 60 == 0) \
                    and warn_info['is_send_alt_mail_times'] <=5:
               write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'warning')
               send_message(cfg,
                            get_proj_recever_wx(cfg,r['market_id']),
                            v_title,
                            v_content_wx)

               send_mail465(cfg.get('send_server'),
                            cfg.get('send_port'),
                            cfg.get('sender'),
                            cfg.get('sendpass'),
                            get_proj_recever(cfg,r['market_id']),
                            v_title,
                            v_content)
        else:
            # 写恢复告警日志
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'success')

    '''发送异常恢复邮件或微信'''
    cr.execute("""SELECT * FROM t_monitor_server_warn_log a,t_server b
                         where a.server_id=b.id 
                           AND b.server_ip NOT LIKE '10.2.39.%' 
                             AND a.succ_times=1 
                               AND a.index_code='server_available' 
                                AND a.is_send_alt_mail ='Y'
                                  AND a.is_send_rcv_mail='N' order by server_id""")
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], '', 'recover')

        v_title = '{}告警已恢复'.format(server_info['server_desc'])
        v_content = st.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复')
        v_content_wx = st_wx.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复')

        send_message(cfg,
                    get_proj_recever_wx(cfg,r['market_id']),
                    v_title,
                    v_content_wx)

        send_mail465(cfg.get('send_server'),
                     cfg.get('send_port'),
                     cfg.get('sender'),
                     cfg.get('sendpass'),
                     get_proj_recever(cfg, r['market_id']),
                     v_title,
                     v_content)

    '''关送数据库连接'''
    cfg['db_mysql'].close()
    cfg['db_mysql_dict'].close()

def cpu_warning(config):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    ft = '''服务器名：{0}:{1}<br>
告警时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}<br>
失败次数：{5}'''

    ft_wx = '''服务器名：{0}:{1}
告警时间：{2}
告警级别：{3}
异常描述：{4}
失败次数：{5}'''

    st = '''服务器名：{0}:{1}<br>
恢复时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}'''

    st_wx = '''服务器名：{0}:{1}
恢复时间：{2}
告警级别：{3}
异常描述：{4}'''

    '''发送异常邮件或微信'''
    cr.execute(''' 
                SELECT 
                   a.server_id,
                   a.market_id,
                   a.create_date,
                   cpu_total_usage   AS index_value,
                   'cpu_total_usage' AS index_code,
                   'cpu使用率'        AS index_name,
                   a.create_date
                FROM t_monitor_task_server_log a ,t_server b
                 WHERE  a.server_id=b.id 
                     AND b.server_ip NOT LIKE '10.2.39.%' 
                     AND (a.server_id,a.create_date) IN( 
                          SELECT a.server_id, MAX(a.create_date) FROM t_monitor_task_server_log a GROUP BY server_id) 
               ''')
    rs = cr.fetchall()
    print('>>>>>>>cpu告警....')
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        index_threshold = get_index_threshold(config, r['index_code'])
        if float(r['index_value']) > index_threshold :
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'failure')
            warn_info = get_warn_info(cfg, r['server_id'],r['index_code'])
            v_title = '{}告警({})'.format(server_info['server_desc'], warn_info['fail_times'])
            v_content = ft.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '{}{}% (阀值{}%)'.format(r['index_name'], r['index_value'], index_threshold),
                                  warn_info['fail_times']
                                  )
            v_content_wx = ft_wx.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '{}{}% (阀值{}%)'.format(r['index_name'], r['index_value'], index_threshold),
                                  warn_info['fail_times']
                                  )
            if (warn_info['is_send_alt_mail'] == 'N' and warn_info['fail_times'] > 3 or \
                warn_info['is_send_alt_mail'] == 'Y' and warn_info['fail_times'] % 60 == 0) \
                    and warn_info['is_send_alt_mail_times'] <= 5:
                write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'warning')
                send_message(cfg,
                             get_proj_recever_wx(cfg, r['market_id']),
                             v_title,
                             v_content_wx)

                send_mail465(cfg.get('send_server'),
                             cfg.get('send_port'),
                             cfg.get('sender'),
                             cfg.get('sendpass'),
                             get_proj_recever(cfg, r['market_id']),
                             v_title,
                             v_content)
        else:
            # 写恢复告警日志
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'success')

    '''发送异恢复邮件或微信'''
    cr.execute("""SELECT * FROM t_monitor_server_warn_log a,t_server b
                   where a.server_id=b.id 
                     AND b.server_ip NOT LIKE '10.2.39.%' 
                       AND a.succ_times=1 
                         AND a.index_code='cpu_total_usage' 
                          AND a.is_send_alt_mail ='Y'
                           AND a.is_send_rcv_mail='N' order by server_id""")
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], '', 'recover')

        v_title = '{}告警已恢复'.format(server_info['server_desc'])

        v_content = st.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复({})'.format(r['index_value']))

        v_content_wx = st_wx.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复').format(r['index_value'])

        send_message(cfg,
                    get_proj_recever_wx(cfg,r['market_id']),
                    v_title,
                    v_content_wx)

        send_mail465(cfg.get('send_server'),
                     cfg.get('send_port'),
                     cfg.get('sender'),
                     cfg.get('sendpass'),
                     get_proj_recever(cfg, r['market_id']),
                     v_title,
                     v_content)

    '''关送数据库连接'''
    cfg['db_mysql'].close()
    cfg['db_mysql_dict'].close()

def mem_warning(config):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    ft = '''服务器名：{0}:{1}
告警时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}<br>
失败次数：{5}'''

    ft_wx = '''服务器名：{0}:{1}
告警时间：{2}
告警级别：{3}
异常描述：{4}
失败次数：{5}'''

    st = '''服务器名：{0}:{1}
恢复时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}'''

    st_wx = '''服务器名：{0}:{1}
恢复时间：{2}
告警级别：{3}
异常描述：{4}'''

    '''发送异常邮件或微信'''
    cr.execute('''
                SELECT 
                   a.server_id,
                   a.market_id,
                   a.create_date,
                   mem_usage     AS index_value,
                   'mem_usage'   AS index_code,
                   '内存使用率'    AS index_name 
                FROM t_monitor_task_server_log a ,t_server b
                 WHERE a.server_id=b.id 
                     AND b.server_ip NOT LIKE '10.2.39.%' 
                     AND (a.server_id,a.create_date) IN( 
                          SELECT a.server_id, MAX(a.create_date) FROM t_monitor_task_server_log a GROUP BY server_id) 
               ''')
    rs = cr.fetchall()
    print('>>>>>>>内存告警....')
    for r in rs:
        server_info     = get_server_info(cfg, r['server_id'])
        index_threshold = get_index_threshold(config, r['index_code'])
        if float(r['index_value']) > index_threshold :
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'failure')
            warn_info = get_warn_info(cfg, r['server_id'],r['index_code'])
            v_title = '{}告警'.format(server_info['server_desc'])
            v_content = ft.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '{}{}% (阀值{}%)'.format(r['index_name'], r['index_value'], index_threshold),
                                  warn_info['fail_times']
                                  )
            v_content_wx = ft_wx.format(server_info['server_ip'], server_info['server_port'],
                                        get_time(),
                                        cfg['warn_level'],
                                        '{}{}% (阀值{}%)'.format(r['index_name'], r['index_value'], index_threshold),
                                        warn_info['fail_times']
                                        )

            if (warn_info['is_send_alt_mail'] == 'N' and warn_info['fail_times'] > 3 or \
                warn_info['is_send_alt_mail'] == 'Y' and warn_info['fail_times'] % 60 == 0) \
                    and warn_info['is_send_alt_mail_times'] <= 5:
                write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'warning')
                send_message(cfg,
                             get_proj_recever_wx(cfg, r['market_id']),
                             v_title,
                             v_content_wx)

                send_mail465(cfg.get('send_server'),
                             cfg.get('send_port'),
                             cfg.get('sender'),
                             cfg.get('sendpass'),
                             get_proj_recever(cfg, r['market_id']),
                             v_title,
                             v_content)
        else:
            # 写恢复告警日志
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'success')

    '''发送异恢复邮件或微信'''
    cr.execute("""SELECT * FROM t_monitor_server_warn_log a,t_server b
                   where a.server_id=b.id 
                     AND b.server_ip NOT LIKE '10.2.39.%' 
                      AND a.succ_times=1 
                       AND a.index_code='mem_usage' 
                        AND a.is_send_alt_mail ='Y'
                         AND a.is_send_rcv_mail='N' order by server_id""")
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], '', 'recover')
        v_title = '{}告警已恢复'.format(server_info['server_desc'])
        v_content = st.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复({})'.format(r['index_value'])
                              )
        v_content_wx = st_wx.format(server_info['server_ip'],
                                    server_info['server_port'],
                                    get_time(),
                                    cfg['warn_level'],
                                    r['index_name'] + '已恢复').format(r['index_value'])

        send_message(cfg,
                     get_proj_recever_wx(cfg, r['market_id']),
                     v_title,
                     v_content_wx)

        send_mail465(cfg.get('send_server'),
                     cfg.get('send_port'),
                     cfg.get('sender'),
                     cfg.get('sendpass'),
                     get_proj_recever(cfg, r['market_id']),
                     v_title,
                     v_content)

    '''关送数据库连接'''
    cfg['db_mysql'].close()
    cfg['db_mysql_dict'].close()

def disk_warning(config):
    cfg = get_config(config)
    db  = cfg['db_mysql_dict']
    cr  = db.cursor()
    ft  = '''服务器名：{0}:{1}<br>
告警时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}<br>
失败次数：{5}
'''
    ft_wx = '''服务器名：{0}:{1}
告警时间：{2}
告警级别：{3}
异常描述：{4}
失败次数：{5}'''

    st = '''服务器名：{0}:{1}<br>
恢复时间：{2}<br>
告警级别：{3}<br>
异常描述：{4}'''

    st_wx = '''服务器名：{0}:{1}
恢复时间：{2}
告警级别：{3}
异常描述：{4}'''

    '''发送异常邮件或微信'''
    cr.execute('''SELECT 
                     a.server_id,
                     a.market_id,
                     a.create_date,
                     a.disk_usage      AS index_value,
                     'disk_usage'      AS index_code,
                     '磁盘使用率'       AS index_name,
                     a.create_date
                  FROM t_monitor_task_server_log a ,t_server b
                   WHERE  a.server_id=b.id 
                       AND b.server_ip NOT LIKE '10.2.39.%' 
                       AND (a.server_id,a.create_date) IN( 
                            SELECT a.server_id, MAX(a.create_date) FROM t_monitor_task_server_log a GROUP BY server_id) 
                 ''')
    rs = cr.fetchall()
    print('>>>>>>>磁盘使用率告警....')
    for r in rs:
        server_info     = get_server_info(cfg, r['server_id'])
        index_threshold = get_index_threshold(config, r['index_code'])
        max_disk_usage  = get_max_disk_usage(json.loads(r['index_value']))

        if float(max_disk_usage) > index_threshold:
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], max_disk_usage, 'failure')
            warn_info = get_warn_info(cfg, r['server_id'],r['index_code'])
            v_title   = '{}告警'.format(server_info['server_desc'])
            v_content = ft.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '{}{}% (阀值{}%)'.format(r['index_name'], max_disk_usage+'%', index_threshold),
                                  warn_info['fail_times']
                                  )

            v_content_wx = ft.format(server_info['server_ip'], server_info['server_port'],
                                  get_time(),
                                  cfg['warn_level'],
                                  '{}{}% (阀值{}%)'.format(r['index_name'], max_disk_usage+'%', index_threshold),
                                  warn_info['fail_times']
                                  )
            if (warn_info['is_send_alt_mail'] == 'N' and warn_info['fail_times'] > 3 or \
                warn_info['is_send_alt_mail'] == 'Y' and warn_info['fail_times'] % 60 == 0) \
                    and warn_info['is_send_alt_mail_times'] <= 5:
                write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], r['index_value'], 'warning')
                send_message(cfg,
                             get_proj_recever_wx(cfg, r['market_id']),
                             v_title,
                             v_content_wx)

                send_mail465(cfg.get('send_server'),
                             cfg.get('send_port'),
                             cfg.get('sender'),
                             cfg.get('sendpass'),
                             get_proj_recever(cfg, r['market_id']),
                             v_title,
                             v_content)
        else:
            # 写恢复告警日志
            write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], max_disk_usage, 'success')

    '''发送异恢复邮件或微信'''
    cr.execute("""SELECT * FROM t_monitor_server_warn_log a,t_server b
                     where a.server_id=b.id 
                       AND b.server_ip NOT LIKE '10.2.39.%' 
                         AND a.succ_times=1 
                           AND a.index_code='disk_usage' 
                             AND a.is_send_alt_mail ='Y'
                              AND a.is_send_rcv_mail='N' order by server_id""")
    rs = cr.fetchall()
    for r in rs:
        server_info = get_server_info(cfg, r['server_id'])
        write_warn_log(cfg, r['server_id'], r['index_code'], r['index_name'], '', 'recover')
        v_title     = '{}告警已恢复'.format(server_info['server_desc'])
        v_content   = st.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复({})'.format(r['index_value'])
                              )
        v_content_wx = st.format(server_info['server_ip'],
                              server_info['server_port'],
                              get_time(),
                              cfg['warn_level'],
                              r['index_name'] + '已恢复({})'.format(r['index_value'])
                              )

        send_message(cfg,
                     get_proj_recever_wx(cfg, r['market_id']),
                     v_title,
                     v_content_wx)

        send_mail465(cfg.get('send_server'),
                     cfg.get('send_port'),
                     cfg.get('sender'),
                     cfg.get('sendpass'),
                     get_proj_recever(cfg, r['market_id']),
                     v_title,
                     v_content)

    '''关送数据库连接'''
    cfg['db_mysql'].close()
    cfg['db_mysql_dict'].close()

def get_max_disk_usage(d_disk):
    n_max_val =0.0
    for key in d_disk:
        if n_max_val <= float (d_disk[key]):
           n_max_val = float (d_disk[key])
    result = str(n_max_val)
    return result

def check_alert_config(p_tag):
    cfg = get_config(config)
    db = cfg['db_mysql_dict']
    cr = db.cursor()
    st = "select count(0) as rec from t_alert_task where task_tag='{0}'".format(p_tag)
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

def get_items_from_monitor_templete(p_templete):
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
    print('rs=',rs)
    rs['templete_monitor_indexes'] = get_items_from_monitor_templete(rs['templete_id'])
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

    # cpu 告警
    cpu_warning(cfg)

    # 内存告警
    mem_warning(config)

    # 磁盘使用率告警
    disk_warning(config)