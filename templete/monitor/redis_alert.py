import json
import pymysql
import traceback
import requests
import datetime
import socket
import smtplib
from email.mime.text import MIMEText
from urllib import parse

STAT_RANGE=10
SEND_URL="ops.zhitbar.cn:59521/monitor/redis/slowlog?{}"
# SEND_USER='190343'
# SEND_MAIL='190343@lifeat.cn'
SEND_USER='190343|190205|609717|850686'
SEND_MAIL='190343@lifeat.cn,190205@lifeat.cn,609479@hopson.com.cn,609717@hopson.com.cn'

ALERT_TITLE = 'REDIS慢日志告警'
ALERT_MESSAGE = '''实例名称：{}
实例地址：{}
预警阀值：{}ms
日志数量：{}个
告警时间：{}'''

MYSQL_SETTINGS = {
    "host": '10.2.39.18',
    "port": 3306,
    "user": "puppet",
    "passwd": "Puppet@123",
    "db": 'puppet'
}

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

def send_mail25(p_sendserver,p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP(p_sendserver, 25,timeout = 10.0)
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
        server = smtplib.SMTP_SSL(p_sendserver, 465,timeout = 10.0)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print('send_mail465 error=',e)

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
        "url":SEND_URL.format(batch_id),
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

def get_dbid(batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = 'SELECT dbid FROM t_monitor_redis_hz_log where batch_id={} limit 1'.format(batch_id.split(',')[0])
    cr.execute(st)
    rs=cr.fetchone()
    return rs['dbid']

def get_index_threshold(batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT index_threshold FROM t_monitor_index \
          WHERE index_code =(SELECT index_code FROM t_monitor_redis_log WHERE batch_id ={} LIMIT 1)""".format(batch_id.split(',')[0])
    cr.execute(st)
    rs = cr.fetchone()
    return rs['index_threshold']

def get_batch_id():
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT GROUP_CONCAT( DISTINCT batch_id) as batch_id 
              FROM t_monitor_redis_log a
                where a.start_time between DATE_SUB(NOW(), INTERVAL {} MINUTE) and now()
                     AND a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b 
                                     WHERE a.dbid=b.dbid and b.status='1')
                  """.format(STAT_RANGE)
    cr.execute(st)
    rs=cr.fetchone()
    return rs['batch_id']

def check_slowlog():
    db = get_db()
    cr = db.cursor()
    st = """SELECT count(0) FROM t_monitor_redis_log a
             WHERE a.start_time between DATE_SUB(NOW(), INTERVAL {} MINUTE) and now()
                AND a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b 
                                     WHERE a.dbid=b.dbid and b.status='1')""".format(STAT_RANGE)
    cr.execute(st)
    rs = cr.fetchone()
    if rs[0]>0:
        return True
    else:
        return False

def get_contents(dbinfo,slowlog_info,hz,mx):
    tpl = '''
     <html>
        <head>
           <style type="text/css">
               .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
               .xwtable thead td {font-size: 12px;color: #333333;
                                  text-align: center;
                                  border: 1px solid #ccc; font-weight:bold;}
               .xwtable thead th {font-size: 14px;color: #333333;
                                  text-align: center;
                                  border: 1px solid #ccc; font-weight:bold;}
               .xwtable thead tr {background: #ded9e1;font-size: 14px;color: #333926;}
               .xwtable tbody tr {background: #ffffff;font-size: 14px;color: #522c2c;}
               .xwtable tbody tr.alt-row {background: #f2f7fc;}
               .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #bcb2b2
           </style>
        </head>
        <body>  
           <span>实例名称:&nbsp;&nbsp;$$DBNAME$$</span><br>
           <span>实例地址:&nbsp;&nbsp;$$DBINFO$$</span><br>
           <span>统计范围:&nbsp;&nbsp;$$MIN_START_TIME$$ - $$MAX_START_TIME$$</span>
           <hr>    
           <h3 align="center">汇总信息</h3>    
           <table class='xwtable'>
              <thead>
                 <tr>
                    <th width=12%>序号</th>
                    <th width=22%>操作命令</th>
                    <th width=22%>平均耗时</th>
                    <th width=22%>开始时间</th>
                    <th width=22%>结束时间</th>
                 </tr>
              </thead>
              <tbody>
                $$HZINFO$$
              </tbody>
           </table>     
           <br>
           <br>
           <h3 align="center">明细信息</h3>
           <table class="xwtable">
               <thead>
                 <tr>
                    <th width=12%>序号</th>
                    <th width=22%>命令</th>
                    <th width=22%>耗时</th>
                    <th width=22%>开始时间</th>
                    <th width=22%>采集时间</th>
                </tr>
              </thead>
              <tbody>
                $$MXINFO$$
              </tbody>
           </table>          
        </body>
     </html>
    '''
    th=''
    for h in hz:
        th += """<tr>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
</tr>""".format(h['xh'],
                h['command'],
                h['avg_duration'],
                h['start_time'],
                h['end_time'])
    tm = ''
    for m in mx:
        tm += """<tr>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
  <td><span >{}</span></td>
</tr>""".format(m['xh'],
                m['command'],
                m['duration'],
                m['start_time'],
                m['create_time'])
    title = 'redis慢日志详情'
    contents = tpl.replace('$$DBINFO$$',dbinfo['ip']+':'+str(dbinfo['port'])).\
                   replace('$$DBNAME$$',dbinfo['db_desc']).\
                   replace('$$MIN_START_TIME$$',str(slowlog_info['min_start_time'])). \
                   replace('$$MAX_START_TIME$$',str(slowlog_info['max_start_time'])). \
                   replace('$$HZINFO$$',th).\
                   replace('$$MXINFO$$',tm)
    return title,contents

def get_slowlog_info(batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT COUNT(DISTINCT dbid,command,duration,start_time) as slowlog_number,
                   min(start_time) as min_start_time,
                   max(start_time) as max_start_time
            FROM t_monitor_redis_log a
            where a.batch_id in({})
               AND a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b 
                                     WHERE a.dbid=b.dbid and b.status='1')""".format(batch_id,STAT_RANGE)
    cr.execute(st)
    rs=cr.fetchone()
    return rs

def get_hzinfo(p_batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT concat((@rowNum:=@rowNum+1),'') as "xh",t.* 
            FROM (SELECT dbid,start_time,end_time,avg_duration,command 
                  FROM t_monitor_redis_hz_log a,(select (@rowNum:=0)) b
                  where a.batch_id in({0})                  
                    AND a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b 
                                     WHERE a.dbid=b.dbid and b.status='1')  
                    GROUP BY dbid,start_time,end_time,avg_duration,command) t""".format(p_batch_id)
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_mxinfo(p_batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT concat((@rowNum:=@rowNum+1),'') as "xh",t.* 
            FROM (SELECT dbid,start_time,duration,command,MAX(create_time) AS create_time
             FROM t_monitor_redis_log a ,(select (@rowNum:=0)) b
             where a.batch_id in({0})
                 AND a.command NOT IN(SELECT command FROM t_monitor_redis_whitelist b 
                                     WHERE a.dbid=b.dbid and b.status='1')  
                 GROUP BY dbid,start_time,duration,command) t""".format(p_batch_id)
    cr.execute(st)
    rs = cr.fetchall()
    return rs

def get_dbinfo(p_batch_id):
    db = get_db_dict()
    cr = db.cursor()
    st = """SELECT  dbid FROM t_monitor_redis_log where batch_id='{0}' limit 1""".format(p_batch_id.split(',')[0])
    cr.execute(st)
    rs = cr.fetchone()
    dbid = rs['dbid']
    st = """SELECT  * FROM t_db_source where id='{0}'""".format(dbid)
    cr.execute(st)
    rs = cr.fetchone()
    return rs

def monitor():
    if check_slowlog():
        batch_id = get_batch_id()
        print('batch_id=',batch_id)
        print('batch_id2=', batch_id.replace(',','#'))
        dbid= get_dbid(batch_id)
        dbinfo = get_db_info(dbid)
        slowlog_info = get_slowlog_info(batch_id)

        message = ALERT_MESSAGE. \
            format(dbinfo['db_desc'],
                   dbinfo['ip']+':'+str(dbinfo['port']),
                   get_index_threshold(batch_id),
                   slowlog_info['slowlog_number'],
                   get_time())
        print(message)

        # 发送微信
        query = parse.urlencode({'batch_id':batch_id})
        print('query=',query)
        send_message(SEND_USER, ALERT_TITLE, message,query)

        # 发送邮件
        hz =get_hzinfo(batch_id)
        mx = get_mxinfo(batch_id)
        dbinfo = get_dbinfo(batch_id)
        title,contents = get_contents(dbinfo,slowlog_info,hz,mx)
        send_mail_param('smtp.exmail.qq.com','190343@lifeat.cn', 'R86hyfjobMBYR76h', SEND_MAIL, title,contents)

    else:
        print('未找到10分钟内的慢日志!')

def main():
    monitor()


if __name__ == '__main__':
    main()
