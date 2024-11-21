#!/usr/bin/env python3
import traceback
import pymysql
import sys
import warnings
import json
import smtplib
import requests
import datetime
import socket
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
        server = smtplib.SMTP_SSL(p_sendserver, 465,timeout=10.0)
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
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db='information_schema', charset='utf8')
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db='information_schema', charset='utf8', cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mysql_test(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',read_timeout=3)
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
            if config.get('id_ro') is not None and config.get('id_ro') !='':
                db_ip_ro      = config['ds_ro']['ip']
                db_port_ro    = config['ds_ro']['port']
                db_service_ro = config['ds_ro']['service']
                db_user_ro    = config['ds_ro']['user']
                db_pass_ro    = aes_decrypt(config['ds_ro']['password'], db_user_ro)
                config['db_mysql_ro'] = get_ds_mysql(db_ip_ro, db_port_ro, db_service_ro, db_user_ro, db_pass_ro)
                config['db_mysql_dict_ro'] = get_ds_mysql_dict(db_ip_ro, db_port_ro, db_service_ro, db_user_ro, db_pass_ro)
                config['db_ip_ro']   = db_ip_ro
                config['db_port_ro'] = db_port_ro
                config['db_service_ro'] = db_service_ro
                config['db_user_ro'] = db_user_ro
                config['db_pass_ro'] = config['ds_ro']['password']
                config['db_desc_ro'] = config['ds_ro']['db_desc']
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
        v_content = v_content.replace('$$parameter$$', json.dumps(par))
        v_content = v_content.replace('$$error$$', res['msg'])
        exception_interface(v_title, v_content)
        sys.exit(0)

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

def db_to_html(str):
    return str.replace('\t','&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;').replace('\n','<br>')

def get_conn_info(db):
    cr = db.cursor()
    p_conn_total_sql  = "SELECT count(0) FROM information_schema.processlist"
    p_conn_active_sql = "SELECT count(0) FROM information_schema.processlist where command <>'Sleep'"
    cr.execute(p_conn_total_sql)
    rs=cr.fetchone()
    p_conn_total=rs[0]
    cr.execute(p_conn_active_sql)
    rs=cr.fetchone()
    p_conn_active=rs[0]
    db.commit()
    cr.close()
    return "total:{0},active:{1}".format(p_conn_total,p_conn_active)

def get_html_contents(config,thead,tbody):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_html='''<html>
		<head>
		   <style type="text/css">
			   .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
			   .xwtable thead td {font-size: 12px;color: #333333;
					      text-align: center;background: url(table_top.jpg) repeat-x top center;
				              border: 1px solid #ccc; font-weight:bold;}
			   .xwtable thead th {font-size: 10px;color: #333333;
				              text-align: center;background: url(table_top.jpg) repeat-x top center;
					      border: 1px solid #ccc; font-weight:bold;}
			   .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
			   .xwtable tbody tr.alt-row {background: #f2f7fc;}
			   .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
		   </style>
		</head>
		<body>
		  <label>发送时间：</label><span>'''+nowTime+'''</span><br>
		  <label>数据库IP：</label><span>'''+config['db_ip']+'''</span><br>
		  <label>数据端口：</label><span>'''+config['db_port']+'''</span><br>
		  <label>当前连接</label><span>：'''+get_conn_info(config['db_mysql'])+'''</span><br>
		  <p>
		  <table class="xwtable">
			<thead>\n'''+thead+'\n</thead>\n'+'<tbody>\n'+tbody+'''\n</tbody>
		  </table>
		</body>
	    </html>
           '''.format(thead,tbody)
    return v_html

def get_html_contents_slave(config,thead,tbody):
    nowTime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    v_html='''<html>
		<head>
		   <style type="text/css">
			   .xwtable {width: 120%;border-collapse: collapse;border: 1px solid #ccc;}
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
		  <label>发送时间：</label><span>'''+nowTime+'''</span><br>
		  <label>数据库IP：</label><span>'''+config['db_ip_ro']+'''</span><br>
		  <label>数据端口：</label><span>'''+config['db_port_ro']+'''</span><br>
		  <label>当前连接</label><span>：'''+get_conn_info(config['db_mysql_ro'])+'''</span><br>
		  <p>
		  <table class="xwtable">
			<thead>\n'''+thead+'\n</thead>\n'+'<tbody>\n'+tbody+'''\n</tbody>
		  </table>
		</body>
	    </html>
           '''.format(thead,tbody)
    return v_html

def get_slow_sql(config,p_slow_query_time,flag='master'):
    tbody=''
    thead=''

    if flag == 'master':
      cr = config['db_mysql'].cursor()

    if flag == 'slave':
      cr = config['db_mysql_ro'].cursor()

    sql = """Select   cast(id as char)      as "线程ID",
                       HOST                 as "访问者IP",
                       USER                 AS "用户",
                       db                   as "数据库",
                       command              as "命令类型",
                       cast(TIME as char)   as "耗时(s)" ,
                       state                as "状态",
                       info                 as "语句"
                From information_schema.processlist  
                Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
                  -- and instr(info,'SQL_NO_CACHE')=0
                  and time>={0}  order by time desc
            """.format(p_slow_query_time)
    cr.execute(sql)
    rs=cr.fetchall()
    desc = cr.description

    if len(rs)==0:
      return ''

    row='<tr>'
    for k in range(len(desc)):
        if k==0:
           row=row+'<th bgcolor=#8E8E8E width=5%>'+str(desc[k][0])+'</th>'
        elif k in(1,2,3,4,5,6):
           row=row+'<th bgcolor=#8E8E8E width=10%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=45%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
      row='<tr>'
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        else:
          row=row+'<td>'+db_to_html(i[j])+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row

    if flag == 'master':
        v_html = get_html_contents(config, thead, tbody)

    if flag == 'slave':
        v_html = get_html_contents_slave(config, thead, tbody)

    if flag == 'master':
        config['db_mysql'].commit()
    if flag == 'slave':
        config['db_mysql_ro'].commit()
    cr.close()

    return v_html

def get_big_tran(config,p_trx_time,flag='master'):
    tbody=''
    thead=''

    if flag == 'master':
      cr = config['db_mysql'].cursor()

    if flag == 'slave':
      cr = config['db_mysql_ro'].cursor()

    sql = """SELECT CAST(a.id AS CHAR)    AS "线程ID",
                   a.HOST                 AS "地址",
                   a.USER                 AS "用户",
                   a.db                   AS "数据库",
                   CAST(a.TIME AS CHAR)   AS "耗时" ,
                   b.trx_state            AS "状态",
                   date_format(b.trx_started,'%Y-%m-%d %H:%i:%s') AS "开始时间",
                   a.info                 AS "语句"
            FROM information_schema.processlist a,information_schema.innodb_trx b 
            WHERE a.command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
              AND a.id = b.trx_mysql_thread_id
              AND INSTR(a.info,'SQL_NO_CACHE')=0
              AND a.time>={}  ORDER BY a.time DESC
            """.format(p_trx_time)
    cr.execute(sql)
    rs=cr.fetchall()
    desc = cr.description

    if len(rs)==0:
      return ''

    row='<tr>'
    for k in range(len(desc)):
        if k==0:
           row=row+'<th bgcolor=#8E8E8E width=5%>'+str(desc[k][0])+'</th>'
        elif k in(1,2,3,4,5,6):
           row=row+'<th bgcolor=#8E8E8E width=6%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=45%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
      row='<tr>'
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        else:
          print(i,j,i[j])
          row=row+'<td>'+db_to_html(i[j])+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row

    if flag == 'master':
        v_html = get_html_contents(config, thead, tbody)

    if flag == 'slave':
        v_html = get_html_contents_slave(config, thead, tbody)

    if flag == 'master':
        config['db_mysql'].commit()
    if flag == 'slave':
        config['db_mysql_ro'].commit()
    cr.close()
    return v_html

def get_block_txn(config,p_trx_query_time,flag='master'):
    tbody=''
    thead=''
    if flag == 'master':
        cr = config['db_mysql'].cursor()

    if flag == 'slave':
        cr = config['db_mysql_ro'].cursor()

    sql = """SELECT distinct 
                      b.trx_id      AS '事务ID',
                      a.id          AS '线程ID',
                      a.host        AS '主机',
                      a.db          AS '数据库',
                      a.user        AS '用户',
                      IFNULL(e.block_trx_id,'') AS '阻塞事务ID',
                      b.trx_started AS '开始时间',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         a.time 
                      ELSE 
                        TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)  END AS '时长(s)',
                      CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN 
                         'BLOCKING' 
                      ELSE 'RUNNING'  END AS '事务状态',
                      a.state       AS '进程状态',
                      b.trx_isolation_level AS '隔离级别',
                      c.lock_table  AS '锁定表',
                      c.lock_mode   AS '锁模式',
                      c.lock_type   AS '锁类型',
                      b.trx_query   AS 'SQL语句'
		FROM information_schema.processlist a
		INNER JOIN information_schema.innodb_trx  b ON b.trx_mysql_thread_id=a.id
		INNER JOIN information_schema.INNODB_LOCKS c ON b.trx_id=c.lock_trx_id
		LEFT  JOIN (SELECT w.requesting_trx_id,
					GROUP_CONCAT(CONCAT(w.blocking_trx_id,'|') ORDER BY t.trx_started  SEPARATOR '') AS block_trx_id,
					GROUP_CONCAT(CONCAT('kill ',t.trx_mysql_thread_id,'; \n') ORDER BY t.trx_started  SEPARATOR '') AS kill_thread
	     		    FROM information_schema.innodb_lock_waits w,
				 information_schema.innodb_trx  t
			    WHERE w.blocking_trx_id=t.trx_id
			    GROUP BY w.requesting_trx_id) e ON b.trx_id=e.requesting_trx_id
                WHERE  CASE WHEN(e.requesting_trx_id IS NOT NULL) THEN
                           a.time
                       ELSE
                           TIMESTAMPDIFF(SECOND,b.trx_started,CURRENT_TIMESTAMP)
                       END >={0}     
	         ORDER BY b.trx_started""".format(p_trx_query_time)

    cr.execute(sql)
    rs=cr.fetchall()

    if len(rs)==0:
     return ''

    desc = cr.description
    row='<tr>'
    for k in range(len(desc)):
      if k in(0,1,4,5):
         row=row+'<th bgcolor=#8E8E8E width=4%>'+desc[k][0]+'</th>'
      elif k in(2,3,7,11,12):
         row=row+'<th bgcolor=#8E8E8E width=6%>'+desc[k][0]+'</th>'
      elif k in(6,):
         row=row+'<th bgcolor=#8E8E8E width=14%>'+desc[k][0]+'</th>'
      elif k in(8,9,10):
         row=row+'<th bgcolor=#8E8E8E width=10%>'+desc[k][0]+'</th>'
      else:
         row=row+'<th bgcolor=#8E8E8E width=24%>'+db_to_html(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
     row='<tr>'
     for j in range(len(i)):
       if i[j] is None:
         row=row+'<td>&nbsp;</td>'
       else:
         row=row+'<td>'+str(i[j])+'</td>'
     row=row+'</tr>\n'
     tbody=tbody+row

    if flag == 'master':
        v_html = get_html_contents(config, thead, tbody)

    if flag == 'slave':
        v_html = get_html_contents_slave(config, thead, tbody)

    if flag == 'master':
        config['db_mysql'].commit()
    if flag == 'slave':
        config['db_mysql_ro'].commit()
    cr.close()

    return v_html

def kill_slow_sql(config,p_slow_query_time,flag='master'):
    tbody=''
    thead=''

    if flag == 'master':
      cr = config['db_mysql'].cursor()

    if flag == 'slave':
      cr = config['db_mysql_ro'].cursor()

    sql = """Select   cast(id as char)      as "线程ID",
                       HOST                 as "访问者IP",
                       USER                 AS "用户",
                       db                   as "数据库",
                       command              as "命令类型",
                       cast(TIME as char)   as "耗时(s)" ,
                       state                as "状态",
                       info                 as "语句"
                From information_schema.processlist  
                Where command NOT IN('Sleep','Binlog Dump','Daemon','Connect','Binlog Dump GTID')  
                  and instr(info,'SQL_NO_CACHE')=0
                  and time>={0}  order by time desc
            """.format(p_slow_query_time)
    cr.execute(sql)
    rs=cr.fetchall()
    desc = cr.description

    if len(rs)==0:
      return ''

    row='<tr>'
    for k in range(len(desc)):
        if k==0:
           row=row+'<th bgcolor=#8E8E8E width=5%>'+str(desc[k][0])+'</th>'
        elif k in(1,2,3,4,5,6):
           row=row+'<th bgcolor=#8E8E8E width=10%>'+str(desc[k][0])+'</th>'
        else:
           row=row+'<th bgcolor=#8E8E8E width=45%>'+str(desc[k][0])+'</th>'
    row=row+'</tr>'
    thead=thead+row

    for i in rs:
      row='<tr>'
      for j in range(len(i)):
        if i[j] is None:
          row=row+'<td>&nbsp;</td>'
        else:
          row=row+'<td>'+db_to_html(i[j])+'</td>'
      row=row+'</tr>\n'
      tbody=tbody+row

    if flag == 'master':
       v_html =get_html_contents(config,thead,tbody)

    if flag == 'slave':
       v_html = get_html_contents_slave(config, thead, tbody)

    # kill slow sql
    for i in rs:
        print('kill session id: {} for {}!'.format(i[0],flag))
        cr.execute('kill {}'.format(i[0]))

    if flag == 'master':
        config['db_mysql'].commit()
    if flag == 'slave':
        config['db_mysql_ro'].commit()
    cr.close()
    return v_html

def monitor(config):

   for idx in config['templete_monitor_indexes']:

       if idx['index_code'].count('mysql_slow_time')>0:
           # check master
           print('mysql_slow_time...')
           content = get_slow_sql(config,idx['index_threshold'],'master')
           title = config.get('db_desc') + '慢查询告警'
           if content != '':
               send_mail_param(config.get('send_server'),config.get('sender'),config.get('sendpass'),config.get('receiver'),title, content)
               print("master slow query mail send success!")
           # check slave
           if config.get('id_ro') is not None and config.get('id_ro') !='':
               content = get_slow_sql(config, idx['index_threshold'],'slave')
               title = config.get('db_desc_ro') + '慢查询告警'
               if content != '':
                   send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'),
                                   config.get('receiver'), title, content)
                   print("slave slow query mail send success!")

       if idx['index_code'].count('mysql_tran_time')>0:
           # check master
           print('mysql_tran_time...')
           content = get_big_tran(config, idx['index_threshold'], 'master')
           title = config.get('db_desc') + '大事务告警'
           if content != '':
               send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'),
                               config.get('receiver'), title, content)
               print("master big transaction  mail send success!")
           # check slave
           if config.get('id_ro') is not None and config.get('id_ro') !='':
               content = get_big_tran(config, idx['index_threshold'], 'slave')
               title = config.get('db_desc_ro') + '大事务告警'
               if content != '':
                   send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'),
                                   config.get('receiver'), title, content)
                   print("slave big transaction mail send success!")

       if idx['index_code'].count('mysql_lock_block_time')>0:
           print('mysql_lock_block_time...')
           # check master
           content = get_block_txn(config, idx['index_threshold'], 'master')
           title = config.get('db_desc') + '锁等待告警'
           if content != '':
               send_mail_param(config.get('send_server'),
                               config.get('sender'),
                               config.get('sendpass'),
                               config.get('receiver'), title, content)
               print("master lock wait  mail send success!")
           # check slave
           if config.get('id_ro') is not None and config.get('id_ro') !='':
               content = get_block_txn(config, idx['index_threshold'], 'slave')
               title = config.get('db_desc_ro') + '锁等待告警'
               if content != '':
                   send_mail_param(config.get('send_server'),
                                   config.get('sender'),
                                   config.get('sendpass'),
                                   config.get('receiver'), title, content)
                   print("slave lock wait  mail send success!")

       if idx['index_code'].count('mysql_kill_time')>0:
           # check master
           print('mysql_kill_time...')
           content = kill_slow_sql(config, idx['index_threshold'], 'master')
           title = config.get('db_desc') + '杀会话告警'
           if content != '':
               send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'),
                               config.get('receiver'), title, content)
               print("master kill slow query mail send success!")

           # check slave
           if config.get('id_ro') is not None and config.get('id_ro') !='':
               content = kill_slow_sql(config, idx['index_threshold'], 'slave')
               title = config.get('db_desc_ro') + '杀会话告警'
               if content != '':
                   send_mail_param(config.get('send_server'), config.get('sender'), config.get('sendpass'),
                                   config.get('receiver'), title, content)
                   print("slave kill slow query mail send success!")

       if idx['index_code'] == 'mysql_relay_time':
           pass


def main():
    config = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]

    #init
    config = init(config)

    # monitor
    monitor(config)

if __name__ == "__main__":
     main()
