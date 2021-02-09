#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Func : MySQL->MySQL增量同步（实时车流）
# @Software: PyCharm

import sys,time
import traceback
import configparser
import warnings
import pymssql
import pymysql
import datetime
import hashlib
import smtplib
from email.mime.text import MIMEText
import json
import urllib.parse
import urllib.request
import ssl

def send_mail465(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):
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

def exception_info():
    e_str=traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]

def get_now():
    return datetime.datetime.now().strftime("%H:%M:%S")

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn

def get_ds_sqlserver(ip,port,service,user,password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service,charset='utf8')
    return conn

def get_db_mysql_sour(config):
    return get_ds_mysql(config['db_mysql_sour_ip'],config['db_mysql_sour_port'],config['db_mysql_sour_service'],\
                        config['db_mysql_sour_user'],config['db_mysql_sour_pass'])

def get_db_mysql_desc(config):
    return get_ds_mysql(config['db_mysql_desc_ip'],config['db_mysql_desc_port'],config['db_mysql_desc_service'],\
                        config['db_mysql_desc_user'],config['db_mysql_desc_pass'])


def aes_decrypt(p_password,p_key):
    values = {
        'password': p_password,
        'key':p_key
    }
    url = 'http://10.2.39.18:8600/read_db_decrypt'
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

def get_archive_rq(config):
    db = config['db_mysql_sour']
    cr = db.cursor()
    sql = """select date_format(min({0}),'%Y-%m-%d %H:%i:%S'),
                    date_format(DATE_SUB(NOW(),INTERVAL {1} {2}),'%Y-%m-%d %H:%i:%S') from {3}                    
          """.format(config['archive_time_col'],
                     config['rentition_time'],
                     config['rentition_time_type_cn'].upper(),
                     config['sour_table']
                     )
    cr.execute(sql)
    rs = cr.fetchone()
    cr.close()
    print('get_archive_rq=',rs)
    return rs[0],rs[1]


def get_archive_where(config):
    v = ''
    v_rqq,v_rqz = get_archive_rq(config)
    v = """where {0} between '{1}' and '{2}'""".format(config['archive_time_col'], v_rqq, v_rqz)
    print('get_archive_where=',v)
    return v

def get_config_from_db(tag):
    values = {
        'tag': tag
    }
    print('values=', values)
    url = 'http://10.2.39.18:8600/read_config_archive'
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
            db_sour_ip                      = config['archive_db_sour'].split(':')[0]
            db_sour_port                    = config['archive_db_sour'].split(':')[1]
            db_sour_service                 = config['archive_db_sour'].split(':')[2]
            db_sour_user                    = config['archive_db_sour'].split(':')[3]
            db_sour_pass                    = aes_decrypt(config['archive_db_sour'].split(':')[4],db_sour_user)
            db_dest_ip                      = config['archive_db_dest'].split(':')[0]
            db_dest_port                    = config['archive_db_dest'].split(':')[1]
            db_dest_service                 = config['archive_db_dest'].split(':')[2]
            db_dest_user                    = config['archive_db_dest'].split(':')[3]
            db_dest_pass                    = aes_decrypt(config['archive_db_dest'].split(':')[4],db_dest_user)
            config['db_mysql_sour_ip']      = db_sour_ip
            config['db_mysql_sour_port']    = db_sour_port
            config['db_mysql_sour_service'] = db_sour_service
            config['db_mysql_sour_user']    = db_sour_user
            config['db_mysql_sour_pass']    = db_sour_pass
            config['db_mysql_desc_ip']      = db_dest_ip
            config['db_mysql_desc_port']    = db_dest_port
            config['db_mysql_desc_service'] = db_dest_service
            config['db_mysql_desc_user']    = db_dest_user
            config['db_mysql_desc_pass']    = db_dest_pass
            config['db_mysql_sour_string']  = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
            config['db_mysql_desc_string']  = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
            config['db_mysql_sour']         = get_ds_mysql(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
            config['db_mysql_desc']         = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
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

def write_archive_log(config):
    v_tag = {
        'table_name'      : config['table_name'],
        'archive_tag'     : config['archive_tag'],
        'create_date'     : config['create_date'],
        'start_time'      : config['start_time'],
        'end_time'        : config['end_time'] ,
        'duration'        : config['duration'],
        'amount'          : config['amount'],
        'percent'         : config['percent'],
        'message'         : config['message']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    url = 'http://10.2.39.18:8600/write_archive_log'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req  = urllib.request.Request(url, data=data)
    res  = urllib.request.urlopen(req, context=context)
    res  = json.loads(res.read())
    if res['code'] != 200:
        print('Interface write_archive_log call failed!')

def check_mysql_tab_exists(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema=database() and table_name='{0}'""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_rows(db,tab):
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_rows_where(db,tab,where):
   cr=db.cursor()
   sql="""select count(0) from {0} {1}""".format(tab,where )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_sync(config,tab):
   db=config['db_mysql_desc']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_exists_pk(config,tab):
   db=config['db_mysql_sour']
   cr=db.cursor()
   sql = """select count(0) from information_schema.columns
              where table_schema=database() and table_name='{0}' and column_key='PRI'""".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_tab_header_pkid(config,tab):
    db=config['db_mysql_sour']
    cr=db.cursor()
    cols=get_sync_table_cols_pkid(db,tab)
    sql="select {0} from {1} limit 1".format(cols,tab)
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+cols+")"
    cr.close()
    return s1+s2

def get_tab_header(config,tab):
    db=config['db_mysql_sour']
    cr=db.cursor()
    sql="select * from {0} limit 1".format(tab)
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+tab.lower()+"("
    s2=" values "
    s1=s1+get_sync_table_cols(db,tab)+")"
    cr.close()
    return s1+s2

def f_get_table_ddl(config,tab):
    db_source = config['db_mysql_sour']
    cr_source = db_source.cursor()
    v_sql     ="""show create table {0}""".format(tab)
    cr_source.execute(v_sql)
    rs=cr_source.fetchone()
    return rs[1]

def archive_mysql_ddl(config,debug):
    try:
        db_source = config['db_mysql_sour']
        cr_source = db_source.cursor()
        db_desc   = config['db_mysql_desc']
        cr_desc   = db_desc.cursor()
        for i in config['sour_table'].split(","):
            tab=i.split(':')[0]
            sql = """SELECT table_schema,table_name 
                                  FROM information_schema.tables
                                 WHERE table_schema=database() and table_name='{0}' order by table_name
                              """.format(tab)
            cr_source.execute(sql)
            rs_source = cr_source.fetchall()
            for j in range(len(rs_source)):
                tab_name = rs_source[j][1].lower()
                if check_mysql_tab_exists_pk(config,tab)==0:
                   print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_mysql_sour_string'],tab_name))
                   v_cre_sql = f_get_table_ddl(config, tab_name)
                   if check_mysql_tab_exists(config, tab_name) > 0:
                       print("DB:{0},Table :{1} already exists!".format(config['db_mysql_sour_string'],tab_name))
                   else:
                       cr_desc.execute(v_cre_sql)
                       print("Table:{0} creating success!".format(tab_name))
                       v_pk_sql="""ALTER TABLE {0} ADD COLUMN pkid INT(11) NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (pkid)
                                """.format(tab_name)
                       print( "Table:{0} add primary key pkid success!".format(tab_name))
                       cr_desc.execute(v_pk_sql)
                       db_desc.commit()
                else:
                   #编写函数完成生成创表语句
                   v_cre_sql = f_get_table_ddl(config,tab_name)
                   if check_mysql_tab_exists(config,tab_name)>0:
                       print("DB:{0},Table :{1} already exists!".format(config['db_mysql_desc_string'],tab_name))
                   else:
                      cr_desc.execute(v_cre_sql)
                      print("Table:{0} creating success!".format(tab_name))
                      db_desc.commit()
        cr_source.close()
        cr_desc.close()
    except Exception as e:
        print('sync_mysql_ddl exceptiion:'+traceback.format_exc())
        exception_running(config,traceback.format_exc())
        exit(0)

def get_sync_table_rows(db,tab,v_where):
    cr_source = db.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_pk_names(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select column_name 
              from information_schema.columns
              where table_schema=database() 
                and table_name='{0}' and column_key='PRI' order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_cols(db,tab):
    cr_source = db.cursor()
    v_col=''
    v_sql="""select concat('`',column_name,'`') from information_schema.columns
              where table_schema=database() and table_name='{0}'  order by ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

#获取非主键列以外的列名列表
def get_sync_table_cols_pkid(db,tab):
    return ','.join(get_sync_table_cols(db, tab).split(',')[1:])

def get_sync_table_pk_vals(db,tab):
    cr_source  = db.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CAST(" + i[0] + " as char)," + "\'^^^\'" + ","
    cr_source.close()
    return 'CONCAT('+v_col[0:-7]+')'

def get_sync_table_pk_vals2(db,tab):
    cr_source  = db.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col +  i[0] + ","
    cr_source.close()
    return v_col[0:-1]

def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def get_sync_incr_max_rq(db,tab):
    cr_desc = db.cursor()
    v_tab   = tab.split(':')[0]
    v_rq    = tab.split(':')[1]
    if v_rq=='':
       return ''
    v_sql   = "select max({0}) from {1}".format(v_rq,v_tab)
    cr_desc.execute(v_sql)
    rs=cr_desc.fetchone()
    db.commit()
    cr_desc.close()
    return rs[0]

def get_pk_vals_mysql(db,ftab,v_where):
    cr        = db.cursor()
    tab       = ftab.split(':')[0]
    v_pk_cols = get_sync_table_pk_vals(db, tab)
    v_sql     = "select {0} from {1} {2}".format(v_pk_cols, tab,v_where)
    cr.execute(v_sql)
    rs        = cr.fetchall()
    l_pk_vals = []
    for i in list(rs):
        l_pk_vals.append(i[0])
    cr.close()
    return l_pk_vals

def check_tab_index(db,tab,col):
    cr = db.cursor()
    v_sql = """SHOW INDEX FROM {0}
             """.format(tab)
    cr.execute(v_sql)
    rs = cr.fetchall()
    for i in rs:
        if col==i[4]:
           return True
    cr.close()
    return False

def archive_check(config):
    db_sour = config['db_mysql_sour']
    db_desc = config['db_mysql_desc']
    v_tab   = config['sour_table']
    v_col   = config['archive_time_col']
    v_where = get_archive_where(config)

    # 1.检测源库是归档列上是否存在索引
    if not check_tab_index(db_sour,v_tab,v_col):
        config['table_name']  = v_tab
        config['create_date'] = get_time()
        config['start_time']  = get_time()
        config['end_time']    = get_time()
        config['duration']    = 0
        config['amount']      = 0
        config['percent']     = 0
        config['message']     = "源库归档列 {0} 无索引!".format(config['archive_time_col'])
        write_archive_log(config)
        print(config['message'])
        return False

    # 2.检测源库是否存在数据
    if get_mysql_tab_rows_where(db_sour,v_tab,v_where)==0:
        config['table_name']  = v_tab
        config['create_date'] = get_time()
        config['start_time']  = get_time()
        config['end_time']    = get_time()
        config['duration']    = 0
        config['amount']      = 0
        config['percent']     = 0
        config['message']     = '源库未找到数据!'
        write_archive_log(config)
        print(config['message'])
        return False

    # 3.检测目标库中是否已经有数据
    if config['if_cover'] == '0':
        if get_mysql_tab_rows_where(db_desc, v_tab, v_where) > 0:
            config['table_name']  = v_tab
            config['create_date'] = get_time()
            config['start_time']  = get_time()
            config['end_time']    = get_time()
            config['duration']    = 0
            config['amount']      = 0
            config['percent']     = 0
            config['message']     = '目标库已存在数据!'
            write_archive_log(config)
            print(config['message'])
            return False

    return True

def archive_mysql_init(config,debug):
    try:
        for i in config['sour_table'].split(","):
            tab=i.split(':')[0]
            if (check_mysql_tab_exists(config,tab)==0 or check_mysql_tab_exists(config,tab)>0):
                i_counter        = 0
                ins_sql_header   = get_tab_header(config,tab)
                n_batch_size     = int(config['batch_size'])
                db_source        = config['db_mysql_sour']
                cr_source        = db_source.cursor()
                db_desc          = config['db_mysql_desc']
                cr_desc          = db_desc.cursor()
                start_time       = datetime.datetime.now()
                start_time_v     = get_time()
                v_where          = get_archive_where(config)
                v_start ,v_end   = get_archive_rq(config)
                v_pk_names       = get_sync_table_pk_names(db_source, tab)
                v_pk_cols        = get_sync_table_pk_vals(db_source, tab)

                print("Archiving table '{0}' rentition recent {1} {2}...".format(tab,str(config['rentition_time']),config['rentition_time_type_cn']))
                n_tab_total_rows = get_sync_table_rows(db_source, tab, v_where)
                v_sql = """select {0} as 'pk',{1} from {2} {3}""".format(v_pk_cols, get_sync_table_cols(db_source, tab),
                                                                         tab, v_where)
                print('Execute Query:{0},Total rows:{1}'.format(v_sql,n_tab_total_rows))

                cr_source.execute(v_sql)
                rs_source = cr_source.fetchmany(n_batch_size)

                while rs_source:
                    batch_sql =''
                    batch_sql_del = ""
                    v_sql = ''
                    v_sql_del = ''
                    for r in list(rs_source):
                        rs_source_desc = cr_source.description
                        ins_val = ""
                        for j in range(1, len(r)):
                            col_type = str(rs_source_desc[j][1])
                            if r[j] is None:
                                ins_val = ins_val + "null,"
                            elif col_type == '253':  # varchar,date
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                            elif col_type in ('1', '3', '8', '246'):  # int,decimal
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                            elif col_type == '12':  # datetime
                                ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                            else:
                                ins_val = ins_val + "'" + format_sql(str(r[j])) + "',"
                        v_sql = v_sql + '(' + ins_val[0:-1] + '),'
                        v_sql_del = v_sql_del + get_sync_where(v_pk_names, r[0]) + ","
                    batch_sql = ins_sql_header + v_sql[0:-1]

                    if config['if_cover'] == '1':
                        print('delete batch data...')
                        for d in v_sql_del[0:-1].split(','):
                            cr_desc.execute('delete from {0} where {1}'.format(tab, d))

                    cr_desc.execute(batch_sql)
                    i_counter = i_counter + len(rs_source)

                    if n_tab_total_rows == 0:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows,i_counter,round(i_counter / 1 * 100,2)), end='')
                        config['table_name']  = tab
                        config['create_date'] = start_time_v
                        config['start_time']  = v_start
                        config['end_time']    = v_end
                        config['duration']    = str(get_seconds(start_time))
                        config['amount']      = str(i_counter)
                        config['percent']     = str(round(i_counter / 1 * 100,2))
                        config['message']     = ''
                        write_archive_log(config)
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows,i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
                        config['table_name']  = tab
                        config['create_date'] = start_time_v
                        config['start_time']  = v_start
                        config['end_time']    = v_end
                        config['duration']    = str(get_seconds(start_time))
                        config['amount']      = str(i_counter)
                        config['percent']     = str(round(i_counter / n_tab_total_rows * 100, 2))
                        config['message']     = ''
                        write_archive_log(config)

                    rs_source = cr_source.fetchmany(n_batch_size)
                db_desc.commit()
                print('')

                print('comparing source and dest data...')
                n_desc_rows = get_sync_table_rows(db_desc, tab, v_where)
                if n_tab_total_rows == n_desc_rows:
                    print("Comparing source and dest data...ok!")
                    print("Archive rq range:'{0}' ~ '{1}'".format(v_start,v_end))
                    print("Archive {0} rows.".format(n_desc_rows))
                    print('Deleting sour:{0},table {1} data...'.format(config['archive_db_sour'],tab))
                    v_sql = 'delete from {0} {1}'.format(tab,v_where)
                    print(v_sql)
                    cr_source.execute(v_sql)
                    db_source.commit()
                    print('Deleting table {0} data ok!'.format(tab))
                else:
                    print('Table data not equal,source:{0} rows, dest:{1} rows '.format(n_tab_total_rows,n_desc_rows))

    except Exception as e:
        print('archive_mysql_init exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def init(config,debug):
    config = get_config_from_db(config)

    if debug:
       print_dict(config)
    return config

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
                  <tr><td>任务标识</td><td>$$transfer_tag$$</td></tr>    
                  <tr><td>传输服务器</td><td>$$server_desc$$</td></tr>
                  <tr><td>源数据源</td><td>$$transfer_db_sour$$</td></tr>
                  <tr><td>目标数据源</td><td>$$transfer_db_dest$$</td></tr>
                  <tr><td>同步表名</td><td>$$sour_table$$</td></tr>
                  <tr><td>同步脚本</td><td>$$script_file$$</td></tr>
                  <tr><td>异常信息</td><td>$$run_error$$</td></tr>
              </table>                
           </body>
        </html>
       '''
    v_title   = '数据同步数据库异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$archive_tag$$' , config.get('archive_tag'))
    v_content = v_content.replace('$$server_desc$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$archive_db_sour$$', config.get('archive_db_sour'))
    v_content = v_content.replace('$$archive_db_dest$$', config.get('archive_db_dest'))
    v_content = v_content.replace('$$sour_table$$', config.get('sour_table'))
    v_content = v_content.replace('$$script_file$$', config.get('script_file'))
    v_content = v_content.replace('$$run_error$$', str(p_error))
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

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

def exception_running(config,p_error):
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
               <tr><td>任务标识</td><td>$$archive_tag$$</td></tr>
               <tr><td>传输服务器</td><td>$$server_desc$$</td></tr>
               <tr><td>源数据源</td><td>$$archive_db_sour$$</td></tr>
               <tr><td>目标数据源</td><td>$$archive_db_dest$$</td></tr>
               <tr><td>同步表名</td><td>$$sour_table$$</td></tr>
               <tr><td>同步脚本</td><td>$$script_file$$</td></tr>             
               <tr><td>异常信息</td><td>$$run_error$$</td></tr>
           </table>                
        </body>
     </html>
    '''
    v_title   = '数据传输运行异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$'    ,config.get('comments'))
    v_content = v_content.replace('$$archive_tag$$'  ,config.get('archive_tag'))
    v_content = v_content.replace('$$server_desc$$'   ,str(config.get('server_desc')))
    v_content = v_content.replace('$$archive_db_sour$$'  ,config.get('archive_db_sour'))
    v_content = v_content.replace('$$archive_db_dest$$'  ,config.get('archive_db_dest'))
    v_content = v_content.replace('$$sour_table$$'    ,config.get('sour_table'))
    v_content = v_content.replace('$$script_file$$'  , config.get('script_file'))
    v_content = v_content.replace('$$run_error$$'     ,str(p_error))
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

def archive(config,debug):
    #init sync table
    archive_mysql_init(config, debug)

def main():
    #init variable
    config = ""
    debug = False
    check = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    # 初始化
    config=init(config,debug)

    # 建表
    archive_mysql_ddl(config, debug)

    # 归档检测
    if archive_check(config):
      # 数据同步
      archive(config, debug)

if __name__ == "__main__":
     main()
