#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/30 9:31
# @Author  : 马飞
# @File    : sync_mysql2mongo.py
# @Software: PyCharm
# @func    ：mssql->mysql数据同步自动从dbops_api Server 获取配置文件
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

def get_db_sqlserver(config):
    return  get_ds_sqlserver(config['db_sqlserver_ip'],config['db_sqlserver_port'],\
                             config['db_sqlserver_service'],config['db_sqlserver_user'],config['db_sqlserver_pass'])

def get_db_mysql(config):
    return get_ds_mysql(config['db_mysql_ip'],config['db_mysql_port'],config['db_mysql_service'],\
                        config['db_mysql_user'],config['db_mysql_pass'])

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
    values = {'tag': tag }
    print('values=', values)
    url = 'http://$$API_SERVER$$/read_config_transfer'
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
            config = res['msg']
            db_sour_ip                     = config['transfer_db_sour'].split(':')[0]
            db_sour_port                   = config['transfer_db_sour'].split(':')[1]
            db_sour_service                = config['transfer_db_sour'].split(':')[2]
            db_sour_user                   = config['transfer_db_sour'].split(':')[3]
            db_sour_pass                   = aes_decrypt(config['transfer_db_sour'].split(':')[4],db_sour_user)
            db_dest_ip                     = config['transfer_db_dest'].split(':')[0]
            db_dest_port                   = config['transfer_db_dest'].split(':')[1]
            db_dest_service                = config['transfer_db_dest'].split(':')[2]
            db_dest_user                   = config['transfer_db_dest'].split(':')[3]
            db_dest_pass                   = aes_decrypt(config['transfer_db_dest'].split(':')[4],db_dest_user)
            config['db_sqlserver_ip']      = db_sour_ip
            config['db_sqlserver_port']    = db_sour_port
            config['db_sqlserver_service'] = db_sour_service
            config['db_sqlserver_user']    = db_sour_user
            config['db_sqlserver_pass']    = db_sour_pass
            config['db_mysql_ip']          = db_dest_ip
            config['db_mysql_port']        = db_dest_port
            config['db_mysql_service']     = db_dest_service
            config['db_mysql_user']        = db_dest_user
            config['db_mysql_pass']        = db_dest_pass
            config['db_sqlserver_string']  = db_sour_ip + ':' + db_sour_port + '/' + db_sour_service
            config['db_mysql_string']      = db_dest_ip + ':' + db_dest_port + '/' + db_dest_service
            config['db_sqlserver']         = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
            config['db_mysql']             = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
            return config
        except Exception as e:
            v_error = '从接口配置获取数据库连接对象时出现异常:{0}'.format(traceback.format_exc())
            print(v_error)
            exception_connect_db(config, v_error)
            exit(0)
    else:
        print('接口调用失败!,{0}'.format(res['msg']))  #发异常邮件
        v_title   = '数据同步接口异常[★]'
        v_content = '''<table class='xwtable'>
                           <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                           <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                           <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr>            
                       </table>'''
        v_content = v_content.replace('$$interface$$', url)
        v_content = v_content.replace('$$parameter$$', json.dumps(values))
        v_content = v_content.replace('$$error$$', res['msg'])
        exception_interface(v_title,v_content)
        sys.exit(0)

def write_transfer_log(config):
    v_tag = {
        'table_name'      : config['table_name'],
        'transfer_tag'    : config['transfer_tag'],
        'create_date'     : config['create_date'],
        'duration'        : config['duration'],
        'amount'          : config['amount'],
        'percent'         : config['percent']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    url = 'http://$$API_SERVER$$/write_transfer_log'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] != 200:
        print('Interface write_sync_log call failed!')


def check_mysql_tab_exists(config,tab):
   db=config['db_mysql']
   cr=db.cursor()
   sql="""select count(0) from information_schema.tables
            where table_schema='{0}' and table_name='{1}'""".format(config['db_mysql_service'],tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_col_exists(config,tab,col):
   db=config['db_mysql']
   cr=db.cursor()
   sql="""select count(0) from information_schema.columns
            where table_schema='{0}' and table_name='{1}' and column_name='{2}'""".format(config['db_mysql_service'],tab,col )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_mysql_tab_rows(config,tab):
   db=config['db_mysql']
   cr=db.cursor()
   sql="""select count(0) from {0}""".format(tab )
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists_data(config,tname):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tname)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_mysql_tab_sync(config,tab):
   db=config['db_mysql']
   cr=db.cursor()
   sql="select count(0) from {0}".format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def check_sqlserver_tab_exists_pk(config,tab):
   db=config['db_sqlserver']
   cr=db.cursor()
   sql = """select
             count(0)
            from syscolumns col, sysobjects obj
            where col.id=obj.id and obj.id=object_id('{0}')
            and  (select  1
                  from  dbo.sysindexes si
                      inner join dbo.sysindexkeys sik on si.id = sik.id and si.indid = sik.indid
                      inner join dbo.syscolumns sc on sc.id = sik.id    and sc.colid = sik.colid
                      inner join dbo.sysobjects so on so.name = si.name and so.xtype = 'pk'
                  where  sc.id = col.id  and sc.colid = col.colid)=1
         """.format(tab)
   cr.execute(sql)
   rs=cr.fetchone()
   cr.close()
   db.commit()
   return rs[0]

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def convert(v_sql):
    return v_sql.lower().replace("nvarchar","varchar").\
                  replace("varchar(-1)", "longtext").\
                  replace("datetime(23)","datetime"). \
                  replace("datetime2(27)", "datetime"). \
                  replace("time(16)","time").\
                  replace("date(10)","date").\
                  replace("numeric","decimal").\
                  replace("nvarchar","varchar").\
                  replace("money","DECIMAL").\
                  replace("identity(1,1)","").\
                  replace("smalldatetime(16)","datetime").\
                  replace("smalldatetime", "datetime").\
                  replace("float","decimal").\
                  replace("bit","varchar").\
                  replace("timestamp(8)","varchar(50)")

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def format_sql(v_sql):
    return v_sql.replace("\\","\\\\").replace("'","\\'")

def get_tab_columns(config,tab):
    cr=config['db_sqlserver'].cursor()
    sql="""select col.name
           from syscolumns col, sysobjects obj
           where col.id=obj.id 
             and obj.id=object_id('{0}')
           order by isnull((SELECT  'Y'
                            FROM  dbo.sysindexes si
                            INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                            inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                            where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
        """.format(tab)
    cr.execute(sql)
    rs=cr.fetchall()
    s1=""
    for i in range(len(rs)):
        s1 = s1 + '[' + rs[i][0].lower() + '],'
    cr.close()
    return s1[0:-1]

def get_tab_columns_incr(config,tab):
    cr=config['db_sqlserver'].cursor()
    sql="""select col.name
           from syscolumns col, sysobjects obj
           where col.id=obj.id 
             and obj.id=object_id('{0}')
           order by isnull((SELECT  'Y'
                            FROM  dbo.sysindexes si
                            INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                            inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                            where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid    
        """.format(tab)
    cr.execute(sql)
    rs=cr.fetchall()
    s1=""
    for i in range(len(rs)):
        print('get_sync_table_pk_names(config,tab)=',get_sync_table_pk_names(config,tab),rs[i][0].lower())
        if get_sync_table_pk_names(config,tab)!=rs[i][0].lower():
           s1 = s1 + '[' + rs[i][0].lower() + '],'
    cr.close()
    return s1[0:-1]

def get_tab_header(config,tab):
    cr=config['db_sqlserver'].cursor()
    sql="select top 1 * from {0}".format(tab)
    cr.execute(sql)
    desc=cr.description
    s1="insert into "+get_mapping_tname(tab.lower())+"("
    s2=" values "
    '''for i in range(len(desc)):
      s1=s1+desc[i][0].lower()+','
    '''
    s1=s1+get_sync_table_cols(config,tab)+")"
    cr.close()
    return s1+s2

def check_sync_sqlserver_col_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment = """SELECT  count(0)
                    FROM sys.tables A
                    INNER JOIN syscolumns B ON B.id = A.object_id
                    left join  systypes t   on b.xtype=t.xusertype
                    LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                    WHERE A.name = '{0}'  and c.value is not null        
                   """.format(tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_col_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_comment ="""SELECT                                
                        case when t.name ='numeric' then
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+','+
                            CAST(isnull(COLUMNPROPERTY(b.id,b.name,'Scale'),0) as varchar)	   
                           +') comment '''+CAST(c.value as varchar)+''''
                        when t.name in('nvarchar','varchar','int') then
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+'('+
                            cast(COLUMNPROPERTY(b.id,b.name,'PRECISION') AS varchar)+') comment '''+CAST(c.value as varchar)+''''
                        else
                          'alter table '+lower(A.name)+' modify column '+lower(B.name)+' '+t.name+' comment '''+CAST(c.value as varchar)+''''
                        end
                FROM sys.tables A
                INNER JOIN syscolumns B ON B.id = A.object_id
                left join  systypes t   on b.xtype=t.xusertype
                LEFT JOIN sys.extended_properties C ON C.major_id = B.id AND C.minor_id = B.colid
                WHERE A.name = '{0}'  and c.value is not null        
               """.format(tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchall()
    for j in range(len(rs_source)):
        v_ddl_sql = rs_source[j][0]
        cr_desc.execute(convert(v_ddl_sql))
    db_desc.commit()
    cr_desc.close()

def check_sync_sqlserver_tab_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_comment ="""select count(0)  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')  and a.name='{0}'""".format(tab,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    return rs_source[0]

def sync_sqlserver_tab_comments(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    v_comment ="""select 
                   'alter table '+lower(a.name)+' comment '''+cast(a.value as varchar)+''''
                  from sys.extended_properties A  
                  where A.major_id=object_id('{0}')
                    and a.name='{0}'""".format(tab,tab)
    cr_source.execute(v_comment)
    rs_source = cr_source.fetchone()
    if len(rs_source)>0:
        v_ddl_sql = rs_source[0]
        cr_desc.execute(v_ddl_sql)
        cr_desc.close()

def get_mapping_tname(tab):
    return tab.replace('.','_')

def f_get_table_ddl(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql     ="""SELECT       									
                    a.colorder 字段序号,
                    '`'+a.name+'`' 字段名,
                    b.name 类型,
                    COLUMNPROPERTY(a.id,a.name,'PRECISION') as 长度,
                    isnull(COLUMNPROPERTY(a.id,a.name,'Scale'),0) as 小数位,
                    (case when (SELECT count(*)
                        FROM sysobjects
                        WHERE (name in
                                (SELECT name
                                FROM sysindexes
                                WHERE (id = a.id) AND (indid in
                                         (SELECT indid
                                        FROM sysindexkeys
                                        WHERE (id = a.id) AND (colid in
                                                  (SELECT colid
                                                   FROM syscolumns
                                                  WHERE (id = a.id) AND (name = a.name))))))) AND
                            (xtype = 'PK'))>0 then '√' else '' end) 主键
                    FROM  syscolumns  a 
                    left join systypes b    on a.xtype=b.xusertype
                    inner join sysobjects d on a.id=d.id  and  d.xtype='U' and  d.name<>'dtproperties'
                    left join syscomments e on a.cdefault=e.id
                    left join sys.extended_properties g on a.id=g.major_id AND a.colid = g.major_id
                    where d.id=object_id('{0}') 
                    order by 6 desc,a.id,a.colorder""".format(tab)
    cr_source.execute(v_sql)
    rs=cr_source.fetchall()
    v_cre_tab= 'create table '+tab+'(';
    for i in range(len(rs)):
        v_name=rs[i][1]
        v_type=rs[i][2]
        v_len =str(rs[i][3])
        v_scale=str(rs[i][4])
        if v_type in('int','date'):
           v_cre_tab=v_cre_tab+'   '+v_name+'    '+v_type+','
        elif v_type in('numeric','decimal'):
           v_cre_tab = v_cre_tab + '   ' + v_name + '    ' + v_type +'('+ v_len+','+ v_scale+') ,'
        else:
           v_cre_tab = v_cre_tab + '   ' + v_name + '    ' + v_type + '(' + v_len +') ,'
    return v_cre_tab[0:-1]+')'

def sync_sqlserver_ddl(config,debug):
    try:
        db_source = config['db_sqlserver']
        cr_source = db_source.cursor()
        db_desc   = config['db_mysql']
        cr_desc   = db_desc.cursor()
        for i in config['sour_table'].split(","):
            tab=i.split(':')[0]
            cr_source.execute("""select id,
                                        OBJECT_SCHEMA_NAME(id) as schema_name, 
                                        OBJECT_NAME(id) as table_name,
                                        DB_NAME() as db_name,
                                        OBJECT_SCHEMA_NAME(id)+'.'+OBJECT_NAME(id) as full_table_name
                                 from sysobjects 
                                 where xtype='U' and id=object_id('{0}') order by name""".format(tab))
            rs_source = cr_source.fetchall()
            for j in range(len(rs_source)):
                tab_name      = rs_source[j][2].lower()
                tab_prefix    = (str(rs_source[j][1]) + '.').lower()
                full_tab_name = rs_source[j][4].lower()
                if check_sqlserver_tab_exists_pk(config,tab)==0:
                   print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_sqlserver_string'],full_tab_name))
                   v_ddl_sql = f_get_table_ddl(config, full_tab_name)
                   #print(v_ddl_sql)
                   v_cre_sql = v_ddl_sql.replace(full_tab_name, get_mapping_tname(full_tab_name))
                   if check_mysql_tab_exists(config, get_mapping_tname(full_tab_name)) > 0:
                       print("DB:{0},Table :{1} already exists!".format(config['db_mysql_string'],get_mapping_tname(full_tab_name)))
                   else:
                       cr_desc.execute(convert(v_cre_sql))
                       print("Table:{0} creating success!".format(get_mapping_tname(full_tab_name)))
                       v_pk_sql="""ALTER TABLE {0} ADD COLUMN pkid INT(11) NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (pkid)
                                """.format(get_mapping_tname(full_tab_name))
                       print("Table:{0} add primary key pkid success!".format(get_mapping_tname(full_tab_name)))
                       cr_desc.execute(v_pk_sql)
                       db_desc.commit()
                       #create mysql table comments
                       if check_sync_sqlserver_tab_comments(config, tab) > 0:
                           sync_sqlserver_tab_comments(config, tab)
                           print("Table:{0}  comments create complete!".format(tab))
                       #create mysql table column comments
                       if check_sync_sqlserver_col_comments(config, tab) > 0:
                           sync_sqlserver_col_comments(config, tab)
                           print("Table:{0} columns comments create complete!".format(tab))

                else:
                   #编写函数完成生成创表语句
                   v_ddl_sql = f_get_table_ddl(config,full_tab_name)
                   v_cre_sql = v_ddl_sql.replace(full_tab_name,get_mapping_tname(full_tab_name))

                   if check_mysql_tab_exists(config,get_mapping_tname(full_tab_name))>0:
                       print("DB:{0},Table :{1} already exists!".format(config['db_mysql_string'],get_mapping_tname(full_tab_name)))
                   else:
                      cr_desc.execute(convert(v_cre_sql))
                      print("Table:{0} creating success!".format(get_mapping_tname(full_tab_name)))
                      cr_desc.execute('alter table {0} add primary key ({1})'.format(get_mapping_tname(full_tab_name),get_sync_table_pk_names(config, full_tab_name)))
                      print("Table:{0} add primary key {1} success!".format(get_mapping_tname(full_tab_name),get_sync_table_pk_names(config, full_tab_name)))
                      db_desc.commit()
                      #create mysql table comments
                      if check_sync_sqlserver_tab_comments(config,tab)>0:
                         sync_sqlserver_tab_comments(config, tab)
                         print("Table:{0}  comments create complete!".format(tab))
                      #create mysql table column comments
                      if check_sync_sqlserver_col_comments(config,tab)>0:
                         sync_sqlserver_col_comments(config, tab)
                         print("Table:{0} columns comments create complete!".format(tab))

        cr_source.close()
        cr_desc.close()
    except Exception as e:
        print('sync_sqlserver_ddl exceptiion:'+traceback.format_exc())
        exception_running(config,traceback.format_exc())
        exit(0)

def get_sync_table_total_rows(config,tab,v_where):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql="select count(0) from {0} with(nolock) {1}".format(tab,v_where)
    cr_source.execute(v_sql)
    rs_source=cr_source.fetchone()
    cr_source.close()
    return  rs_source[0]

def get_sync_table_total_rows_mysql(config,tab,v_where):
    db_desc = config['db_mysql']
    cr_desc = db_desc.cursor()
    v_sql="select count(0) from {0} {1}".format(tab,v_where)
    cr_desc.execute(v_sql)
    rs_desc=cr_desc.fetchone()
    cr_desc.close()
    return  rs_desc[0]

def get_sync_table_pk_names(config,tab):
    #db_source = config['db_sqlserver']
    cr_source = get_db_sqlserver(config).cursor()
    v_col=''
    v_sql="""select col.name
              from syscolumns col, sysobjects obj
              where col.id=obj.id and  obj.id=object_id('{0}')
               and (SELECT  1
                    FROM dbo.sysindexes si
                        INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                        INNER JOIN dbo.syscolumns sc ON sc.id = sik.id    AND sc.colid = sik.colid
                        INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                    WHERE  sc.id = col.id  AND sc.colid = col.colid)=1  order by col.colid
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_cols(config,tab):
    #db_source = config['db_sqlserver']
    cr_source = get_db_sqlserver(config).cursor()
    v_col=''
    v_sql="""select '`'+col.name+'`'
               from syscolumns col, sysobjects obj
               where col.id = obj.id 
                 and obj.id = object_id('{0}')
               order by isnull((SELECT  'Y'
                                FROM  dbo.sysindexes si
                                INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                                inner join dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                                where sik.id=obj.id and sik.colid=col.colid),'N') desc,col.colid
         """.format(tab)

    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col=v_col+i[0]+','
    cr_source.close()
    return v_col[0:-1]

def get_sync_table_pk_vals(config,tab):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_col=''
    v_sql="""select col.name
              from syscolumns col, sysobjects obj
              where col.id=obj.id and  obj.id=object_id('{0}')
               and (SELECT  1
                    FROM dbo.sysindexes si
                        INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid
                        INNER JOIN dbo.syscolumns sc ON sc.id = sik.id    AND sc.colid = sik.colid
                        INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK'
                    WHERE  sc.id = col.id  AND sc.colid = col.colid)=1 order by col.colid
          """.format(tab)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CONVERT(varchar(100)," + i[0] + ", 20)+" + "\'^^^\'" + "+"
    cr_source.close()
    return v_col[0:-7]

def get_sync_table_pk_vals_mysql(config,tab):
    db_source = config['db_mysql']
    cr_source = db_source.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(get_mapping_tname(tab))
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + "CAST(" + i[0] + " as char)," + "\'^^^\'" + ","
    cr_source.close()
    return 'CONCAT('+v_col[0:-7]+')'

def get_sync_table_pk_names_mysql(config,tab):
    db_source = config['db_mysql']
    cr_source = db_source.cursor()
    v_col=''
    v_sql="""SELECT column_name FROM information_schema.`COLUMNS`
             WHERE table_schema=DATABASE()
               AND table_name='{0}' AND column_key='PRI'  ORDER BY ordinal_position
          """.format(get_mapping_tname(tab))
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    for i in list(rs_source):
        v_col = v_col + i[0]+","
    cr_source.close()
    return v_col[0:-1]

def get_sync_where(pk_cols,pk_vals):
    v_where=''
    for i in range(len(pk_cols.split(','))):
        v_where=v_where+pk_cols.split(',')[i]+"='"+pk_vals.split('^^^')[i]+"' and "
    return v_where[0:-4]

def get_sync_where_incr(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = 'where {0} >=DATEADD(DAY,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = 'where {0} >=DATEADD(HOUR,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = 'where {0} >=DATEADD(MINUTE,-{1},GETDATE())'.format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def get_sync_where_incr_mysql(tab,config):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} DAY)".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} HOUR)".format(v_rq_col, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >= DATE_SUB(NOW(),INTERVAL {1} MINUTE)".format(v_rq_col, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def get_sync_where_incr_rq(tab,config,currq):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >=DATEADD(DAY,-{1},'{2}')".format(v_rq_col, v_expire_time,currq)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >=DATEADD(HOUR,-{1},'{2}')".format(v_rq_col, v_expire_time,currq)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >=DATEADD(MINUTE,-{1},'{2}')".format(v_rq_col, v_expire_time,currq)
    else:
        v = ''
    if tab.split(':')[1]=='':
       return ''
    else:
       return v

def get_sync_where_incr_mysql_rq(tab,config,currq):
    v_rq_col=tab.split(':')[1]
    v_expire_time=tab.split(':')[2]
    v = ''
    if config['sync_time_type'] == 'day':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} DAY)".format(v_rq_col,currq, v_expire_time)
    elif config['sync_time_type'] == 'hour':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} HOUR)".format(v_rq_col,currq, v_expire_time)
    elif config['sync_time_type'] == 'min':
        v = "where {0} >= DATE_SUB('{1}',INTERVAL {2} MINUTE)".format(v_rq_col,currq, v_expire_time)
    else:
        v = ''
    if tab.split(':')[1] == '':
        return ''
    else:
        return v

def sync_sqlserver_init(config,debug):
    try:
        config_init = {}
        for i in config['sour_table'].split(","):
            tab=i.split(':')[0]
            if check_mysql_tab_exists(config,get_mapping_tname(tab))==0  or check_mysql_tab_exists(config,get_mapping_tname(tab))>0:
                i_counter        = 0
                n_tab_total_rows = get_sync_table_total_rows(config,tab,'')
                ins_sql_header   = get_tab_header(config,tab)
                v_tab_cols       = get_tab_columns(config,tab)
                v_pk_name        = get_sync_table_pk_names(config,tab)
                n_batch_size     = int(config['batch_size'])
                db_source        = config['db_sqlserver']
                cr_source        = db_source.cursor()
                db_desc          = config['db_mysql']
                cr_desc          = db_desc.cursor()
                start_time       = datetime.datetime.now()
                start_time_v     = get_time()

                if check_mysql_tab_sync(config, get_mapping_tname(tab)) > 0:
                    print('truncate table:{0} all data!'.format(get_mapping_tname(tab)))
                    cr_desc.execute('delete from {0}'.format(get_mapping_tname(tab)))
                    print('truncate table:{0} all data ok!'.format(get_mapping_tname(tab)))

                if config['sour_where'] == '':
                    print("Transfer table:'{0}' for full...".format(tab))
                    n_tab_total_rows = get_sync_table_total_rows(config, tab, '')
                    v_sql = "select {0} from {1} with(nolock)".format(v_tab_cols, tab)
                else:
                    print("Transfer table:'{0}' for conditioin...".format(tab))
                    n_tab_total_rows = get_sync_table_total_rows(config, tab, config['sour_where'])
                    v_sql = "select {0} from {1} with(nolock) {2}".format(v_tab_cols, tab,config['sour_where'])

                print('Execute Query:{0},Total rows:{1}'.format(v_sql, n_tab_total_rows))

                cr_source.execute(v_sql)
                rs_source = cr_source.fetchmany(n_batch_size)
                while rs_source:
                    batch_sql  = ""
                    v_sql      = ''
                    for i in range(len(rs_source)):
                        rs_source_desc = cr_source.description
                        ins_val = ""
                        for j in range(len(rs_source[i])):
                            col_type = str(rs_source_desc[j][1])
                            if  rs_source[i][j] is None:
                                ins_val = ins_val + "null,"
                            elif col_type == "1":  #varchar,date
                                ins_val = ins_val + "'"+format_sql(str(rs_source[i][j])) + "',"
                            elif col_type == "5":  #int,decimal
                                ins_val = ins_val + "'" + str(rs_source[i][j])+ "',"
                            elif col_type == "4":  #datetime
                                ins_val = ins_val + "'" + str(rs_source[i][j]).split('.')[0] + "',"
                            elif col_type == "3":  # bit
                                if str(rs_source[i][j]) == "True":  # bit
                                    ins_val = ins_val + "'" + "1" + "',"
                                elif str(rs_source[i][j]) == "False":  # bit
                                    ins_val = ins_val + "'" + "0" + "',"
                                else:  # bigint ,int
                                    ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                            elif col_type == "2":  # timestamp
                                ins_val = ins_val + "null,"
                            else:
                                ins_val = ins_val + "'" + str(rs_source[i][j]) + "',"
                        v_sql = v_sql +'('+ins_val[0:-1]+'),'
                    batch_sql = ins_sql_header + v_sql[0:-1]

                    cr_desc.execute(batch_sql)
                    db_desc.commit()
                    i_counter = i_counter +len(rs_source)

                    if n_tab_total_rows == 0:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows,i_counter,round(i_counter / 1 * 100,2)), end='')
                        config['table_name']  = tab
                        config['create_date'] = start_time_v
                        config['duration']    = str(get_seconds(start_time))
                        config['amount']      = str(i_counter)
                        config['percent']     = str(round(i_counter / 1 * 100,2))
                        write_transfer_log(config)
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows,i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
                        config['table_name']  = tab
                        config['create_date'] = start_time_v
                        config['duration']    = str(get_seconds(start_time))
                        config['amount']      = str(i_counter)
                        config['percent']     = str(round(i_counter / n_tab_total_rows * 100, 2))
                        write_transfer_log(config)

                    print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                          .format(get_time(),tab,n_tab_total_rows, i_counter,
                                  round(i_counter / n_tab_total_rows * 100,2),str(get_seconds(start_time))), end='')

                    if n_tab_total_rows == 0:
                        print("Table:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows,round(i_counter / 1 * 100,2)))
                    else:
                        print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                              format(tab, n_tab_total_rows,i_counter, round(i_counter / n_tab_total_rows * 100, 2)), end='')
                    rs_source = cr_source.fetchmany(n_batch_size)
                db_desc.commit()
                print('')
    except Exception as e:
        print('sync_sqlserver_init exceptiion:' + traceback.format_exc())
        exception_running(config, traceback.format_exc())
        exit(0)

def get_pk_vals_sqlserver(config,ftab):
    db_source  = config['db_sqlserver']
    cr_source  = db_source.cursor()
    tab        = ftab.split(':')[0]
    v_pk_cols  = get_sync_table_pk_vals(config, tab)
    v_sql      = "select {0} from {1} with(nolock) {2}".format(v_pk_cols, tab,get_sync_where_incr(ftab))
    cr_source.execute(v_sql)
    rs_source  = cr_source.fetchall()
    l_pk_vals  =[]
    for i in list(rs_source):
        l_pk_vals.append(i[0])
    cr_source.close()
    return l_pk_vals

def get_pk_vals_mysql(config,ftab):
    db_dest  = config['db_mysql']
    cr_dest  = db_dest.cursor()
    tab      = ftab.split(':')[0]
    v_pk_cols= get_sync_table_pk_vals_mysql(config, tab)
    v_sql    = "select {0} from {1} {2}".format(v_pk_cols, get_mapping_tname(tab),get_sync_where_incr_mysql(ftab))
    cr_dest.execute(v_sql)
    rs_dest  = cr_dest.fetchall()
    l_pk_vals=[]
    for i in list(rs_dest):
        l_pk_vals.append(i[0])
    cr_dest.close()
    return l_pk_vals


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
    v_title  = '数据同步数据库异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$', config.get('comments'))
    v_content = v_content.replace('$$transfer_tag$$'  , config.get('transfer_tag'))
    v_content = v_content.replace('$$server_desc$$', str(config.get('server_desc')))
    v_content = v_content.replace('$$transfer_db_sour$$', config.get('transfer_db_sour'))
    v_content = v_content.replace('$$transfer_db_dest$$', config.get('transfer_db_dest'))
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
    v_title   = '数据传输运行异常[★★★]'
    v_content = v_templete.replace('$$task_desc$$'        ,config.get('comments'))
    v_content = v_content.replace('$$transfer_tag$$'      ,config.get('transfer_tag'))
    v_content = v_content.replace('$$server_desc$$'       ,str(config.get('server_desc')))
    v_content = v_content.replace('$$transfer_db_sour$$'  ,config.get('transfer_db_sour'))
    v_content = v_content.replace('$$transfer_db_dest$$'  ,config.get('transfer_db_dest'))
    v_content = v_content.replace('$$sour_table$$'        ,config.get('sour_table'))
    v_content = v_content.replace('$$script_file$$'       , config.get('script_file'))
    v_content = v_content.replace('$$run_error$$'         ,str(p_error))
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_content)

def transfer(config,debug):
    #init dict
    config=get_config_from_db(config)

    #print dict
    if debug:
       print_dict(config)

    #sync table ddl
    sync_sqlserver_ddl(config, debug)

    #init sync table
    sync_sqlserver_init(config, debug)

def main():
    #init variable
    config = ''
    debug  = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True
    print('config=',config)
    if config=='':
       print('Please input tag value!')
       sys.exit(0)

    #process
    transfer(config, debug)

if __name__ == "__main__":
     main()
