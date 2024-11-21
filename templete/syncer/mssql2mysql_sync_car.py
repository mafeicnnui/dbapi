#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongodb.py
# @Software: PyCharm
# 功能：北京朝合反向寻车实时同步

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

def get_sync_time_type_name(sync_time_type):
    if sync_time_type=="day":
       return '天'
    elif sync_time_type=="hour":
       return '小时'
    elif sync_time_type=="min":
       return '分'
    else:
       return ''

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

def get_config_from_db(tag,workdir):
    try:
        values = {
            'tag': tag
        }
        print('values=', values)
        url = 'http://$$API_SERVER$$/read_config_sync'
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
            config['sync_time_type_name'] = get_sync_time_type_name(config['sync_time_type'])
            db_sour_ip      = config['sync_db_sour'].split(':')[0]
            db_sour_port    = config['sync_db_sour'].split(':')[1]
            db_sour_service = config['sync_db_sour'].split(':')[2]
            db_sour_user    = config['sync_db_sour'].split(':')[3]
            db_sour_pass    = aes_decrypt(config['sync_db_sour'].split(':')[4],db_sour_user)
            db_dest_ip      = config['sync_db_dest'].split(':')[0]
            db_dest_port    = config['sync_db_dest'].split(':')[1]
            db_dest_service = config['sync_db_dest'].split(':')[2]
            db_dest_user    = config['sync_db_dest'].split(':')[3]
            db_dest_pass    = aes_decrypt(config['sync_db_dest'].split(':')[4],db_dest_user)
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
            config['db_sqlserver2']        = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                       db_sour_pass)
            config['db_mysql']             = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
            config['db_sqlserver3']        = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user,
                                                       db_sour_pass)
            config['db_mysql3']            = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
            config['run_mode']             = 'remote'

            #write local config file
            write_local_config_file(config)
            return config
        else:
            print('接口调用失败!,{0}'.format(res['msg']))
            sys.exit(0)
    except Exception as e :
        file_name = workdir + '/config/' + tag + '.ini'
        print('接口调用失败:{0}'.format(str(e)))
        print('从本地读取最近一次配置文件：{0}'.format(file_name))
        config = get_config(file_name,tag)
        return config

def write_log(msg):
    file_name   = '/tmp/mysql_sync.log'
    file_handle = open(file_name, 'a+')
    file_handle.write(msg + '\n')
    file_handle.close()

def write_sync_log(config):
    v_tag = {
        'sync_tag'        : config['sync_tag'],
        'create_date'     : get_time(),
        'duration'        : config['sync_duration'],
        'amount'          : config['sync_amount']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print(values)
    #write_log('values='+json.dump(values))
    url = 'http://$$API_SERVER$$/write_sync_log'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        write_log('Interface write_sync_log call successful!')
    else:
        write_log('Interface write_sync_log call failed!')

def write_sync_log_detail(config):
    v_tag = {
        'sync_tag'        : config['sync_tag'],
        'create_date'     : get_time(),
        'sync_table'      : config['sync_table_inteface'],
        'sync_amount'     : config['sync_amount'],
        'duration'        : config['sync_duration']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('write_sync_log_detail=', values)
    #write_log('values='+json.dump(values))
    url     = 'http://$$API_SERVER$$/write_sync_log_detail'
    context = ssl._create_unverified_context()
    data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req     = urllib.request.Request(url, data=data)
    res     = urllib.request.urlopen(req, context=context)
    res     = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        write_log('Interface write_sync_log_detail call successful!')
    else:
        write_log('Interface write_sync_log_detail call failed!')

def get_config(fname,tag):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    sync_server_sour                  = cfg.get("sync","sync_db_server")
    sync_server_dest                  = cfg.get("sync","sync_db_mysql")
    config['sync_table']              = cfg.get("sync", "sync_table").lower()
    config['sync_col_name']           = cfg.get("sync", "sync_col_name").lower()
    config['sync_col_val']            = cfg.get("sync", "sync_col_val").lower()
    config['batch_size']              = cfg.get("sync", "batch_size")
    config['batch_size_incr']         = cfg.get("sync", "batch_size_incr")
    config['sync_gap']                = cfg.get("sync", "sync_gap")
    config['sync_time_type']          = cfg.get("sync", "sync_time_type")
    config['sync_time_type_name']     = get_sync_time_type_name(config['sync_time_type'])
    db_sour_ip                        = sync_server_sour.split(':')[0]
    db_sour_port                      = sync_server_sour.split(':')[1]
    db_sour_service                   = sync_server_sour.split(':')[2]
    db_sour_user                      = sync_server_sour.split(':')[3]
    db_sour_pass                      = sync_server_sour.split(':')[4]
    db_dest_ip                        = sync_server_dest.split(':')[0]
    db_dest_port                      = sync_server_dest.split(':')[1]
    db_dest_service                   = sync_server_dest.split(':')[2]
    db_dest_user                      = sync_server_dest.split(':')[3]
    db_dest_pass                      = sync_server_dest.split(':')[4]
    config['db_sqlserver_ip']         = db_sour_ip
    config['db_sqlserver_port']       = db_sour_port
    config['db_sqlserver_service']    = db_sour_service
    config['db_sqlserver_user']       = db_sour_user
    config['db_sqlserver_pass']       = db_sour_pass
    config['db_mysql_ip']             = db_dest_ip
    config['db_mysql_port']           = db_dest_port
    config['db_mysql_service']        = db_dest_service
    config['db_mysql_user']           = db_dest_user
    config['db_mysql_pass']           = db_dest_pass
    config['db_sqlserver_string']     = db_sour_ip+':'+db_sour_port+'/'+db_sour_service
    config['db_mysql_string']         = db_dest_ip+':'+db_dest_port+'/'+db_dest_service
    config['db_sqlserver']            = get_ds_sqlserver(db_sour_ip,db_sour_port ,db_sour_service,db_sour_user,db_sour_pass)
    config['db_sqlserver2']           = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
    config['db_mysql']                = get_ds_mysql(db_dest_ip,db_dest_port ,db_dest_service,db_dest_user,db_dest_pass)
    config['db_sqlserver3']           = get_ds_sqlserver(db_sour_ip, db_sour_port, db_sour_service, db_sour_user, db_sour_pass)
    config['db_mysql3']               = get_ds_mysql(db_dest_ip, db_dest_port, db_dest_service, db_dest_user, db_dest_pass)
    config['run_mode']                = 'local'
    config['sync_tag']                = tag
    return config

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
    cr=config['db_sqlserver3'].cursor()
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
      #s1=s1+rs[i][0].lower()+','
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
    s1=s1+get_sync_table_cols(config,tab)+','+config['sync_col_name']+")"
    #s1=s1+config['sync_col_name']+")"
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
                    order by a.id,a.colorder""".format(tab)
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
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    db_desc   = config['db_mysql']
    cr_desc   = db_desc.cursor()
    for i in config['sync_table'].split(","):
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
            tab_name = rs_source[j][2].lower()
            tab_prefix = (str(rs_source[j][1]) + '.').lower()
            full_tab_name = rs_source[j][4].lower()
            if check_sqlserver_tab_exists_pk(config,tab)==0:
               print("DB:{0},Table:{1} not exist primary,ignore!".format(config['db_sqlserver_string'],full_tab_name))
               sys.exit(0)
            else:
               #编写函数完成生成创表语句
               v_ddl_sql = f_get_table_ddl(config,full_tab_name)
               v_cre_sql = v_ddl_sql.replace(full_tab_name,get_mapping_tname(full_tab_name))
               #print(v_cre_sql)
               #print(convert(v_cre_sql))
               if check_mysql_tab_exists(config,get_mapping_tname(full_tab_name))>0:
                   print("DB:{0},Table :{1} already exists!".format(config['db_mysql_string'],get_mapping_tname(full_tab_name)))
               else:
                  print(v_cre_sql)
                  print(convert(v_cre_sql))
                  cr_desc.execute(convert(v_cre_sql))
                  print("Table:{0} creating success!".format(get_mapping_tname(full_tab_name)))
                  cr_desc.execute('alter table {0} add primary key ({1})'.format(get_mapping_tname(full_tab_name),get_sync_table_pk_names(config, full_tab_name)))
                  print("Table:{0} add primary key {1} success!".format(get_mapping_tname(full_tab_name),get_sync_table_pk_names(config, full_tab_name)))

                  #add new columns.
                  for ac in config['sync_col_name'].split(','):
                      print('alter table {0} add {1} varchar(50)'.format(get_mapping_tname(full_tab_name),ac))
                      cr_desc.execute('alter table {0} add {1} varchar(50)'.format(get_mapping_tname(full_tab_name),ac))
                      print("Table:{0} add column {1} success!".format(get_mapping_tname(full_tab_name),config['sync_col_name']))
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

def get_sync_table_total_rows(config,tab,v_where):
    db_source = config['db_sqlserver']
    cr_source = db_source.cursor()
    v_sql="select count(0) from {0} with(nolock) {1}".format(tab,v_where)
    #print('get_sync_table_total_rows=',v_sql)
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
                    WHERE  sc.id = col.id  AND sc.colid = col.colid)=1
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
              where col.id=obj.id and  obj.id=object_id('{0}')
               order by col.colid
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

def get_sqlserver_row_strings(config,tab,pkid):
    db_source = config['db_sqlserver3']
    cr_source  = db_source.cursor()
    v_tab_cols = get_tab_columns(config, tab)
    v_pk_names = get_sync_table_pk_names(config, tab)
    v_pk_where = get_sync_where(v_pk_names,pkid)
    v_sql      = "select {0} from {1} with(nolock) where {2} order by {3}".format(v_tab_cols,tab,v_pk_where,v_pk_names)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    ins_val=''
    for i in range(len(rs_source)):
        rs_source_desc = cr_source.description
        ins_val = ""
        for j in range(len(rs_source[i])):
            col_type = str(rs_source_desc[j][1])
            if rs_source[i][j] is None:
                ins_val = ins_val + ","
            elif col_type == "1":  #varchar,date
                ins_val = ins_val + format_sql(str(rs_source[i][j])) + ","
            elif col_type == "5":  #int,decimal
                ins_val = ins_val + str(rs_source[i][j]) + ","
            elif col_type == "4":  #datetime
                ins_val = ins_val + str(rs_source[i][j]).split('.')[0] + ","
            elif col_type == "3":  #bit
                if str(rs_source[i][j]) == "True":     #bit
                    ins_val = ins_val + "1" + ","
                elif str(rs_source[i][j]) == "False":  #bit
                    ins_val = ins_val + "0" + ","
                else:  #bigint ,int
                    ins_val = ins_val + str(rs_source[i][j]) + ","
            elif col_type == "2":  #timestamp
                ins_val = ins_val + ","
            else:
                ins_val = ins_val + str(rs_source[i][j]) + ","
    cr_source.close()
    return ins_val

def get_mysql_row_strings(config, tab, pkid):
    db_source  = config['db_mysql3']
    cr_source  = db_source.cursor()
    v_tab_cols = get_tab_columns(config, tab)
    v_pk_names = get_sync_table_pk_names(config, tab)
    v_pk_where = get_sync_where(v_pk_names, pkid)
    v_sql      = "select {0} from {1} where {2} order by {3}".format(v_tab_cols, get_mapping_tname(tab), v_pk_where,v_pk_names)
    #('get_mysql_row_strings=',v_sql)
    cr_source.execute(v_sql)
    rs_source = cr_source.fetchall()
    ins_val=''
    for i in range(len(rs_source)):
        rs_source_desc = cr_source.description
        #print("\nget_mysql_row_strings.rs_source_desc=",rs_source_desc)
        ins_val = ""
        for j in range(len(rs_source[i])):
            col_type = str(rs_source_desc[j][1])
            if rs_source[i][j] is None:
                ins_val = ins_val + ","
            elif col_type == '253':  #varchar,date
                ins_val = ins_val + format_sql(str(rs_source[i][j])) + ","
            elif col_type in('1','3','8','246'):  #int,decimal
                ins_val = ins_val + str(rs_source[i][j]) + ","
            elif col_type == '12':  #datetime
                ins_val = ins_val + str(rs_source[i][j]).split('.')[0] + ","
            else:
                ins_val = ins_val + str(rs_source[i][j]) + ","
    cr_source.close()
    #print("get_mysql_row_strings.ins_val=",ins_val)
    return ins_val

def get_sqlserver_row_strings_batch(config,tab,rs):
    db_source  = config['db_sqlserver3']
    cr_source  = db_source.cursor()
    v_tab_cols = get_tab_columns(config, tab)
    v_pk_names = get_sync_table_pk_names(config, tab)
    v_ins_batch=''
    for r in range(len(rs)):
        pkid = str(rs[r][0])
        v_pk_where = get_sync_where(v_pk_names,pkid)
        v_sql      = "select {0} from {1} with(nolock) where {2} order by {3}".format(v_tab_cols,tab,v_pk_where,v_pk_names)
        cr_source.execute(v_sql)
        rs_source  = cr_source.fetchall()
        for i in range(len(rs_source)):
            rs_source_desc = cr_source.description
            ins_val = ""
            for j in range(len(rs_source[i])):
                col_type = str(rs_source_desc[j][1])
                if rs_source[i][j] is None:
                    ins_val = ins_val + ","
                elif col_type == "1":  # varchar,date
                    ins_val = ins_val + format_sql(str(rs_source[i][j])) + ","
                elif col_type == "5":  # int,decimal
                    ins_val = ins_val + str(rs_source[i][j]) + ","
                elif col_type == "4":  # datetime
                    ins_val = ins_val + str(rs_source[i][j]).split('.')[0] + ","
                elif col_type == "3":  # bit
                    if str(rs_source[i][j]) == "True":  # bit
                        ins_val = ins_val + "1" + ","
                    elif str(rs_source[i][j]) == "False":  # bit
                        ins_val = ins_val + "0" + ","
                    else:  # bigint ,int
                        ins_val = ins_val + str(rs_source[i][j]) + ","
                elif col_type == "2":  # timestamp
                    ins_val = ins_val + ","
                else:
                    ins_val = ins_val + str(rs_source[i][j]) + ","
            v_ins_batch=v_ins_batch+ins_val+'|'
    cr_source.close()
    return v_ins_batch

def get_mysql_row_strings_batch(config, tab, rs):
    db_source  = config['db_mysql3']
    cr_source  = db_source.cursor()
    v_tab_cols = get_tab_columns(config, tab)
    v_pk_names = get_sync_table_pk_names(config, tab)
    v_ins_batch = ''
    for r in range(len(rs)):
        pkid = str(rs[r][0])
        v_pk_where = get_sync_where(v_pk_names, pkid)
        v_sql      = "select {0} from {1} where {2} order by {3}".format(v_tab_cols, get_mapping_tname(tab), v_pk_where,v_pk_names)
        cr_source.execute(v_sql)
        rs_source = cr_source.fetchall()
        for i in range(len(rs_source)):
            rs_source_desc = cr_source.description
            ins_val = ""
            for j in range(len(rs_source[i])):
                col_type = str(rs_source_desc[j][1])
                if rs_source[i][j] is None:
                    ins_val = ins_val + ","
                elif col_type == '253':  # varchar,date
                    ins_val = ins_val + format_sql(str(rs_source[i][j])) + ","
                elif col_type in ('1', '3', '8', '246'):  # int,decimal
                    ins_val = ins_val + str(rs_source[i][j]) + ","
                elif col_type == '12':  # datetime
                    ins_val = ins_val + str(rs_source[i][j]).split('.')[0] + ","
                else:
                    ins_val = ins_val + str(rs_source[i][j]) + ","
            v_ins_batch = v_ins_batch + ins_val+'|'
    cr_source.close()
    return v_ins_batch

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

def get_md5(str):
    hash = hashlib.md5()
    hash.update(str.encode('utf-8'))
    return (hash.hexdigest())

def sync_sqlserver_init(config,debug):
    config_init = {}
    for i in config['sync_table'].split(","):
        tab=i.split(':')[0]
        config_init[tab] = False
        if (check_mysql_tab_exists(config,get_mapping_tname(tab))==0 \
                or (check_mysql_tab_exists(config,get_mapping_tname(tab))>0 and check_mysql_tab_sync(config,get_mapping_tname(tab))==0)):
            #write init dict
            config_init[tab] = True
            #start first sync data
            i_counter        = 0
            start_time       = datetime.datetime.now()
            n_tab_total_rows = get_sync_table_total_rows(config,tab,'')
            ins_sql_header   = get_tab_header(config,tab)
            v_tab_cols       = get_tab_columns(config,tab)
            v_pk_name        = get_sync_table_pk_names(config,tab)
            v_pk_cols        = get_sync_table_pk_vals(config, tab)
            n_batch_size     = int(config['batch_size'])
            db_source        = config['db_sqlserver']
            cr_source        = db_source.cursor()
            db_desc          = config['db_mysql']
            cr_desc          = db_desc.cursor()
            v_sql            = "select  {0} as 'pk',{1}  from {2} with(nolock)".format(v_pk_cols,get_tab_columns(config,tab),tab)
            cr_source.execute(v_sql)
            rs_source = cr_source.fetchmany(n_batch_size)
            while rs_source:
                batch_sql  = ""
                v_sql      = ''
                for r in list(rs_source):
                    rs_source_desc = cr_source.description
                    ins_val = ""
                    for j in range(1,len(r)):
                        col_type = str(rs_source_desc[j][1])
                        if   r[j] is None:
                            ins_val = ins_val + "null,"
                        elif col_type == "1":  #varchar,date
                            ins_val = ins_val + "'"+ format_sql(str(r[j])) + "',"
                        elif col_type == "5":  #int,decimal
                            ins_val = ins_val + "'" + str(r[j])+ "',"
                        elif col_type == "4":  #datetime
                            ins_val = ins_val + "'" + str(r[j]).split('.')[0] + "',"
                        elif col_type == "3":  # bit
                            if  str(r[j]) == "True":  # bit
                                ins_val = ins_val + "'" + "1" + "',"
                            elif str(r[j]) == "False":  # bit
                                ins_val = ins_val + "'" + "0" + "',"
                            else:  # bigint ,int
                                ins_val = ins_val + "'" + str(r[j]) + "',"
                        elif col_type == "2":  # timestamp
                            ins_val = ins_val + "null,"
                        else:
                            ins_val = ins_val + "'" +str(r[j]) + "',"
                    v_sql = v_sql +'('+ins_val+config['sync_col_val'].replace('$PK$',r[0].replace('^^^','_'))+'),'

                batch_sql = ins_sql_header + v_sql[0:-1]

                #noinspection PyBroadException
                try:
                  cr_desc.execute(batch_sql)
                  i_counter = i_counter +len(rs_source)
                  db_desc.commit()
                except:
                  print(traceback.format_exc())
                  print(batch_sql)
                  sys.exit(0)

                print("\rTime:{0},Table:{1},Total rec:{2},Process rec:{3},Complete:{4}%,elapsed time:{5}s"
                      .format(get_time(),tab,n_tab_total_rows, i_counter, round(i_counter / n_tab_total_rows * 100,2),str(get_seconds(start_time))), end='')
                if n_tab_total_rows == 0:
                    print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                          format(tab, n_tab_total_rows,round(i_counter / 1 * 100,2)), end='')
                else:
                    print("\rTable:{0},Total rec:{1},Process rec:{2},Complete:{3}%".
                          format(tab, n_tab_total_rows,i_counter,
                                 round(i_counter / n_tab_total_rows * 100, 2)), end='')
                rs_source = cr_source.fetchmany(n_batch_size)
            db_desc.commit()
            print('')
    return config_init

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

def check_mysql_exists_pk(config,ftab,v_where):
    db       = config['db_mysql']
    cr       = db.cursor()
    v_tab    = ftab.split(':')[0]
    v_sql    = 'select count(0) from {0} where {1}'.format(get_mapping_tname(v_tab),v_where)
    #print('check_mysql_exists_pk=',v_sql)
    cr.execute(v_sql)
    rs=cr.fetchone()
    #print('rs=',rs)
    if rs[0]>0 :
       return True
    else:
       return False


def calc_pk_minus(mysql,sqlserver):
    minus = []
    for i in mysql:
        if i not in sqlserver:
            minus.append(i)
    return minus

def get_temp_table_cols(v_pk_names):
    result=''
    for i in range(v_pk_names.count(',')+1):
        result=result+'v{0},'.format(str(i))
    return result[0:-1]

def sync_sqlserver_data_pk(config,ftab,config_init):
    #start sync dml data
    if not config_init[ftab.split(':')[0]]:
        tab              = ftab.split(':')[0]
        v_where          = get_sync_where_incr(ftab,config)
        i_counter        = 0
        i_counter_upd    = 0
        n_tab_total_rows = get_sync_table_total_rows(config,tab,v_where)
        ins_sql_header   = get_tab_header(config,tab)
        v_pk_names       = get_sync_table_pk_names(config, tab)
        v_pk_names_mysql = get_sync_table_pk_names_mysql(config, tab)
        v_pk_cols        = get_sync_table_pk_vals(config, tab)
        v_pk_cols_mysql  = get_sync_table_pk_vals_mysql(config, tab)
        n_batch_size     = int(config['batch_size_incr'])
        db_source        = config['db_sqlserver']
        db_source2       = config['db_sqlserver2']
        cr_source        = db_source.cursor()
        cr_source2       = db_source2.cursor()
        db_desc          = config['db_mysql']
        cr_desc          = db_desc.cursor()
        v_sql            = """select {0} as 'pk',{1} from {2} with(nolock) {3}
                           """.format(v_pk_cols,get_tab_columns(config,tab), tab,v_where)
        n_rows           = 0
        cr_source.execute(v_sql)
        rs_source        = cr_source.fetchmany(n_batch_size)
        start_time       = datetime.datetime.now()
        print('v_sql=',v_sql)
        if ftab.split(':')[1]=='':
            print("Sync Table increment :{0} ...".format(ftab.split(':')[0]),True)
        else:
            print("Sync Table increment :{0} for In recent {1} {2}...".
                  format(ftab.split(':')[0], ftab.split(':')[2],config['sync_time_type']),True)

        while rs_source:
            v_sql_tmp = ''
            v_sql_ins = ''
            v_sql_upd = ''
            v_sql_upd_tmp= ''
            n_rows=n_rows+len(rs_source)
            print("\r{0},Scanning table:{1},{2}/{3} rows,elapsed time:{4}s...".
                  format(get_time(),get_mapping_tname(tab),str(n_rows),
                         str(n_tab_total_rows),str(get_seconds(start_time))),end='')
            #time.sleep(1)
            rs_source_desc = cr_source.description
            if len(rs_source) > 0:
                for r in list(rs_source):
                    v_sql_tmp = ''
                    v_sql_upd_tmp = ''
                    for j in range(1, len(r)):
                        col_type = str(rs_source_desc[j][1])
                        if r[j] is None:
                            v_sql_tmp = v_sql_tmp + "null,"
                            v_sql_upd_tmp = v_sql_upd_tmp+"{0} = {1},".\
                                            format('`'+str(rs_source_desc[j][0])+'`','null')
                        elif col_type == "1":  # varchar,date
                            v_sql_tmp = v_sql_tmp + "'" + format_sql(str(r[j])) + "',"
                            v_sql_upd_tmp = v_sql_upd_tmp+"{0} = '{1}',".\
                                            format('`'+str(rs_source_desc[j][0])+'`',format_sql(str(r[j])))
                        elif col_type == "5":  # int,decimal
                            v_sql_tmp = v_sql_tmp + "'" + str(r[j]) + "',"
                            v_sql_upd_tmp = v_sql_upd_tmp + "{0} = '{1}',".\
                                            format('`'+str(rs_source_desc[j][0])+'`',str(r[j]))
                        elif col_type == "4":  # datetime
                            v_sql_tmp = v_sql_tmp + "'" + str(r[j]).split('.')[0] + "',"
                            v_sql_upd_tmp = v_sql_upd_tmp + "{0} = '{1}',".\
                                            format('`'+str(rs_source_desc[j][0])+'`',str(r[j]).split('.')[0])
                        elif col_type == "3":  # bit
                            if str(r[j]) == "True":  # bit
                                v_sql_tmp = v_sql_tmp + "'" + "1" + "',"
                                v_sql_upd_tmp = v_sql_upd_tmp + "{0} = '{1}',". \
                                                format('`'+str(rs_source_desc[j][0])+'`', '1')
                            elif str(r[j]) == "False":  # bit
                                v_sql_tmp = v_sql_tmp + "'" + "0" + "',"
                                v_sql_upd_tmp = v_sql_upd_tmp + "{0} = '{1}',". \
                                                format('`'+str(rs_source_desc[j][0])+'`', '0')
                            else:  # bigint ,int
                                v_sql_tmp = v_sql_tmp + "'" + str(r[j]) + "',"
                                v_sql_upd_tmp = v_sql_upd_tmp + "{0} = '{1}',". \
                                                format('`'+str(rs_source_desc[j][0])+'`', str(r[j]))
                        elif col_type == "2":  # timestamp
                            v_sql_tmp = v_sql_tmp + "null,"
                            v_sql_upd_tmp = v_sql_upd_tmp + "{0} = {1},". \
                                            format('`'+str(rs_source_desc[j][0])+'`', 'null')
                        else:
                            v_sql_tmp = v_sql_tmp + "'" + format_sql(str(r[j])) + "',"
                            v_sql_upd_tmp = v_sql_upd_tmp + "{0} = '{1}',". \
                                            format('`'+str(rs_source_desc[j][0])+'`', format_sql(str(r[j])))

                    v_where   = get_sync_where(v_pk_names_mysql, r[0])
                    v_sql_tmp = v_sql_tmp + config['sync_col_val'].replace('$PK$',r[0].replace('^^^','_'))
                    v_sql_ins = ins_sql_header  + '(' + v_sql_tmp+ ')'
                    v_sql_upd = 'update {0} set {1} where {2}'.format(get_mapping_tname(tab),v_sql_upd_tmp[0:-1],v_where)

                    if check_mysql_exists_pk(config, ftab,v_where ):
                       try:
                           cr_desc.execute(v_sql_upd)
                           i_counter_upd = i_counter_upd+1
                       except:
                           print(traceback.format_exc())
                           print('v_sql_upd=', v_sql_upd)
                           sys.exit(0)
                    else:
                       try:
                           cr_desc.execute(v_sql_ins)
                           i_counter = i_counter + 1
                       except:
                           print(traceback.format_exc())
                           print('v_sql_ins=', v_sql_ins)
                           sys.exit(0)

                if n_tab_total_rows == 0:
                    print("\rTable:{0},Total :{1},Process ins:{2},upd:{3},Complete:{4}%,elapsed time:{5}s"
                              .format(tab,
                                      n_tab_total_rows,
                                      i_counter,
                                      i_counter_upd,
                                      round((i_counter+i_counter_upd) / 1 * 100, 2),
                                      str(get_seconds(start_time))), False)
                else:
                    print( "\rTable:{0},Total :{1},Process ins:{2},upd:{3},Complete:{4}%,elapsed time:{5}s"
                              .format(tab,
                                       n_tab_total_rows,
                                      i_counter,
                                      i_counter_upd,
                                      round((i_counter+i_counter_upd) / n_tab_total_rows * 100, 2),
                                      str(get_seconds(start_time))), False)
                rs_source = cr_source.fetchmany(n_batch_size)
        print('')
        db_desc.commit()
        if config['run_mode'] == 'remote':
            config['sync_duration'] = str(get_seconds(start_time))
            config['sync_table_inteface'] = tab
            config['sync_amount'] = str(n_rows)
            write_sync_log_detail(config)

def sync_sqlserver_data(config,config_init):
    start_time = datetime.datetime.now()
    amount = 0
    for v in config['sync_table'].split(","):
        tab = v.split(':')[0]
        sync_sqlserver_data_pk(config, v,config_init)
        if config['run_mode'] == 'remote':
           amount = amount + int(config['sync_amount'])

    if config['run_mode'] == 'remote':
       #write write_sync_log_detail
       config['sync_amount']  = str(amount)
       config['sync_duration'] = str(get_seconds(start_time))
       write_sync_log(config)
    else:
       print('send mail warning....')
       send_mail25('190343@lifeat.cn',
                 'Hhc5HBtAuYTPGHQ8',
                 '190343@lifeat.cn',
                 '数据同步接口服务器异常',
'''接口服务器:$$API_SERVER$$ 不可用!
同步标识符:{0}
同步任务运行于本地配置文件模式中，请尽快处理!!!'''.format(config['sync_tag']))


def cleaning_table(config):
    print('starting cleaning_table please wait...')
    db   = config['db_mysql']
    cr   = db.cursor()
    desc = config['db_mysql_string']
    start_time = datetime.datetime.now()
    #如果索引不存在，则建立索引
    v_chk_idx_sql="SELECT count(0) FROM information_schema.innodb_sys_indexes WHERE NAME='idx_tc_recordarchive_n1'"
    v_cre_idx_sql="CREATE INDEX idx_tc_recordarchive_n1 ON tc_recordarchive(intime,carno,pkid)"

    #表数据去重
    n_cnt_rep_sql = 0
    v_cnt_rep_sql = """select count(0) from tc_recordarchive
                          where pkid in(select pkid from (select  max(pkid) AS pkid from tc_recordarchive 
                                         where indeviceentrytype=1
                                           group by carno,intime having count(0)>1) t)"""

    v_del_rep_sql = """delete from tc_recordarchive
                        where pkid in(select pkid from (select  max(pkid) AS pkid from tc_recordarchive 
                                       where indeviceentrytype=1
                                         group by carno,intime having count(0)>1) t)"""
    for i in config['sync_table'].split(","):
        tab = get_mapping_tname(i.split(':')[0])
        if tab=="tc_recordarchive":
           #创建索引
           print('DB:{0} cleaning table tc_recordarchive create index...'.format(desc))
           cr.execute(v_chk_idx_sql)
           rs = cr.fetchone()
           if rs[0]==0:
              print('DB:{0},createing index idx_tc_recordarchive_n1 for {1} please wait...'.format(desc,tab))
              cr.execute(v_cre_idx_sql)
              print('DB:{0},Table:{1} index idx_tc_recordarchive_n1 create complete!'.format(desc,tab))
           else:
               print('DB:{0} cleaning table tc_recordarchive index idx_tc_recordarchive_n1 already exists!')
           #删除重复数据
           print('DB:{0} cleaning table tc_recordarchive delete repeat data...'.format(desc))
           cr.execute(v_cnt_rep_sql)
           rs=cr.fetchone()
           if rs[0]>0:
             print('DB:{0},deleting table {1} repeat data please wait...'.format(desc,tab))
             cr.execute(v_del_rep_sql)
             print('DB:{0},Table:{1} delete repeat data {2} rows!'.format(desc,tab,rs[0]))
           else:
             print('DB:{0} cleaning table tc_recordarchive no repeat data!'.format(desc))

    db.commit()
    cr.close()
    print('complete cleaning_table,elaspse:{0}s'.format(str(get_seconds(start_time))))

def write_local_config_file(config):
    sync_db_sour = config['db_sqlserver_ip'] + ':' + \
                   config['db_sqlserver_port'] + ':' + \
                   config['db_sqlserver_service'] + ':' + \
                   config['db_sqlserver_user'] + ':' + \
                   config['db_sqlserver_pass']

    sync_db_dest = config['db_mysql_ip'] + ':' + \
                   config['db_mysql_port'] + ':' + \
                   config['db_mysql_service'] + ':' + \
                   config['db_mysql_user'] + ':' + \
                   config['db_mysql_pass']

    file_name = config['script_path'] + '/config/' + config['sync_tag'] + '.ini'
    file_handle = open(file_name, 'w')
    file_handle.write('[sync]' + '\n')
    file_handle.write('sync_db_server={0}\n'.format(sync_db_sour))
    file_handle.write('sync_db_mysql={0}\n'.format(sync_db_dest))
    file_handle.write('sync_table={0}\n'.format(config['sync_table']))
    file_handle.write('sync_col_name={0}\n'.format(config['sync_col_name']))
    file_handle.write('sync_col_val={0}\n'.format(config['sync_col_val']))
    file_handle.write('batch_size={0}\n'.format(config['batch_size']))
    file_handle.write('batch_size_incr={0}\n'.format(config['batch_size_incr']))
    file_handle.write('sync_time_type={0}\n'.format(config['sync_time_type']))
    file_handle.write('sync_gap={0}\n'.format(config['sync_gap']))
    file_handle.close()
    print("{0} export complete!".format(file_name))

def sync(config,debug,workdir):
    #init dict
    config = get_config_from_db(config,workdir)

    #print dict
    if debug:
       print_dict(config)

    #sync table ddl
    sync_sqlserver_ddl(config, debug)

    #init sync table
    config_init =sync_sqlserver_init(config, debug)

    #sync data
    while True:
      #sync increment data
      sync_sqlserver_data(config,config_init)

      #exit program
      sys.exit(0)


def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    #get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-workdir":
            workdir = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True
    #process
    sync(config, debug,workdir)

if __name__ == "__main__":
     main()
