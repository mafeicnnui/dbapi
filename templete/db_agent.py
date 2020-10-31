#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/10/29
# @Author : 马飞
# @File : db_agent.py
# @Software: PyCharm

import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
from   tornado.options  import define, options
import datetime,json
import pymysql
import pymssql
import os,sys
import traceback

def format_sql(v_sql):
    if v_sql is not None:
       return v_sql.replace("\\","\\\\").replace("'","\\'")
    else:
       return v_sql

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_time2():
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
        print(' '.ljust(3,' ')+key.ljust(20,' ')+'='+str(config[key]))
    print('-'.ljust(85,'-'))

def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8',cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_mysql2(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8')
    return conn

def get_db_mysql(config):
    return get_ds_mysql(config['db_ip'],config['db_port'],config['db_service'],config['db_user'],config['db_pass'])


def get_ds_sqlserver(ip, port, service, user, password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service, charset='utf8')
    return conn


def aes_decrypt(db,p_password,p_key):
    cr = db.cursor()
    sql="""select aes_decrypt(unhex('{0}'),'{1}') as password """.format(p_password,p_key[::-1])
    cr.execute(sql)
    rs=cr.fetchone()
    db.commit()
    cr.close()
    db.close()
    print('aes_decrypt=',str(rs['password'],encoding = "utf-8"))
    return str(rs['password'],encoding = "utf-8")


class get_mssql_tables(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        v_list     = []
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db         = get_ds_sqlserver(db_ip,db_port,db_service,db_user,db_pass)
        cr         = db.cursor(as_dict=False)
        st         = '''select
                            DB_NAME() as db_name,
                            OBJECT_SCHEMA_NAME(id) as schema_name,    
                            OBJECT_NAME(id) as table_name
                        from sysobjects  where xtype='U' order by 1'''
        try:
            cr.execute(st)
            for r in cr.fetchall():
                v_list.append(list(r))
            cr.close()
            db.commit()
            result['code'] = 200
            result['msg'] = v_list
            self.write(result)
        except:
            result['code'] = -1
            result['msg']  = traceback.format_exc()
            self.write(result)
        finally:
            db.close()


class get_mssql_columns(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        v_list     = []
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_tab     = self.get_argument("db_tab")
        db         = get_ds_sqlserver(db_ip,db_port,db_service,db_user,db_pass)
        cr         = db.cursor(as_dict=False)
        st         = '''SELECT           
                            a.name as name,
                            b.name as type 
                        FROM  syscolumns  a 
                        left join systypes b    on a.xtype=b.xusertype
                        inner join sysobjects d on a.id=d.id  and  d.xtype='U' and  d.name<>'dtproperties'
                        left join syscomments e on a.cdefault=e.id
                        left join sys.extended_properties g on a.id=g.major_id AND a.colid = g.major_id
                        where d.id=object_id('{}') 
                        order by 1 desc,a.id,a.colorder'''.format(db_tab)
        try:
            cr.execute(st)
            for r in cr.fetchall():
                v_list.append(list(r))
            result['code'] = 200
            result['msg']  = v_list
            self.write(result)
        except:
            result['code'] = -1
            result['msg']  = traceback.format_exc()
            self.write(result)
        finally:
            db.close()


class get_mssql_incr_columns(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        v_list     = []
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_tab     = self.get_argument("db_tab")
        db         = get_ds_sqlserver(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor(as_dict=False)
        st         = '''SELECT           
                           a.name as name,
                           b.name as type 
                       FROM  syscolumns  a 
                       left join systypes b    on a.xtype=b.xusertype
                       inner join sysobjects d on a.id=d.id  and  d.xtype='U' and  d.name<>'dtproperties'
                       left join syscomments e on a.cdefault=e.id
                       left join sys.extended_properties g on a.id=g.major_id AND a.colid = g.major_id
                       where d.id=object_id('{}') 
                         and b.name in('datetime')
                       order by 1 desc,a.id,a.colorder'''.format(db_tab)
        try:
            cr.execute(st)
            for r in cr.fetchall():
                v_list.append(list(r))
            result['code'] = 200
            result['msg'] = v_list
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            self.write(result)
        finally:
            db.close()

define("port", default=sys.argv[1], help="run on the given port", type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # MSSQL数据库查询接口
            (r"/get_mssql_tables",    get_mssql_tables),
            (r"/get_mssql_columns",   get_mssql_columns),
            (r"/get_mssql_incr_columns", get_mssql_incr_columns),
        ]
        tornado.web.Application.__init__(self, handlers)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(sys.argv[1])
    print('Db Agent Api Server running {0} port ...'.format(sys.argv[1]))
    tornado.ioloop.IOLoop.instance().start()



