#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/10/29
# @Author : ma.fei
# @File : db_agent.py
# @Software: PyCharm

import sys
import pymysql
import pymssql
import datetime
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
import traceback
import pymongo
from clickhouse_driver import connect
from   tornado.options  import define
from   aiomysql import create_pool,DictCursor

class async_processer:
    async def query_list_by_ds(p_ds, p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    v_list = []
                    rs = await cur.fetchall()
                    dc = cur.description
                    for r in rs:
                        v_list.append(list(r))
        return v_list,dc

    async def query_one_by_ds(p_ds, p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    rs = await cur.fetchone()
        return rs

    async def exec_sql_by_ds(p_ds,p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)

    async def query_dict_list_by_ds(p_ds, p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(p_sql)
                    v_list = []
                    rs = await cur.fetchall()
                    for r in rs:
                        v_list.append(r)
        return v_list

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
                           charset='utf8',read_timeout=30)
    return conn

def get_ds_mysql_dict(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service,
                           charset='utf8',read_timeout=30,cursorclass = pymysql.cursors.DictCursor)
    return conn

def get_ds_sqlserver(ip, port, service, user, password):
    conn = pymssql.connect(host=ip, port=int(port), user=user, password=password, database=service, charset='utf8',timeout=30)
    return conn

def get_ds_ck(ip,port,service ,user,password):
    return connect('clickhouse://{}:{}@{}:{}/{}'.format(user,password,ip,port,service))


async def aes_decrypt(db,p_password,p_key):
    sql="""select aes_decrypt(unhex('{0}'),'{1}') as password """.format(p_password,p_key[::-1])
    rs = await async_processer.query_one(sql)
    print('aes_decrypt=',str(rs['password'],encoding = "utf-8"))
    return str(rs['password'],encoding = "utf-8")

def get_ds(db_ip,db_port,db_service,db_user,db_pass):
    ds = {}
    ds['ip']       = db_ip
    ds['port']     = db_port
    ds['user']     = db_user
    ds['password'] = db_pass
    ds['service']  = db_service
    return ds

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
        db = get_ds_sqlserver(db_ip, db_port, db_service, db_user, db_pass)
        try:
            cr   = db.cursor(as_dict=False)
            st   = '''select
                            lower(DB_NAME()) as db_name,
                            lower(OBJECT_SCHEMA_NAME(id)) as schema_name,    
                            lower(OBJECT_NAME(id)) as table_name
                        from sysobjects  where xtype='U' order by 3'''
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
                            lower(a.name) as name,
                            lower(b.name) as type 
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
                           lower(a.name) as name,
                           lower(b.name) as type 
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

class get_mssql_query(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")

        try:
            db         = get_ds_sqlserver(db_ip, db_port, db_service, db_user, db_pass)
            cr         = db.cursor(as_dict=False)
            columns    = []
            data       = []

            cr.execute(db_sql)
            rs = cr.fetchall()

            # process desc
            desc = cr.description
            for i in range(len(desc)):
                columns.append({"title": desc[i][0]})

            # process data
            for i in rs:
                tmp = []
                for j in range(len(desc)):
                    if i[j] is None:
                        tmp.append('')
                    else:
                        tmp.append(str(i[j]))
                data.append(tmp)

            result['code'] = 200
            result['msg'] = ''
            result['data'] = data
            result['column'] = columns
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)
        finally:
            db.close()

class get_mssql_query_dict(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")
        try:
            db         = get_ds_sqlserver(db_ip, db_port, db_service, db_user, db_pass)
            cr         = db.cursor(as_dict=True)
            cr.execute(db_sql)
            rs = cr.fetchall()

            result['code'] = 200
            result['msg'] = ''
            result['data'] = rs
            result['column'] = ''
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)
        finally:
            db.close()

class get_mysql_databases(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        ds         = get_ds(db_ip,db_port,db_service,db_user,db_pass)
        st         = """SELECT schema_name FROM information_schema.`SCHEMATA` 
                          WHERE schema_name NOT IN('mysql','information_schema','performance_schema')  order by 1"""
        try:
            rs, _ = await async_processer.query_list_by_ds(ds, st)
            result['code'] = 200
            result['msg']  = rs
            self.write(result)
        except:
            result['code'] = -1
            result['msg']  = traceback.format_exc()
            self.write(result)

class get_mysql_tables(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        ds         = get_ds(db_ip,db_port,db_service,db_user,db_pass)
        st         = '''select
                            lower(DATABASE()) as db_name,
                            '' as schema_name,    
                            lower(table_name) as table_name
                        from information_schema.tables  where table_schema='{}' order by 3'''.format(db_service)
        try:
            rs, _ = await async_processer.query_list_by_ds(ds, st)
            result['code'] = 200
            result['msg']  = rs
            self.write(result)
        except:
            result['code'] = -1
            result['msg']  = traceback.format_exc()
            self.write(result)

class get_mysql_columns(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_tab     = self.get_argument("db_tab").split('.')[2]
        ds         = get_ds(db_ip,db_port,db_service,db_user,db_pass)
        st         = '''SELECT
                             column_name AS NAME,
                             data_type  AS TYPE
                        FROM information_schema.columns  
                        WHERE table_schema='{}' 
                          AND table_name='{}'	
                        ORDER BY ordinal_position'''.format(db_service,db_tab)
        try:
            rs,_ = await async_processer.query_list_by_ds(ds,st)
            result['code'] = 200
            result['msg']  = rs
            self.write(result)
        except:
            result['code'] = -1
            result['msg']  = traceback.format_exc()
            self.write(result)

class get_mysql_incr_columns(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_tab     = self.get_argument("db_tab").split('.')[2]
        ds         = get_ds(db_ip, db_port, db_service, db_user, db_pass)
        st = '''SELECT
                     column_name AS NAME,
                     data_type  AS TYPE
                FROM information_schema.columns  
                WHERE table_schema='{}' 
                  AND table_name='{}'	
                  AND data_type in('datetime','date','timestamp')
                ORDER BY ordinal_position'''.format(db_service, db_tab)
        try:
            rs, _ = await async_processer.query_list_by_ds(ds, st)
            result['code'] = 200
            result['msg']  = rs
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            self.write(result)

class get_mysql_query(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")
        columns    = []
        data       = []
        try:
            ds = get_ds(db_ip, db_port, db_service, db_user, db_pass)
            rs,desc  = await async_processer.query_list_by_ds(ds,db_sql)
            for i in range(len(desc)):
                columns.append({"title": desc[i][0]})

            # process data
            for i in rs:
                tmp = []
                for j in range(len(desc)):
                    if i[j] is None:
                        tmp.append('')
                    else:
                        tmp.append(str(i[j]))
                data.append(tmp)

            result['code'] = 200
            result['msg'] = ''
            result['data'] = data
            result['column'] = columns
            print('get_mysql_query=',result)
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)

def get_ds_mongo(ip,port,replset):
    conn = pymongo.MongoClient(host=ip, port=int(port),replicaSet=replset)
    return conn

def get_ds_mongo_auth(p_ip,p_port,p_serice,p_user,p_password):
    conn      = pymongo.MongoClient('mongodb://{0}:{1}/'.format(p_ip,int(p_port)))
    db        = conn[p_serice]
    db.authenticate(p_user, p_password)
    return conn

class get_mongo_databases(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        try:
            if db_pass == '':
                db_mongo = get_ds_mongo(db_ip,db_port)
            else:
                db_mongo = get_ds_mongo_auth(db_ip,db_port,db_service,db_user, db_pass)

            data = db_mongo.list_database_names()
            result['code'] = 200
            result['msg'] = ''
            result['data'] = data
            print('get_mysql_query=',result)
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            self.write(result)

class get_mongo_collections(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        try:
            if db_pass == '':
                db_mongo = get_ds_mongo(db_ip,db_port)
            else:
                db_mongo = get_ds_mongo_auth(db_ip,db_port,db_service,db_user, db_pass)

            data = db_mongo[db_service].collection_names()
            result['code'] = 200
            result['msg'] = ''
            result['data'] = data
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            self.write(result)

class get_mongo_query(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")
        columns    = []
        data       = []
        try:
            if db_pass == '':
               db_conn = get_ds_mongo(db_ip,db_port)
            else:
               db_conn = get_ds_mongo_auth(db_ip,db_port,db_service,db_user, db_pass)

            db = db_conn[db_service]
            rs = db.command(db_sql)
            print('rs=',rs)

            #rs,desc  = await async_processer.query_list_by_ds(ds,db_sql)

            # for i in range(len(desc)):
            #     columns.append({"title": desc[i][0]})
            #
            # # process data
            # for i in rs:
            #     tmp = []
            #     for j in range(len(desc)):
            #         if i[j] is None:
            #             tmp.append('')
            #         else:
            #             tmp.append(str(i[j]))
            #     data.append(tmp)

            result['code'] = 200
            result['msg'] = ''
            result['data'] = ''
            result['column'] = ''
            print('get_mysql_query=',result)
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)

class get_mysql_query_dict(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")
        try:
            ds = get_ds(db_ip, db_port, db_service, db_user, db_pass)
            result['code'] = 200
            result['msg'] = ''
            result['data'] = await async_processer.query_dict_list_by_ds(ds,db_sql)
            result['column'] = ''
            print('get_mysql_query_dict=',result)
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)

class get_ck_databases(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db         = get_ds_ck(db_ip, db_port, db_service, db_user, db_pass)
        print('db=',db)
        cr         = db.cursor()
        st         = """select name from system.databases d 
                        where name not in('information_schema','INFORMATION_SCHEMA','system','default') order by name """
        try:
           v_list = []
           cr.execute(st)
           rs = cr.fetchall()
           print('get_ck_databases=',rs)
           for r in rs:
              v_list.append(r[0])
           result['code'] = 200
           result['msg'] = rs
           cr.close()
           self.write(result)
        except:
           result['code'] = -1
           result['msg'] = traceback.format_exc()
           self.write(result)

class get_ck_tables(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db         = get_ds_ck(db_ip, db_port, db_service, db_user, db_pass)
        cr         = db.cursor()
        st         = '''select
                            lower(database) as db_name,
                            '' as schema_name,    
                            lower(name) as table_name
                        from system.tables  where database='{}' order by 3'''.format(db_service)
        try:
            cr.execute(st)
            rs = cr.fetchall()
            print('rs=',rs)
            result['code'] = 200
            result['msg']  = rs
            self.write(result)
        except:
            result['code'] = -1
            result['msg']  = traceback.format_exc()
            self.write(result)

class get_ck_query(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")
        columns    = []
        data       = []
        try:
            db   = get_ds_ck(db_ip, db_port, db_service, db_user, db_pass)
            cr   = db.cursor()
            cr.execute(db_sql)
            rs   = cr.fetchall()
            desc = cr.description
            print('rs=',rs)
            print('desc=',desc)
            for i in range(len(desc)):
                columns.append({"title": desc[i][0]})

            # process data
            for i in rs:
                tmp = []
                for j in range(len(desc)):
                    if i[j] is None:
                        tmp.append('')
                    else:
                        tmp.append(str(i[j]))
                data.append(tmp)

            result['code'] = 200
            result['msg'] = ''
            result['data'] = data
            result['column'] = columns
            print('get_mysql_query=',result)
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)

class get_ck_query_dict(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header("Access-Control-Allow-Origin", '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        result     = {}
        db_ip      = self.get_argument("db_ip")
        db_port    = self.get_argument("db_port")
        db_service = self.get_argument("db_service")
        db_user    = self.get_argument("db_user")
        db_pass    = self.get_argument("db_pass")
        db_sql     = self.get_argument("db_sql")
        columns    = []
        data       = []
        try:
            db   = get_ds_ck(db_ip, db_port, db_service, db_user, db_pass)
            cr   = db.cursor()
            cr.execute(db_sql)
            rs   = cr.fetchall()
            desc = cr.description
            for i in range(len(desc)):
                columns.append({"title": desc[i][0]})

            # process data
            for i in rs:
                tmp = []
                for j in range(len(desc)):
                    if i[j] is None:
                        tmp.append('')
                    else:
                        tmp.append(str(i[j]))
                    data.append(dict(zip([d[0] for d in desc],tmp)))

            result['code'] = 200
            result['msg'] = ''
            result['data'] = data
            result['column'] = columns
            print('get_mysql_query=',result)
            self.write(result)
        except:
            result['code'] = -1
            result['msg'] = traceback.format_exc()
            result['data'] = ''
            result['column'] = ''
            self.write(result)

class health(tornado.web.RequestHandler):
    def get(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write('health check success!')

define("port", default=sys.argv[1], help="run on the given port", type=int)
class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            # 健康检查接口
            (r"/health", health),
            # sqlserver 数据库查询接口
            (r"/get_mssql_tables",       get_mssql_tables),
            (r"/get_mssql_columns",      get_mssql_columns),
            (r"/get_mssql_incr_columns", get_mssql_incr_columns),
            (r"/get_mssql_query",        get_mssql_query),
            (r"/get_mssql_query_dict",   get_mssql_query_dict),

            # mysql 数据库查询接口
            (r"/get_mysql_tables",       get_mysql_tables),
            (r"/get_mysql_columns",      get_mysql_columns),
            (r"/get_mysql_incr_columns", get_mysql_incr_columns),
            (r"/get_mysql_query",        get_mysql_query),
            (r"/get_mysql_query_dict",   get_mysql_query_dict),
            (r"/get_mysql_databases",    get_mysql_databases),

            # mongo 数据库查询接口
            (r"/get_mongo_databases",    get_mongo_databases),
            (r"/get_mongo_collections",  get_mongo_collections),
            (r"/get_mongo_query",        get_mongo_query),

            # clickhouse 数据库查询接口
            (r"/get_ck_databases",       get_ck_databases),
            (r"/get_ck_tables",          get_ck_tables),
            (r"/get_ck_query",           get_ck_query),
            (r"/get_ck_query_dict",      get_ck_query_dict),

            # ElasticSearch 数据库查询接口
            (r"/get_es_indexes", get_mongo_databases),
            (r"/get_es_mapping", get_mongo_collections),
            (r"/get_es_query", get_mongo_query),

            # Redis 数据库查询接口
            (r"/get_redis_dbs", get_mongo_databases),
            (r"/get_redis_query", get_mongo_collections),

        ]
        tornado.web.Application.__init__(self, handlers)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(sys.argv[1])
    print('Db Agent Api Server running {0} port ...'.format(sys.argv[1]))
    tornado.ioloop.IOLoop.instance().start()



