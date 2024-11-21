#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:15
# @Author : ma.fei
# @File : syncer.py.py
# @Software: PyCharm

import json
import tornado
import traceback
from model.syncer import \
    get_db_sync_config, \
    run_remote_sync_task, \
    stop_remote_sync_task, \
    save_sync_log, \
    save_sync_log_detail, \
    update_sync_status, \
    push, save_sync_real_log, \
    get_real_sync_status, set_real_sync_status

from utils.common import DateEncoder

class read_config_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await get_db_sync_config(tag)
            self.write(json.dumps(res, cls=DateEncoder))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})

class read_real_sync_status(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res  = await get_real_sync_status(tag)
            self.write(json.dumps(res, cls=DateEncoder))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})

class write_real_sync_status(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            status = self.get_argument("status")
            print('status=',status)
            res = await set_real_sync_status(tag,status)
            self.write(json.dumps(res, cls=DateEncoder))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})



class read_mysql_real_sync_status(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            res  = await get_mysql_real_sync_status()
            self.write(json.dumps(res, cls=DateEncoder))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})

class write_mysql_real_sync_status(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            status = self.get_argument("status")
            print('status=',status)
            res = await set_mysql_real_sync_status(status)
            self.write(json.dumps(res, cls=DateEncoder))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})


class run_script_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res = await run_remote_sync_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_sync_task error!')

            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class stop_script_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await stop_remote_sync_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('stop_remote_backup_task error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class push_script_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await push(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_sync_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_sync_log(json.loads(tag))
        self.write(json.dumps(res))


class write_sync_real_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_sync_real_log(json.loads(tag))
        self.write(json.dumps(res))


class write_sync_log_detail(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_sync_log_detail(json.loads(tag))
        self.write(json.dumps(res))

class write_sync_status(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag    = self.get_argument("tag")
            status = self.get_argument("status")
            res    = await update_sync_status(tag,status)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})