#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:15
# @Author : ma.fei
# @File : monitor.py.py
# @Software: PyCharm

import json
import tornado
import traceback
from utils.common import  db_config_info,DateEncoder
from model.monitor import get_db_monitor_config, \
    save_monitor_log, \
    push, save_api_log


class read_config_monitor(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await get_db_monitor_config(tag)
            self.write(json.dumps(res,cls=DateEncoder))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class push_script_remote_monitor(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            print('tag=',tag)
            res = await push(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_stack()
            self.write({'code': -1, 'msg': str(e)})

class write_monitor_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_monitor_log(json.loads(tag))
        self.write(json.dumps(res))

class write_api_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_api_log(json.loads(tag))
        self.write(json.dumps(res))

class read_config_db(tornado.web.RequestHandler):
    def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            res = {}
            res['code'] = 200
            res['msg']  = db_config_info()
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})