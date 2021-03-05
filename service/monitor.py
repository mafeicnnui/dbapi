#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:15
# @Author : ma.fei
# @File : monitor.py.py
# @Software: PyCharm

import json
import tornado
import traceback
from utils.common import  db_config_info
from model.monitor import get_db_monitor_config,\
                          transfer_remote_file_monitor,\
                          write_remote_crontab_monitor,\
                          run_remote_cmd_monitor,\
                          save_monitor_log

class read_config_monitor(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await get_db_monitor_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class push_script_remote_monitor(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await transfer_remote_file_monitor(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_monitor error!')
            res = await write_remote_crontab_monitor(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_monitor error!')
            res = await run_remote_cmd_monitor(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd_monitor error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_stack()
            self.write({'code': -1, 'msg': str(e)})

class write_monitor_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_monitor_log(json.loads(tag))
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