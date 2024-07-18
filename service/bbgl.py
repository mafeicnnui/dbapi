#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:15
# @Author : ma.fei
# @File : archiver.py.py
# @Software: PyCharm

import json
import traceback
import tornado
from model.bbgl import get_db_bbgl_config, save_bbgl_log, run_remote_bbgl_task, push, stop_remote_bbgl_task


class read_config_bbgl(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await get_db_bbgl_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_bbgl_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_bbgl_log(json.loads(tag))
        self.write(json.dumps(res))

class run_script_remote_bbgl(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await run_remote_bbgl_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_archive_task error!')

            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class stop_script_remote_bbgl(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await stop_remote_bbgl_task(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_stack()
            self.write({'code': -1, 'msg': str(e)})

class push_script_remote_bbgl(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await push(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})
