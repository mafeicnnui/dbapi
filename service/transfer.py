#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:20
# @Author : ma.fei
# @File : transfer.py.py
# @Software: PyCharm

import json
import tornado
import traceback

from model.transfer import get_db_transfer_config,\
                           run_remote_transfer_task,\
                           stop_remote_transfer_task,\
                           save_transfer_log,\
                           push

class read_config_transfer(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag   = self.get_argument("tag")
            res  = await get_db_transfer_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class run_script_remote_transfer(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await run_remote_transfer_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_transfer_task error!')

            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class stop_script_remote_transfer(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await stop_remote_transfer_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('stop_remote_transfer_task error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class push_script_remote_transfer(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await push(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_stack()
            self.write({'code': -1, 'msg': str(e)})

class write_transfer_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_transfer_log(json.loads(tag))
        self.write(json.dumps(res))
