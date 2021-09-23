#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:23
# @Author : ma.fei
# @File : datax.py.py
# @Software: PyCharm

import json
import tornado
import traceback

from model.datax import get_datax_sync_config,\
                        run_remote_datax_task,\
                        stop_datax_sync_task,\
                        save_datax_sync_log,\
                        get_datax_sync_templete,\
                        push

class read_datax_config_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await get_datax_sync_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_datax_sync_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_datax_sync_log(json.loads(tag))
        self.write(json.dumps(res))

class run_datax_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await run_remote_datax_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_datax_task error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class stop_datax_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await stop_datax_sync_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('stop_datax_sync_task error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_stack()

class push_datax_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  =  await push(tag)
            self.write(json.dumps(res))
        except :
            e = traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class read_datax_templete(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        id   = self.get_argument("id")
        res  = await get_datax_sync_templete(id)
        self.write(json.dumps(res))