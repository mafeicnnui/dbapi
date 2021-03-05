#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:15
# @Author : ma.fei
# @File : syncer.py.py
# @Software: PyCharm

import json
import tornado
import traceback
from model.syncer import get_db_sync_config,\
                         run_remote_cmd_sync,\
                         run_remote_sync_task,\
                         transfer_remote_file_sync,\
                         stop_remote_sync_task,\
                         write_remote_crontab_sync,\
                         save_sync_log,\
                         save_sync_log_detail

class read_config_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await get_db_sync_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})

class run_script_remote_sync(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await transfer_remote_file_sync(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_sync error!')

            res = await run_remote_cmd_sync(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd_sync error!')

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
            tag  = self.get_argument("tag")
            res  = await transfer_remote_file_sync(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_sync error!')

            res  = await run_remote_cmd_sync(tag)
            if res['code']!=200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd_sync error!')

            res  = await write_remote_crontab_sync(tag)
            if res['code']!=200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_sync error!')

            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_sync_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_sync_log(json.loads(tag))
        self.write(json.dumps(res))

class write_sync_log_detail(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag = self.get_argument("tag")
        res = await save_sync_log_detail(json.loads(tag))
        self.write(json.dumps(res))
