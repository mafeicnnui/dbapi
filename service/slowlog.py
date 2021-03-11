#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:21
# @Author : ma.fei
# @File : slowlog.py.py
# @Software: PyCharm

import json
import tornado
import traceback

from model.slowlog import get_slow_config,\
                          save_slow_log,\
                          transfer_remote_file_slow,\
                          write_remote_crontab_slow,\
                          run_remote_cmd_slow

class read_slow_config(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id  = self.get_argument("slow_id")
            res = await get_slow_config(id)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_slow_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag  = self.get_argument("tag")
        res  = save_slow_log(json.loads(tag))
        self.write(json.dumps(res))

class push_script_slow_remote(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id  = self.get_argument("slow_id")
            res = await transfer_remote_file_slow(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_slow error!')

            res = await run_remote_cmd_slow(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd_slow error!')

            res = await write_remote_crontab_slow(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_slow error!')

            self.write(json.dumps(res))

        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})