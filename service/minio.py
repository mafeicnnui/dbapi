#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:21
# @Author : ma.fei
# @File : minio.py
# @Software: PyCharm

import json
import tornado
import traceback

from model.minio import get_minio_config,\
                        save_minio_log,\
                        transfer_remote_file_minio,\
                        write_remote_crontab_minio,\
                        run_remote_cmd_minio

class read_minio_config(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag = self.get_argument("tag")
            res = await get_minio_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_minio_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag  = self.get_argument("tag")
        res  = await save_minio_log(json.loads(tag))
        self.write(json.dumps(res))

class push_script_minio_remote(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await transfer_remote_file_minio(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_minio error!')

            res  = await write_remote_crontab_minio(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_minio error!')

            res  = await run_remote_cmd_minio(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd_minio error!')

            self.write(json.dumps(res))

        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})