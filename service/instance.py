#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:21
# @Author : ma.fei
# @File : instance.py.py
# @Software: PyCharm

import json
import tornado
import traceback

from model.instance import get_db_inst_config, \
    save_inst_log, \
    upd_inst_status, \
    upd_inst_reboot_status, \
    transfer_remote_file_inst, \
    write_remote_crontab_inst, \
    run_remote_cmd_inst, \
    mgr_remote_cmd_inst, \
    destroy_remote_cmd_inst, set_remote_cmd_inst


class read_db_inst_config(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id  = self.get_argument("inst_id")
            res = await get_db_inst_config(id)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class write_db_inst_log(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag     = self.get_argument("tag")
        result  = await save_inst_log(json.loads(tag))
        v_json  = json.dumps(result)
        self.write(v_json)

class update_db_inst_status(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag  = self.get_argument("tag")
        res  = await upd_inst_status(json.loads(tag))
        self.write(json.dumps(res))

class update_db_inst_reboot_status(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag  = self.get_argument("tag")
        res  = await upd_inst_reboot_status(json.loads(tag))
        self.write(json.dumps(res))

class create_remote_inst(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id  = self.get_argument("inst_id")

            res = await transfer_remote_file_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_inst error!')

            res = await write_remote_crontab_inst(id,'create')
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_inst error!')

            res = await run_remote_cmd_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd_inst error!')
            self.write(json.dumps(res))

        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class push_remote_inst(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id  = self.get_argument("inst_id")

            res = await transfer_remote_file_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_inst error!')

            res = await write_remote_crontab_inst(id,'push')
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_inst error!')

            res = await set_remote_cmd_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('set_remote_cmd_inst error!')
            self.write(json.dumps(res))

        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class manager_remote_inst(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id    = self.get_argument("inst_id")
            flag  = self.get_argument("op_type")

            res   = await transfer_remote_file_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_inst error!')

            res   = await mgr_remote_cmd_inst(id,flag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('mgr_remote_cmd_inst error!')

            self.write(json.dumps(res))

        except Exception as e:
            traceback.print_stack()
            self.write({'code': -1, 'msg': str(e)})

class destroy_remote_inst(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            id  = self.get_argument("inst_id")
            res = await transfer_remote_file_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file_inst error!')

            res = await write_remote_crontab_inst(id, 'destroy')
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_inst error!')

            res = await destroy_remote_cmd_inst(id)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab_inst error!')

            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_stack()
            self.write({'code': -1, 'msg': str(e)})