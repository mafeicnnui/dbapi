#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:15
# @Author : ma.fei
# @File : backup.py.py
# @Software: PyCharm

import json
import traceback
import tornado
from crontab import CronTab

from model.backup import get_db_config,\
                         get_task_tags,\
                         update_backup_status,\
                         save_backup_total,\
                         save_backup_detail, \
                         write_remote_crontab,\
                         transfer_remote_file,\
                         run_remote_backup_task,\
                         run_remote_cmd,\
                         stop_remote_backup_task

class read_config_backup(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag   = self.get_argument("tag")
            res  = await get_db_config(tag)
            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code':-1,'msg':str(e)})

class write_backup_status(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tags = await get_task_tags()
            for i in range(len(tags)):
                if not update_backup_status(tags[i]['db_tag']):
                   self.write({'code':-1,'msg':'update error!'})
                   raise Exception('update_backup_status error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write(str(e))

class write_backup_total(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag  = self.get_argument("tag")
        res  = await save_backup_total(json.loads(tag))
        self.write(json.dumps(res))

class write_backup_detail(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag  = self.get_argument("tag")
        res  = await save_backup_detail(json.loads(tag))
        self.write(json.dumps(res))

class set_crontab_local(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        v_tag    = self.get_argument("tag")
        v_msg    = await get_db_config(v_tag)
        v_cron   = CronTab(user=True)
        v_cmd    = '$PYTHON3_HOME/bin/python3 {0}/{1} -tag {2}'.format(v_msg['script_path'],v_msg['script_file'],v_msg['db_tag'])
        job      = v_cron.new(command=v_cmd)
        job.setall(v_msg['run_time'])
        job.enable()
        v_cron.write()
        result   = {'code':200,'msg':v_msg}
        v_json   = json.dumps(result)
        self.write(v_json)

class set_crontab_remote(tornado.web.RequestHandler):
    async def post(self):
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        tag    = self.get_argument("tag")
        res    = await write_remote_crontab(tag)
        self.write(json.dumps(res))

class push_script_remote(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await transfer_remote_file(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file error!')

            res = await run_remote_cmd(tag)
            if res['code'] != 200:
               self.write(json.dumps(res))
               raise Exception('run_remote_cmd error!')

            res = await write_remote_crontab(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('write_remote_crontab error!')

            self.write(json.dumps(res))
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class run_script_remote(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag   = self.get_argument("tag")
            res  = await transfer_remote_file(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('transfer_remote_file error!')

            res = await run_remote_cmd(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_cmd error!')

            res = await run_remote_backup_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('run_remote_backup_task error!')

            self.write({'code': 200, 'msg': 'success'})

        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})

class stop_script_remote(tornado.web.RequestHandler):
    async def post(self):
        try:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            tag  = self.get_argument("tag")
            res  = await stop_remote_backup_task(tag)
            if res['code'] != 200:
                self.write(json.dumps(res))
                raise Exception('stop_remote_backup_task error!')
            self.write({'code': 200, 'msg': 'success'})
        except Exception as e:
            traceback.print_exc()
            self.write({'code': -1, 'msg': str(e)})