#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:37
# @Author  : ma.fei
# @File    : instance.py.py
# @func    : instance interface
# @Software: PyCharm

from   service.instance import read_db_inst_config,\
                               create_remote_inst,\
                               destroy_remote_inst,\
                               write_db_inst_log,\
                               update_db_inst_status,\
                               update_db_inst_reboot_status,\
                               manager_remote_inst,\
                               push_remote_inst

instance = [
    (r"/read_db_inst_config", read_db_inst_config),
    (r"/create_db_inst", create_remote_inst),
    (r"/push_db_inst", push_remote_inst),
    (r"/destroy_db_inst", destroy_remote_inst),
    (r"/write_db_inst_log", write_db_inst_log),
    (r"/update_db_inst_status", update_db_inst_status),
    (r"/update_db_inst_reboot_status", update_db_inst_reboot_status),
    (r"/manager_db_inst", manager_remote_inst),
]

