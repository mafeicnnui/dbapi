#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:34
# @Author  : ma.fei
# @File    : transfer.py.py
# @func    : transfer interface
# @Software: PyCharm

from  service.transfer import read_config_transfer,\
                              push_script_remote_transfer,\
                              write_transfer_log,\
                              run_script_remote_transfer,\
                              stop_script_remote_transfer

transfer = [
    (r"/read_config_transfer", read_config_transfer),
    (r"/push_script_remote_transfer", push_script_remote_transfer),
    (r"/write_transfer_log", write_transfer_log),
    (r"/run_script_remote_transfer", run_script_remote_transfer),
    (r"/stop_script_remote_transfer", stop_script_remote_transfer),
]

