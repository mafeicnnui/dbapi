#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:36
# @Author  : ma.fei
# @File    : monitor.py.py
# @func    : monitor interface
# @Software: PyCharm

from  service.monitor  import read_config_monitor,\
                              read_config_db,\
                              push_script_remote_monitor,\
                              write_monitor_log

monitor = [
    (r"/read_config_monitor", read_config_monitor),
    (r"/read_config_db", read_config_db),
    (r"/push_script_remote_monitor", push_script_remote_monitor),
    (r"/write_monitor_log", write_monitor_log),
]
