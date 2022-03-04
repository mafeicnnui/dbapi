#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:36
# @Author  : ma.fei
# @File    : monitor.py.py
# @func    : monitor interface
# @Software: PyCharm

from  service.monitor import read_config_db
from  service.alert  import read_config_alert,push_script_remote_alert

alert = [
    (r"/read_config_alert", read_config_alert),
    (r"/read_config_db", read_config_db),
    (r"/push_script_remote_alert", push_script_remote_alert),
]
