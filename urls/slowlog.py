#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:39
# @Author  : ma.fei
# @File    : slowlog.py.py
# @func    ：slowlog interface
# @Software: PyCharm

from  service.slowlog  import read_slow_config,\
                              push_script_slow_remote,\
                              write_slow_log,\
                              write_slow_log_oracle,\
                              write_slow_log_mssql

slowlog = [
    (r"/read_slow_config",      read_slow_config),
    (r"/push_slow_remote",      push_script_slow_remote),
    (r"/write_slow_log",        write_slow_log),
    (r"/write_slow_log_oracle", write_slow_log_oracle),
    (r"/write_slow_log_mssql",  write_slow_log_mssql),
]



