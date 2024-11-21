#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 13:32
# @Author : ma.fei
# @File : datax.py.py
# @func : datax interface
# @Software: PyCharm

from   service.datax    import push_datax_remote_sync,\
                               read_datax_config_sync,\
                               read_datax_templete,\
                               run_datax_remote_sync,\
                               stop_datax_remote_sync,\
                               write_datax_sync_log

datax = [
    (r"/push_datax_remote_sync", push_datax_remote_sync),
    (r"/read_datax_config_sync", read_datax_config_sync),
    (r"/read_datax_templete",    read_datax_templete),
    (r"/run_datax_remote_sync",  run_datax_remote_sync),
    (r"/stop_datax_remote_sync", stop_datax_remote_sync),
    (r"/write_datax_sync_log",   write_datax_sync_log),
]