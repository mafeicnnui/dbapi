#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:18
# @Author  : ma.fei
# @File    : syncer.py.py
# @func    : sync interface
# @Software: PyCharm

from  service.syncer import read_config_sync,\
                            push_script_remote_sync,\
                            write_sync_log,\
                            write_sync_real_log,\
                            write_sync_status,\
                            write_sync_log_detail,\
                            run_script_remote_sync,\
                            stop_script_remote_sync,\
                            read_real_sync_status,\
                            write_real_sync_status

syncer = [
    (r"/read_config_sync",        read_config_sync),
    (r"/write_sync_log",          write_sync_log),
    (r"/write_sync_log_detail",   write_sync_log_detail),
    (r"/write_sync_real_log",     write_sync_real_log),
    (r"/update_sync_status",      write_sync_status),
    (r"/push_script_remote_sync", push_script_remote_sync),
    (r"/run_script_remote_sync",  run_script_remote_sync),
    (r"/stop_script_remote_sync", stop_script_remote_sync),
    (r"/get_real_sync_status",    read_real_sync_status),
    (r"/set_real_sync_status",    write_real_sync_status),

]
