#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:35
# @Author  : ma.fei
# @File    : archive.py.py
# @func    : archive interface
# @Software: PyCharm

from service.archiver import read_config_archive,\
                             push_script_remote_archive,\
                             write_archive_log,\
                             run_script_remote_archive,\
                             stop_script_remote_archive

archive = [
    (r"/read_config_archive", read_config_archive),
    (r"/push_script_remote_archive", push_script_remote_archive),
    (r"/write_archive_log", write_archive_log),
    (r"/run_script_remote_archive", run_script_remote_archive),
    (r"/stop_script_remote_archive", stop_script_remote_archive),
]