#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/5/21 16:15
# @Author  : ma.fei
# @File    : bbgl.py
# @func    : bbgl interface
# @Software: PyCharm

from service.bbgl import read_config_bbgl,\
                             push_script_remote_bbgl,\
                             write_bbgl_log,\
                             run_script_remote_bbgl,\
                             stop_script_remote_bbgl

bbgl = [
    (r"/read_config_bbgl", read_config_bbgl),
    (r"/push_script_remote_bbgl", push_script_remote_bbgl),
    (r"/write_bbgl_log", write_bbgl_log),
    (r"/run_script_remote_bbgl", run_script_remote_bbgl),
    (r"/stop_script_remote_bbgl", stop_script_remote_bbgl),
]
