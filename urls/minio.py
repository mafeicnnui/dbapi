#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/2/23 13:40
# @Author  : ma.fei
# @File    : minio.py.py
# @func    : minio interface
# @Software: PyCharm

from service.minio   import read_minio_config,push_script_minio_remote,write_minio_log

slowlog = [
    (r"/read_minio_config", read_minio_config),
    (r"/push_minio_remote", push_script_minio_remote),
    (r"/write_minio_log", write_minio_log),
]

