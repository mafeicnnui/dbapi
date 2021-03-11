#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 13:08
# @Author : ma.fei
# @File : backup.py.py
# @func : backup interface
# @Software: PyCharm

from  utils.common    import read_db_decrypt
from  service.backup  import read_config_backup,write_backup_status,write_backup_total,\
                             write_backup_detail,push_script_remote,run_script_remote,stop_script_remote

backup = [
     (r"/read_config_backup",   read_config_backup),
     (r"/read_db_decrypt",      read_db_decrypt),
     (r"/write_backup_total",   write_backup_total),
     (r"/write_backup_detail",  write_backup_detail),
     (r"/update_backup_status", write_backup_status),
     (r"/push_script_remote",   push_script_remote),
     (r"/run_script_remote",    run_script_remote),
     (r"/stop_script_remote",   stop_script_remote),

]