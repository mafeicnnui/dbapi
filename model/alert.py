#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:05
# @Author : ma.fei
# @File : monitor.py.py
# @Software: PyCharm

import traceback
from utils.common import aes_decrypt,exec_ssh_cmd,gen_transfer_file,get_time2
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper

async def check_alert_config(p_tag):
    st = "select count(0) from t_alert_task where task_tag='{0}'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def check_server_alert_status(p_tag):
    st = "select count(0) from t_alert_task a,t_server b \
                  where a.server_id=b.id and a.task_tag='{0}' and b.status='0'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def get_itmes_from_templete_ids(p_templete):
    st = "SELECT index_code,index_threshold FROM t_monitor_index \
              WHERE id IN(SELECT index_id FROM `t_monitor_templete_index` \
                           WHERE INSTR('{0}',templete_id)>0) AND STATUS='1'".format(p_templete)
    rs = await async_processer.query_dict_list(st)
    t=''
    for i in rs:
       t=t+i['index_code']+','
    return t[0:-1]


async def get_itmes_from_monitor_templete(p_templete):
    st = "SELECT index_code,index_threshold FROM t_monitor_index \
              WHERE id IN(SELECT index_id FROM `t_monitor_templete_index` \
                           WHERE INSTR('{0}',templete_id)>0) AND STATUS='1'".format(p_templete)
    rs = await async_processer.query_dict_list(st)
    return rs

async def get_db_alert_config(p_tag):
    if await check_server_alert_status(p_tag)>0:
       return {'code': -1, 'msg': '告警服务器已禁用!'}

    if await check_alert_config(p_tag)==0:
       return {'code': -1, 'msg': '告警标识不存在!'}

    st = '''SELECT a.task_tag,a.comments,a.templete_id,
                   a.server_id,a.run_time,
                   a.python3_home,a.api_server,a.script_path,
                   a.script_file,a.status,b.server_ip,
                   b.server_port,b.server_user,b.server_pass,
                   b.server_desc, b.market_id,
                   (select `value` from t_sys_settings where `key`='send_server') as send_server,
                   (select `value` from t_sys_settings where `key`='send_port') as send_port,
                   (select `value` from t_sys_settings where `key`='sender') as sender,
                   (select `value` from t_sys_settings where `key`='sendpass') as sendpass,
                   (select `value` from t_sys_settings where `key`='API_REQUEST_TIMEOUT') as API_REQUEST_TIMEOUT,
                   (select `value` from t_sys_settings where `key`='API_REQUEST_TIMEOUT_SLEEP') as API_REQUEST_TIMEOUT_SLEEP,
                   (select `value` from t_sys_settings where `key`='API_REQUEST_GAP_SLEEP') as API_REQUEST_GAP_SLEEP,
                   (select `value` from t_sys_settings where `key`='API_REQUEST_RECOVER_SLEEP') as API_REQUEST_RECOVER_SLEEP,
                   (select `value` from t_sys_settings where `key`='API_INTERFACE_MAIL') as API_INTERFACE_MAIL
        FROM t_alert_task a JOIN t_server b ON a.server_id=b.id 
        where a.task_tag ='{0}' ORDER BY a.id'''.format(p_tag)
    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    rs['templete_indexes'] = await get_itmes_from_templete_ids(rs['templete_id'])
    rs['templete_monitor_indexes'] = await get_itmes_from_monitor_templete(rs['templete_id'])
    return {'code': 200, 'msg': rs}

async def write_remote_crontab_alert(cfg,ssh):
    v_cmd   = '{0}/db_monitor.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'], cfg['msg']['task_tag'])

    v_cron  = '''
               crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(cfg['msg']['task_tag'],cfg['msg']['comments'],cfg['msg']['task_tag'],cfg['msg']['run_time'],v_cmd)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(cfg['msg']['task_tag'], cfg['msg']['comments'], cfg['msg']['task_tag'], cfg['msg']['run_time'], v_cmd)

    v_cron2 ='''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''

    v_cron3 ='''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}'''.\
             format(cfg['msg']['script_path'],cfg['msg']['script_path'],get_time2())

    v_cron4 ='''crontab /tmp/config'''

    if cfg['msg']['status']=='1':
       if not ssh.exec(v_cron)['status']:
          return {'code': -1, 'msg': 'failure!'}
    else:
       if not ssh.exec(v_cron_)['status']:
          return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron2)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron3)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron4)['status']:
       return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec('crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def transfer_remote_file_alert(cfg,ssh,ftp):
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'monitor', cfg['msg']['script_file'])
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'monitor', 'db_monitor.sh')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_alert(cfg,ssh):
    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_monitor.sh')
    res = ssh.exec(cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def push(tag):
    cfg = await get_db_alert_config(tag)
    print('cfg=',cfg)
    if cfg['code']!=200:
       return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)

    res = await transfer_remote_file_alert(cfg,ssh,ftp)
    if res['code'] != 200:
        raise Exception('transfer_remote_file error!')

    res = await run_remote_cmd_alert(cfg,ssh)
    if res['code'] != 200:
        raise Exception('run_remote_cmd error!')

    res = await write_remote_crontab_alert(cfg,ssh)
    if res['code'] != 200:
        raise Exception('write_remote_crontab error!')

    ssh.close()
    ftp.close()
    return res
