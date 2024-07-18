#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:05
# @Author : ma.fei
# @File : archive.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists,aes_decrypt,gen_transfer_file
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper


async def check_bbgl_task_status(p_tag):
    st ="select count(0) from t_bbgl_task a,t_server b  \
            where a.server_id=b.id and a.task_tag='{0}' and a.status='0'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def check_server_bbgl_status(p_tag):
    st = "select count(0) from t_bbgl_task a,t_server b \
              where a.server_id=b.id and a.task_tag='{0}' and b.status='0'".format(p_tag)
    return  (await async_processer.query_one(st))[0]

async def check_bbgl_config(p_tag):
    st = "select count(0) from t_bbgl_task where task_tag='{0}'".format(p_tag)
    return  (await async_processer.query_one(st))[0]

async def get_db_bbgl_config(p_tag):

    if await check_bbgl_task_status(p_tag) > 0:
       return {'code': -1, 'msg': '报表任务已禁用!'}

    if await check_server_bbgl_status(p_tag) > 0:
        return {'code': -1, 'msg': '报表服务器已禁用!'}

    if await check_bbgl_config(p_tag) == 0:
        return {'code': -1, 'msg': '报表任务标识不存在!'}

    st = '''SELECT 
               a.task_tag,
               f.`bbdm`,
               f.bbmc,
               c.ip AS db_ip, 
               c.port  AS db_port,
               f.db AS db_service,
               c.user AS db_user,
               c.password AS db_pass,
               c.db_type AS db_type,
               c.db_desc,
               CONCAT(c.ip,':',c.port,':',f.db,':',c.user,':',c.password) AS bbgl_db,
               a.server_id,b.server_desc,a.api_server,
               f.statement AS statement,
               a.`tjrq_begin`,
               a.`tjrq_end`,
               a.script_path,a.script_file,a.run_time,
               a.comments,a.python3_home,a.status,
               b.server_ip,b.server_port,b.server_user,b.server_pass,
               (select `value` from t_sys_settings where `key`='send_server') as send_server,
               (select `value` from t_sys_settings where `key`='send_port') as send_port,
               (select `value` from t_sys_settings where `key`='sender') as sender,
               (select `value` from t_sys_settings where `key`='sendpass') as sendpass,
               a.receiver  as receiver,
               a.cc as cc
            FROM t_bbgl_task a 
            JOIN t_bbgl_config f ON a.bbid=f.id
            JOIN t_server b ON a.server_id=b.id 
            JOIN t_db_source c ON f.dsid=c.id
            WHERE a.task_tag ='{}' ORDER BY a.id'''.format(p_tag)
    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    rs['preproccess'] = await async_processer.query_dict_list(
                           "SELECT * FROM `t_bbgl_preproccess` WHERE bbdm='{}'".format(rs['bbdm']))
    rs['headers'] = await async_processer.query_dict_list(
                            "select xh,header_name from t_bbgl_header where bbdm='{}' ORDER BY xh".format(rs['bbdm']))
    return {'code': 200, 'msg': rs}

async def run_remote_bbgl_task(p_tag):
    cfg = await get_db_bbgl_config(p_tag)
    if cfg['code']!=200:
       return cfg

    cmd = 'nohup {0}/bbgl_task.sh {1} {2} &>/dev/null &'.format(cfg['msg']['script_path'], cfg['msg']['script_file'], p_tag)
    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd)
    if res['status']:
        res = {'code': 200, 'msg': res['stdout']}
    else:
        res = {'code': -1, 'msg': 'failure!'}
    ssh.close()
    return res

async def stop_remote_bbgl_task(v_tag):
    cfg = await get_db_bbgl_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd1 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}'  | wc -l".replace('$$TAG$$',v_tag)
    cmd2 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}'  | xargs kill -9".replace('$$TAG$$',v_tag)

    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd1)
    if res['status']:
        if int(res['stdout']) == 0:
            res = {'code': -2, 'msg': 'task not running!'}
        else:
            res = ssh.exec(cmd2)
            if res['status']:
                res = {'code': 200, 'msg': 'success'}
            else:
                res = {'code': -1, 'msg': 'failure!'}
    else:
        res = {'code': -1, 'msg': 'failure!'}

    ssh.close()
    return res

async def transfer_remote_file_bbgl(cfg,ssh,ftp):
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'bbgl', cfg['msg']['script_file'])
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'bbgl', 'bbgl_task.sh')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_bbgl(cfg,ssh):
    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'bbgl_task.sh')
    res = ssh.exec(cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd2)
    if not res['status']:
        return {'code': -1, 'msg': res['stderr']}

    return {'code': 200, 'msg': 'success!'}

async def write_remote_crontab_bbgl(cfg,ssh):
    v_cmd   = '{0}/bbgl_task.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],cfg['msg']['task_tag'])
    v_cron0 = '''echo -e "#{0}" >/tmp/config'''.format(cfg['msg']['task_tag'])
    v_cron1 = '''
                 crontab -l >> /tmp/config && sed -i "/{0}/d" /tmp/config && echo -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config && crontab /tmp/config       
              '''.format(cfg['msg']['task_tag'],cfg['msg']['comments'],cfg['msg']['task_tag'],cfg['msg']['run_time'],v_cmd)

    v_cron1_= '''
                 crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(cfg['msg']['task_tag'], cfg['msg']['comments'], cfg['msg']['task_tag'], cfg['msg']['run_time'], v_cmd)

    v_cron2 = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''
    v_cron3 = '''crontab /tmp/config'''

    if not ssh.exec(v_cron0)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if cfg['msg']['status'] == '1':
        if not ssh.exec(v_cron1)['status']:
           return {'code': -1, 'msg': 'failure!'}

    else:
        if ssh.exec(v_cron1_)['status']:
           return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron2)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron3)['status']:
       return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec('crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def save_bbgl_log(config):
    vv = " where archive_tag='{0}' and create_date ='{1}'".format(config['task_tag'], config['create_date'])
    if await check_tab_exists('t_bbgl_task_log', vv) == 0:
       st='''insert into t_bbgl_task_log(task_tag,bbdm,bbmc,create_date,start_time,end_time,duration,status,message) 
             values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')
          '''.format(config['task_tag'],config['bbdm'],config['bbmc'],config['create_date'],
                     config['start_time'],config['end_time'],config['duration'],
                     config['status'],config['message'])
    else:
        st = '''update t_bbgl_task_log
                            set duration     = '{0}',
                                start_time   = '{1}',
                                end_time     = '{2}',
                                status       = '{3}',
                                message      = '{4}'
                          where task_tag  = '{5}' and create_date='{6}'
                      '''.format(config['duration'],
                                 config['start_time'],
                                 config['end_time'],
                                 config['status'],config['message'],
                                 config['task_tag'],config['create_date'])
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def push(tag):
    cfg = await get_db_bbgl_config(tag)
    if cfg['code']!=200:
       return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)

    res = await transfer_remote_file_bbgl(cfg,ssh,ftp)
    if res['code'] != 200:
        raise Exception('transfer_remote_file error!')

    res = await run_remote_cmd_bbgl(cfg,ssh)
    if res['code'] != 200:
        print(res['msg'])
        raise Exception('run_remote_cmd error!')

    res = await write_remote_crontab_bbgl(cfg,ssh)
    if res['code'] != 200:
        raise Exception('write_remote_crontab error!')

    ssh.close()
    ftp.close()
    return res