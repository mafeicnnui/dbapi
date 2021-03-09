#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:05
# @Author : ma.fei
# @File : monitor.py.py
# @Software: PyCharm

import traceback
from utils.common import aes_decrypt,exec_ssh_cmd,gen_transfer_file,ftp_transfer_file,get_time2
from utils.mysql_async import async_processer

async def check_db_monitor_config(p_tag):
    st = "select count(0) from t_monitor_task where task_tag='{0}'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def check_server_monitor_status(p_tag):
    st = "select count(0) from t_monitor_task a,t_server b \
                  where a.server_id=b.id and a.task_tag='{0}' and b.status='0'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def get_itmes_from_templete_ids(p_templete):
    st = "SELECT index_code FROM t_monitor_index \
              WHERE id IN(SELECT index_id FROM `t_monitor_templete_index` \
                           WHERE INSTR('{0}',templete_id)>0) AND STATUS='1'".format(p_templete)
    rs = await async_processer.query_dict_list(st)
    t=''
    for i in rs:
       t=t+i['index_code']+','
    return t[0:-1]

async def get_db_monitor_config(p_tag):
    if await check_server_monitor_status(p_tag)>0:
       return {'code': -1, 'msg': '采集服务器已禁用!'}

    if check_db_monitor_config(p_tag)==0:
       return {'code': -1, 'msg': '监控标识不存在!'}

    st = '''SELECT a.task_tag,a.comments,a.templete_id,
                   a.server_id,a.db_id,a.run_time,
                   a.python3_home,a.api_server,a.script_path,
                   a.script_file,a.status,b.server_ip,
                   b.server_port,b.server_user,b.server_pass,
                   b.server_desc, b.market_id,
                   c.ip as db_ip, c.port  as db_port,
                   c.service as db_service,c.user as db_user,
                   c.password as db_pass,c.db_type as db_type 
        FROM t_monitor_task a JOIN t_server b ON a.server_id=b.id LEFT JOIN t_db_source c  ON  a.db_id=c.id  
        where a.task_tag ='{0}' ORDER BY a.id'''.format(p_tag)

    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    rs['templete_indexes'] = await get_itmes_from_templete_ids(rs['templete_id'])
    return {'code': 200, 'msg': rs}

async def save_monitor_log(config):
    if config['db_id']!='':
        st = '''insert into t_monitor_task_db_log 
                  (task_tag,server_id,db_id,total_connect,active_connect,db_available,db_tbs_usage,db_qps,db_tps,create_date) 
                 values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}',now())
                '''.format(config.get('task_tag',''), config.get('server_id',''),config.get('db_id',''),
                           config.get('total_connect',''),config.get('active_connect',''),config.get('db_available',''),
                           config.get('db_tbs_usage',''),config.get('db_qps',''),config.get('db_tps',''))
    else:
        st = '''insert into t_monitor_task_server_log
                   (task_tag,server_id,cpu_total_usage,cpu_core_usage,mem_usage,disk_usage,disk_read,disk_write,net_in,net_out,market_id,create_date) 
                  values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',now())
                '''.format(config.get('task_tag',''), config.get('server_id',''),
                           config.get('cpu_total_usage',''), config.get('cpu_core_usage',''), config.get('mem_usage',''),
                           config.get('disk_usage',''), config.get('disk_read',''), config.get('disk_write',''),
                           config.get('net_in',''), config.get('net_out',''), config.get('market_id',''))

    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def write_remote_crontab_monitor(v_tag):
    cfg = await get_db_monitor_config(v_tag)
    if cfg['code']!=200:
       return cfg

    v_cmd   = '{0}/db_monitor.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'], v_tag)

    v_cron  = '''
               crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag,cfg['msg']['comments'],v_tag,cfg['msg']['run_time'],v_cmd)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag, cfg['msg']['comments'], v_tag, cfg['msg']['run_time'], v_cmd)

    v_cron2 ='''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''

    v_cron3 ='''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}'''.\
             format(cfg['msg']['script_path'],cfg['msg']['script_path'],get_time2())

    v_cron4 ='''crontab /tmp/config'''

    if cfg['msg']['status']=='1':
       exec_ssh_cmd(cfg, v_cron)
    else:
       exec_ssh_cmd(cfg, v_cron_)

    if not exec_ssh_cmd(cfg, v_cron2)['status']:
       return {'code': -1, 'msg': 'failure!'}
    if not exec_ssh_cmd(cfg, v_cron3)['status']:
       return {'code': -1, 'msg': 'failure!'}
    if not exec_ssh_cmd(cfg, v_cron4)['status']:
       return {'code': -1, 'msg': 'failure!'}

    res = exec_ssh_cmd(cfg, 'crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def transfer_remote_file_monitor(v_tag):
    cfg = await get_db_monitor_config(v_tag)
    if cfg['code']!=200:
       return cfg

    exec_ssh_cmd('mkdir -p {0}'.format(cfg['msg']['script_path']))
    f_local, f_remote = gen_transfer_file(cfg, 'monitor', cfg['msg']['script_file'])
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'monitor', 'db_monitor.sh')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_monitor(v_tag):
    cfg = await get_db_monitor_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_monitor.sh')
    res = exec_ssh_cmd(cfg, cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg, cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}
