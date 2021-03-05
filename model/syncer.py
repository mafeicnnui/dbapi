#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:33
# @Author : ma.fei
# @File : syncer.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists,get_time2,exec_ssh_cmd,gen_transfer_file,ftp_transfer_file
from utils.mysql_async import async_processer

async def check_server_sync_status(p_tag):
    st = "select count(0) from t_db_sync_config a,t_server b \
             where a.server_id=b.id and a.sync_tag='{0}' and b.status='0'".format(p_tag)
    rs =  await async_processer.query_one(st)
    return rs[0]

async def check_db_sync_config(p_tag):
    st = "select count(0) from t_db_sync_config where sync_tag='{0}'".format(p_tag)
    rs = await async_processer.query_one(st)
    return rs[0]

async def check_sync_task_status(p_tag):
    st = "select count(0) from t_db_sync_config a,t_server b \
           where a.server_id=b.id and a.sync_tag='{0}' and a.status='0'".format(p_tag)
    rs = await async_processer.query_one(st)
    return rs[0]

async def get_db_sync_config(p_tag):
    result = {}
    result['code'] = 200
    result['msg'] = ''

    # 检测同步服务器是否有效
    if await check_server_sync_status(p_tag)>0:
       result['code'] = -1
       result['msg'] = '同步服务器已禁用!'
       return result

    # 检测同步标识是否存在
    if await check_db_sync_config(p_tag)==0:
       result['code'] = -2
       result['msg'] = '同步标识不存在!'
       return result

    # 任务已禁用
    if await check_sync_task_status(p_tag) > 0:
       result['code'] = -3
       result['msg'] = '同步任务已禁用!'
       return result

    st = '''SELECT a.sync_tag,
                   a.sync_ywlx,
                   (select dmmc from t_dmmx where dm='08' and dmm=a.sync_ywlx) as sync_ywlx_name,
                   a.sync_type,
                   (select dmmc from t_dmmx where dm='09' and dmm=a.sync_type) as sync_type_name,
                   CASE WHEN c.service='' THEN 
                    CONCAT(c.ip,':',c.port,':',a.sync_schema,':',c.user,':',c.password)
                   ELSE
                    CONCAT(c.ip,':',c.port,':',c.service,':',c.user,':',c.password)
                   END AS sync_db_sour,                          
                   CASE WHEN d.service='' THEN 
                    CONCAT(d.ip,':',d.port,':',IFNULL(a.sync_schema_dest,a.sync_schema),':',d.user,':',d.password)
                   ELSE
                    CONCAT(d.ip,':',d.port,':',d.service,':',d.user,':',d.password)
                   END AS sync_db_dest,                          
                   a.server_id,
                   b.server_desc,
                   a.run_time,
                   a.api_server,
                   LOWER(a.sync_table) AS sync_table,a.batch_size,a.batch_size_incr,a.sync_gap,a.sync_col_name,a.sync_repair_day,
                   a.sync_col_val,a.sync_time_type,a.script_path,a.script_file,a.comments,a.python3_home,
                   a.status,b.server_ip,b.server_port,b.server_user,b.server_pass,
                   c.proxy_server,
                   (select dmmc from t_dmmx where dm='36' and dmm='01') as proxy_local_port
                FROM t_db_sync_config a,t_server b,t_db_source c,t_db_source d
                WHERE a.server_id=b.id 
                  AND a.sour_db_id=c.id
                  AND a.desc_db_id=d.id
                  AND a.sync_tag ='{0}' 
                  ORDER BY a.id,a.sync_ywlx'''.format(p_tag)

    rs = await async_processer.query_dict_one(st)

    st = '''SELECT sync_tag,db_name,schema_name,tab_name,sync_cols,sync_incr_col
             FROM `t_db_sync_tab_config` WHERE sync_tag='{}' and status='1' ORDER BY id'''.format(p_tag)

    rs['cols'] = await async_processer.query_dict_list(st)
    result['msg'] = rs
    return result

async def save_sync_log(config):
    result = {}
    result['code'] = 200
    result['msg'] = 'success'
    st = '''insert into t_db_sync_tasks_log(sync_tag,create_date,duration,amount) 
             values('{0}','{1}','{2}','{3}')
           '''.format(config['sync_tag'],config['create_date'],config['duration'],config['amount'])
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def save_sync_log_detail(config):
    st = '''insert into t_db_sync_tasks_log_detail(sync_tag,create_date,sync_table,sync_amount,duration) 
              values('{0}','{1}','{2}','{3}','{4}')
           '''.format(config['sync_tag'],config['create_date'],
                      config['sync_table'],config['sync_amount'],config['duration'])
    try:
        await async_processer.exec_sql(st)
        vv = " where sync_tag='{0}' and tab_name='{1}'".format(config['sync_tag'], config['tab_name'])
        if await check_tab_exists('t_db_sync_tab_config', vv) == 0:
               st = '''insert into t_db_sync_tab_config
                         (sync_tag,db_name,schema_name,tab_name,sync_cols,sync_incr_col,sync_time,status,create_date)
                          values('{}','{}','{}','{}','{}','{}','{}','1',now())
                    '''.format(config['sync_tag'],config['db_name'],config['schema_name'],
                               config['tab_name'],config['sync_cols'],config['sync_incr_col'],config['sync_time'])
               await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def run_remote_sync_task(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd1 = 'nohup {0}/db_sync.sh {1} {2} &>/dev/null &'.format(cfg['msg']['script_path'], cfg['msg']['script_file'], v_tag)
    cmd2 = '{0}/db_agent.sh'.format(cfg['msg']['script_path'])
    res  = exec_ssh_cmd(cfg,cmd1)
    if res['status']:
        res = exec_ssh_cmd(cfg, cmd2)
        if res['status']:
           return {'code': 200, 'msg': res['stdout']}
        else:
           return {'code': -1, 'msg': 'failure!'}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def stop_remote_sync_task(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg
    cmd1 = "ps -ef | grep {0} |grep -v grep | wc -l".format(v_tag)
    cmd2 = "ps -ef | grep $$SYNC_TAG$$ |grep -v grep | awk '{print $2}'  | xargs kill -9".replace('$$SYNC_TAG$$',v_tag)

    res = exec_ssh_cmd(cfg, cmd1)
    if  res['status']:
        res = exec_ssh_cmd(cfg, cmd2)
        if res['status']:
           return {'code': 200, 'msg': res['stdout']}
        else:
           return {'code': -1, 'msg': 'failure!'}
    else:
        return {'code': -1, 'msg': 'failure!'}
    return res

async def run_remote_cmd_sync(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'mkdir -p {0}'.format(cfg['msg']['script_path'] + '/config')
    cmd2 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd3 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_sync.sh')
    cmd4 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_agent.sh')

    res = exec_ssh_cmd(cfg, cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg, cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg, cmd3)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg, cmd4)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def transfer_remote_file_sync(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    res = exec_ssh_cmd(cfg, 'mkdir -p {0}'.format(cfg['msg']['script_path']))
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'syncer', cfg['msg']['script_file'])
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'db_sync.sh')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'db_agent.sh')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'db_agent.py')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def write_remote_crontab_sync(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    v_cmd  = '{0}/db_sync.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'], v_tag)

    v_cmd_agent = '{0}/db_agent.sh '.format(cfg['msg']['script_path'])

    v_cron = '''
               crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
             '''.format(v_tag,cfg['msg']['comments'],v_tag,cfg['msg']['run_time'],v_cmd)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag, cfg['msg']['comments'], v_tag, cfg['msg']['run_time'], v_cmd)

    v_cron_agent = '''
                sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
              '''.format('db_agent', '数据库代理服务', 'db_agent.py', '*/1 * * * *', v_cmd_agent)

    v_cron_agent_ = '''
                sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
                  '''.format('db_agent', '数据库代理服务', 'db_agent.py', '*/1 * * * *', v_cmd_agent)


    v_cron2 = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''

    v_cron3 = '''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}'''. \
                 format(cfg['msg']['script_path'], cfg['msg']['script_path'], get_time2())

    v_cron4 ='''crontab /tmp/config'''

    if cfg['msg']['status']=='1':
       exec_ssh_cmd(cfg,v_cron)
       exec_ssh_cmd(cfg,v_cron_agent)
    else:
       exec_ssh_cmd(cfg, v_cron_)
       exec_ssh_cmd(cfg, v_cron_agent_)

    exec_ssh_cmd(cfg, v_cron2)
    exec_ssh_cmd(cfg, v_cron3)
    exec_ssh_cmd(cfg, v_cron4)

    res = exec_ssh_cmd(cfg, 'crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}