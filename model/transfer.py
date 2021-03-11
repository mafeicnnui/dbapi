#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:04
# @Author : ma.fei
# @File : transfer.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists,gen_transfer_file
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper

async def check_db_transfer_config(p_tag):
    st = "select count(0) from t_db_transfer_config where transfer_tag='{0}'".format(p_tag)
    rs = await async_processer.query_one(st)
    return rs[0]

async def check_server_transfer_status(p_tag):
    st = "select count(0) from t_db_transfer_config a,t_server b \
            where a.server_id=b.id and a.transfer_tag='{0}' and b.status='0'".format(p_tag)
    rs = await async_processer.query_one(st)
    return rs[0]

async def save_transfer_log(config):
    vv = " where transfer_tag='{0}' and create_date ='{1}'".format(config['transfer_tag'], config['create_date'])
    if await check_tab_exists('t_db_transfer_log', vv) == 0:
        st ='''insert into t_db_transfer_log(transfer_tag,table_name,create_date,duration,amount,percent) 
                  values('{0}','{1}','{2}','{3}','{4}','{5}')
            '''.format(config['transfer_tag'],config['table_name'],
                       config['create_date'],config['duration'],
                       config['amount'],config['percent'])
    else:
        st  = '''update t_db_transfer_log
                    set table_name   = '{0}',
                        duration     = '{1}',
                        amount       = '{2}',
                        percent      = '{3}'
                  where transfer_tag = '{4}' and create_date='{5}'
              '''.format(config['table_name'],config['duration'],
                         config['amount'], config['percent'],
                         config['transfer_tag'],config['create_date'])

    try:
       await async_processer.exec_sql(st)
       return {'code':200,'msg':'success'}
    except:
       traceback.print_exc()
       return {'code': -1, 'msg': 'failure'}

async def get_db_transfer_config(p_tag):
    if await check_server_transfer_status(p_tag)>0:
       return {'code': -1, 'msg': '传输服务器已禁用!'}

    if await check_db_transfer_config(p_tag)==0:
       return {'code': -1, 'msg': '传输标识不存在!'}

    st = '''SELECT  a.transfer_tag,
                          CONCAT(c.ip,':',c.port,':',a.sour_schema,':',c.user,':',c.password) AS transfer_db_sour,
                          CONCAT(d.ip,':',d.port,':',a.dest_schema,':',d.user,':',d.password) AS transfer_db_dest,  
                          a.server_id, b.server_desc,a.api_server,
                          LOWER(a.sour_table) AS sour_table,
                          a.sour_where,a.script_path,a.script_file,
                          a.batch_size,a.comments,a.python3_home,
                          a.status,b.server_ip,b.server_port,b.server_user,b.server_pass
            FROM t_db_transfer_config a,t_server b,t_db_source c,t_db_source d
            WHERE a.server_id=b.id and a.sour_db_id=c.id
              and a.dest_db_id=d.id and a.transfer_tag ='{0}'
            order by a.id'''.format(p_tag)
    rs = await async_processer.query_dict_one(st)
    return {'code': 200, 'msg': rs}

async def run_remote_transfer_task(v_tag):
    cfg = await get_db_transfer_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd  = 'nohup {0}/db_transfer.sh {1} {2} &>/dev/null &'.format(cfg['msg']['script_path'], cfg['msg']['script_file'], v_tag)
    if cfg['code']!=200:
       return cfg

    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd)
    if res['status']:
        res = {'code': 200, 'msg': res['stdout']}
    else:
        res = {'code': -1, 'msg': 'failure!'}

    ssh.close()
    return res

async def stop_remote_transfer_task(v_tag):
    cfg = await get_db_transfer_config(v_tag)
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

async def transfer_remote_file_transfer(cfg,ssh,ftp):
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'transfer', cfg['msg']['script_file'])
    if not ftp.transfer(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'transfer', 'db_transfer.sh')
    if not ftp.transfer(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_transfer(cfg,ssh):
    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_transfer.sh')
    res  = ssh.exec(cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def push(tag):
    cfg = await get_db_transfer_config(tag)
    if cfg['code'] != 200:
        return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)

    res = await transfer_remote_file_transfer(cfg,ssh,ftp)
    if res['code'] != 200:
        raise Exception('transfer_remote_file error!')

    res = await run_remote_cmd_transfer(cfg,ssh)
    if res['code'] != 200:
        raise Exception('run_remote_cmd error!')

    ssh.close()
    ftp.close()
    return res