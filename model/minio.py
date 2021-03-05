#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:03
# @Author : ma.fei
# @File : minio.py.py
# @Software: PyCharm

import  os
import  paramiko
import  traceback
from utils.common import db_config,aes_decrypt,get_time2,get_file_contents,exec_ssh_cmd,gen_transfer_file,ftp_transfer_file
from utils.mysql_async import async_processer

async def get_minio_config(p_tag):

    if await check_server_minio_status(p_tag)>0:
       return {'code': -1, 'msg': '采集服务器已禁用!'}

    if await check_minio_config(p_tag)==0:
       return {'code': -1, 'msg': '同步标识不存在!'}

    st ='''SELECT  
               a.sync_tag,a.sync_type,a.server_id,
               a.sync_path,a.sync_service,a.minio_server,
               a.minio_user,a.minio_pass,a.python3_home,
               a.script_path,a.script_file,a.api_server,
               a.run_time,a.comments, a.status,
               a.minio_bucket,a.minio_dpath,a.minio_incr,a.minio_incr_type,
               b.server_ip,b.server_port,b.server_user,b.server_pass,b.server_desc
    FROM t_minio_config a ,t_server b
     where a.server_id=b.id and a.sync_tag='{0}'  ORDER BY a.id'''.format(p_tag)
    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    return {'code': 200, 'msg': rs}

async def check_minio_config(p_tag):
    st = "select count(0) from t_minio_config where sync_tag='{0}'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def check_server_minio_status(p_tag):
    st = "select count(0) from t_minio_config a, t_server b \
           where a.server_id=b.id  and a.sync_tag='{0}' and a.status='0'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def save_minio_log(config):
    st = '''insert into t_minio_log(sync_tag,server_id,download_time,upload_time,total_time,transfer_file,sync_day,create_date)
             values('{}','{}','{}','{}','{}','{}','{}',now())
         '''.format(config.get('sync_tag'),config.get('server_id'),config.get('download_time'),
                    config.get('upload_time'),config.get('total_time'),config.get('transfer_file'),config.get('sync_day'))
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def write_remote_crontab_minio(v_tag):
    cfg = await get_minio_config(v_tag)
    if cfg['code']!=200:
       return cfg

    v_cmd   = '{0}/minio_sync.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'], v_tag)

    v_cron0 = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} sync_tag={2}\n{3} {4} &>/dev/null & #sync_tag={5}" >> /tmp/config  && crontab /tmp/config
              '''.format("sync_tag="+v_tag,cfg['msg']['comments'],v_tag,cfg['msg']['run_time'],v_cmd,v_tag)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config >> /tmp/config && crontab /tmp/config
              '''.format(v_tag)

    v_cron1 = '''crontab -l > /tmp/config && sed -i '/^$/{N;/\\n$/D};' /tmp/config && crontab /tmp/config'''

    v_cron2 = '''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}
              '''.format(cfg['msg']['script_path'],cfg['msg']['script_path'],get_time2())

    if not exec_ssh_cmd(cfg, v_cron2)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if cfg['msg']['status'] == '0':
        if not exec_ssh_cmd(cfg, v_cron_)['status']:
           return {'code': -1, 'msg': 'failure!'}

    if cfg['msg']['status'] == '1':
        if not exec_ssh_cmd(cfg, v_cron0)['status']:
           return {'code': -1, 'msg': 'failure!'}

        if exec_ssh_cmd(cfg, v_cron1)['status']:
           return {'code': -1, 'msg': 'failure!'}

    res = exec_ssh_cmd(cfg, 'crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def transfer_remote_file_minio(v_tag):
    cfg = await get_minio_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    if not exec_ssh_cmd(cfg, cmd)['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'minio', cfg['msg']['script_file'])
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'minio', 'minio_sync.sh')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_minio(v_tag):
    cfg = await get_minio_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_backup.sh')

    if not exec_ssh_cmd(cfg, cmd1)['status']:
        return {'code': -1, 'msg': 'failure!'}

    if not exec_ssh_cmd(cfg, cmd2)['status']:
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}