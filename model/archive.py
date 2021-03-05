#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:05
# @Author : ma.fei
# @File : archive.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists,aes_decrypt,exec_ssh_cmd,gen_transfer_file,ftp_transfer_file
from utils.mysql_async import async_processer

async def check_server_archive_status(p_tag):
    st = "select count(0) from t_db_archive_config a,t_server b \
              where a.server_id=b.id and a.archive_tag='{0}' and b.status='0'".format(p_tag)
    return  (await async_processer.query_one(st))[0]

async def check_db_archive_config(p_tag):
    st = "select count(0) from t_db_archive_config where archive_tag='{0}'".format(p_tag)
    return  (await async_processer.query_one(st))[0]

async def get_db_archive_config(p_tag):
    st = '''SELECT a.archive_tag,
                   CONCAT(c.ip,':',c.port,':',a.sour_schema,':',c.user,':',c.password) AS archive_db_sour,
                   CONCAT(d.ip,':',d.port,':',a.dest_schema,':',d.user,':',d.password) AS archive_db_dest,  
                   a.server_id,b.server_desc,a.api_server,LOWER(a.sour_table) AS sour_table,
                   a.archive_time_col,a.archive_rentition,a.rentition_time,
                   a.rentition_time_type,e.dmmc AS rentition_time_type_cn,
                   a.if_cover,a.script_path,a.script_file,a.run_time,
                   a.batch_size,a.comments,a.python3_home,a.status,
                   b.server_ip,b.server_port,b.server_user,b.server_pass,a.dest_db_id 
            FROM t_db_archive_config a JOIN t_server b ON a.server_id=b.id 
             JOIN t_db_source c ON a.sour_db_id=c.id
             LEFT JOIN t_db_source d ON a.dest_db_id=d.id
             JOIN t_dmmx e ON a.rentition_time_type=e.dmm AND  e.dm='20'
             WHERE a.archive_tag ='{}' ORDER BY a.id'''.format(p_tag)
    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    return {'code': 200, 'msg': rs}

async def run_remote_archive_task(p_tag):
    cfg = await get_db_archive_config(p_tag)
    if cfg['code']!=200:
       return cfg

    cmd = 'nohup {0}/db_archive.sh {1} {2} &>/dev/null &'.format(cfg['script_path'], cfg['script_file'], p_tag)
    if await check_server_archive_status(p_tag) > 0:
       return {'code':-1,'msg':'归档服务器已禁用!'}

    if await check_db_archive_config(p_tag) == 0:
       return {'code':-1,'msg': '归档标识不存在!'}

    res = exec_ssh_cmd(cfg, cmd)
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def stop_remote_archive_task(v_tag):
    cfg = await get_db_archive_config(v_tag)
    if cfg['code']!=200:
       return cfg
    cmd1 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}'  | wc -l".replace('$$TAG$$',v_tag)
    cmd2 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}'  | xargs kill -9".replace('$$TAG$$',v_tag)

    res = exec_ssh_cmd(cfg, cmd1)
    if res['status']:
        if int(res['stdout']) == 0:
            return {'code': -2, 'msg': 'task not running!'}
        else:
            res = exec_ssh_cmd(cfg, cmd2)
            if res['status']:
                return {'code': 200, 'msg': 'success'}
            else:
                return {'code': -1, 'msg': 'failure!'}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def transfer_remote_file_archive(v_tag):
    cfg = await get_db_archive_config(v_tag)
    if cfg['code']!=200:
       return cfg
    exec_ssh_cmd('mkdir -p {0}'.format(cfg['msg']['script_path']))
    f_local, f_remote = gen_transfer_file(cfg, 'archive', cfg['msg']['script_file'])
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'archive', 'db_archive.sh')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_archive(v_tag):
    cfg = await get_db_archive_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_archive.bat')
    res = exec_ssh_cmd(cfg, cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg, cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def write_remote_crontab_archive(v_tag):
    cfg = await get_db_archive_config(v_tag)
    if cfg['code']!=200:
       return cfg

    v_cmd   = '{0}/db_archive.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],v_tag)
    v_cron0 = '''echo -e "#{0}" >/tmp/config'''.format(v_tag)
    v_cron1 = '''
                 crontab -l >> /tmp/config && sed -i "/{0}/d" /tmp/config && echo -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config && crontab /tmp/config       
              '''.format(v_tag,cfg['msg']['comments'],v_tag,cfg['msg']['run_time'],v_cmd)

    v_cron1_= '''
                 crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag, cfg['msg']['comments'], v_tag, cfg['msg']['run_time'], v_cmd)

    v_cron2 = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''
    v_cron3 = '''crontab /tmp/config'''

    exec_ssh_cmd(cfg, v_cron0)
    if cfg['msg']['status'] == '1':
        exec_ssh_cmd(cfg, v_cron1)
    else:
        exec_ssh_cmd(cfg, v_cron1_)
    exec_ssh_cmd(cfg, v_cron2)
    exec_ssh_cmd(cfg, v_cron3)
    res = exec_ssh_cmd(cfg, 'crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def save_archive_log(config):
    vv = " where archive_tag='{0}' and create_date ='{1}'".format(config['archive_tag'], config['create_date'])
    if await check_tab_exists('t_db_archive_log', vv) == 0:
       st='''insert into t_db_archive_log(archive_tag,table_name,create_date,start_time,end_time,duration,amount,percent,message) 
                values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')
              '''.format(config['archive_tag'],config['table_name'],config['create_date'],
                         config['start_time'],config['end_time'],config['duration'],
                         config['amount'],config['percent'],config['message'])
    else:
        st = '''update t_db_archive_log
                            set table_name   = '{0}',
                                duration     = '{1}',
                                amount       = '{2}',
                                percent      = '{3}',
                                start_time   = '{4}',
                                end_time     = '{5}',
                                message      = '{6}'
                          where archive_tag  = '{7}' and create_date='{8}'
                      '''.format(config['table_name'],config['duration'],config['amount'],
                                 config['percent'],config['start_time'],config['end_time'],
                                 config['message'],config['archive_tag'],config['create_date'])
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}