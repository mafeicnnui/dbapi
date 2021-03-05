#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:18
# @Author : ma.fei
# @File : backup.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists, aes_decrypt,exec_ssh_cmd,gen_transfer_file,ftp_transfer_file
from utils.mysql_async import async_processer

async def check_db_config(p_tag):
    st = "select count(0) from t_db_config where db_tag='{0}'".format(p_tag)
    rs =  await async_processer.query_one(st)
    return rs[0]

async def check_server_backup_status(p_tag):
    st = "select count(0) from t_db_config a,t_server b where a.server_id=b.id and a.db_tag='{0}' and b.status='0'".format(p_tag)
    rs =  await async_processer.query_one(st)
    return rs[0]

async def check_backup_task_status(p_tag):
    st ="select count(0) from t_db_config a,t_server b  \
            where a.server_id=b.id and a.db_tag='{0}' and a.status='0'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def get_db_config(p_tag):

    if await  check_server_backup_status(p_tag) > 0:
       return {'code': -1, 'msg': '服务器已禁用!'}

    if await check_db_config(p_tag) == 0:
       return {'code': -1, 'msg': '备份标识不存在!'}

    if await check_backup_task_status(p_tag) > 0:
       return {'code': -1, 'msg': '备份任务已禁用!'}

    st = """SELECT  a.db_tag,
                    c.ip       as db_ip,
                    c.port     as db_port,
                    c.service  as db_service,
                    c.user     as db_user,
                    c.password as db_pass,
                    a.expire,a.bk_base,a.script_path,a.script_file,a.bk_cmd,a.run_time,
                    b.server_ip,b.server_port,b.server_user,b.server_pass,b.server_os,
                    a.comments,a.python3_home,a.backup_databases,a.api_server,a.status
            FROM t_db_config a,t_server b,t_db_source c
            WHERE a.server_id=b.id and a.db_id=c.id and a.db_tag='{0}' and b.status='1'""".format(p_tag)

    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    return {'code': 200, 'msg': rs}

async def get_task_tags():
    return await async_processer.query_dict_list("SELECT  a.db_tag FROM t_db_config a  WHERE a.status='1'")

async def save_backup_total(config):
    vv = " where db_tag='{0}' and create_date='{1}'".format(config['db_tag'], config['create_date'])
    if await check_tab_exists('t_db_backup_total',vv)==0:
       st = '''insert into t_db_backup_total(db_tag,create_date,bk_base,total_size,start_time,end_time,elaspsed_backup,elaspsed_gzip,status)
                   values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}')
              '''.format(config['db_tag'],config['create_date'],config['bk_base'],config['total_size'],
                         config['start_time'],config['end_time'],config['elaspsed_backup'],
                         config['elaspsed_gzip'],config['status'])
    else:
       st = '''update t_db_backup_total
                    set bk_base     = '{}',
                        total_size  = '{}',
                        start_time  = '{}',
                        end_time    = '{}',
                        elaspsed_backup = '{}',
                        elaspsed_gzip = '{}',
                        status = '{}'
                  where db_tag = '{}' and create_date='{}'
              '''.format(config['bk_base'], config['total_size'],config['start_time'],
                         config['end_time'], config['elaspsed_backup'],config['elaspsed_gzip'],
                         config['status'],config['db_tag'],config['create_date'])
    try:
       await async_processer.exec_sql(st)
       return {'code':200,'msg':'success'}
    except:
       traceback.print_exc()
       return {'code': -1, 'msg': 'failure'}

async def save_backup_detail(config):
    vv = " where db_tag='{}' and db_name='{}' and create_date='{}'".format(config['db_tag'] ,config['db_name'],config['create_date'])
    if await check_tab_exists('t_db_backup_detail',vv)==0:
       st = '''insert into t_db_backup_detail
                (db_tag,create_date,db_name,bk_path,file_name,db_size,start_time,end_time,elaspsed_backup,elaspsed_gzip,status,error)
              values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}')
            '''.format(config['db_tag'],config['create_date'],config['db_name'],
                       config['bk_path'],config['file_name'],config['db_size'],
                       config['start_time'],config['end_time'],config['elaspsed_backup'],
                       config['elaspsed_gzip'],config['status'],config['error'])
    else:
        st= '''update t_db_backup_detail
                    set bk_path         = '{0}',
                        file_name       = '{1}',
                        db_size         = '{2}',
                        start_time      = '{3}',
                        end_time        = '{4}',
                        elaspsed_backup = '{5}',
                        elaspsed_gzip   = '{6}',
                        status          = '{7}',
                        error           = '{8}'
                where db_tag = '{9}' and db_name='{10}' and create_date='{11}'
                '''.format(config['bk_path'],config['file_name'],config['db_size'],
                           config['start_time'],config['end_time'], config['elaspsed_backup'],
                           config['elaspsed_gzip'],config['status'],config['error'],
                           config['db_tag'],config['db_name'],config['create_date'])
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def update_backup_status(p_tag):
    cfg = await get_db_config(p_tag)
    if cfg['code']!=200:
       return cfg
    cmd = 'ps -ef |grep {0} | grep -v grep |wc -l'.format(p_tag)
    res = exec_ssh_cmd(cfg,cmd)
    if res['status']:
       out = res['stdout'].decode().replace('\n','')
       if out == '0':
           await async_processer.exec_sql("update t_db_config set task_status={} where db_tag='{0}'".format(p_tag))
           return {'code':0,'msg':'已停止!' if out=='0' else '运行中!'}
       if out == '1':
           await async_processer.exec_sql("update t_db_config set task_status=1 where db_tag='{0}'".format(p_tag))
           return {'code': 1, 'msg': '运行中!'}
    else:
        return res

async def write_remote_crontab(v_tag):
    cfg = await get_db_config(v_tag)
    if cfg['code']!=200:
       return cfg

    v_cmd   = '{0}/db_backup.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],v_tag)
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
       return {'code':200,'msg':res['stdout']}
    else:
       return {'code':-1,'msg':'failure!'}

async def run_remote_backup_task(v_tag):
    cfg = await get_db_config(v_tag)
    if cfg['code']!=200:
       return cfg
    cmd = 'nohup {0}/db_backup.sh {1} {2} &>/tmp/backup.log &>/dev/null &'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],v_tag)
    res = exec_ssh_cmd(cfg,cmd)
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def stop_remote_backup_task(v_tag):
    cfg = await get_db_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd1 = "ps -ef | grep $$TAG$$ |grep -v grep | wc -l".replace('$$TAG$$', v_tag)
    cmd2 = "ps -ef | grep {0} |grep -v grep | awk '{print $2}'  | xargs kill -9".format(v_tag)
    res  = exec_ssh_cmd(cfg, cmd1)
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

async def transfer_remote_file(v_tag):
    cfg  = await get_db_config(v_tag)
    if cfg['code']!=200:
       return cfg
    f_local,f_remote = gen_transfer_file(cfg,'backup',cfg['msg']['script_file'])
    if not ftp_transfer_file(cfg,f_local,f_remote):
       return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'backup', 'db_backup.bat')
    if not ftp_transfer_file(cfg,f_local, f_remote):
       return {'code': -1, 'msg': 'failure!'}
    f_local, f_remote = gen_transfer_file(cfg, 'backup', 'db_backup.sh')
    if not ftp_transfer_file(cfg,f_local, f_remote):
       return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd(v_tag):
    cfg = await get_db_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'mkdir -p {0}'.format(cfg['msg']['script_path'] + '/config')
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd3 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_backup.sh')
    cmd4 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_backup.bat')

    res = exec_ssh_cmd(cfg,cmd1)
    if not res['status']:
       return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg,cmd2)
    if not res['status']:
       return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg,cmd3)
    if not res['status']:
       return {'code': -1, 'msg': 'failure!'}
    res = exec_ssh_cmd(cfg, cmd4)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

