#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:18
# @Author : ma.fei
# @File : backup.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists, aes_decrypt,gen_transfer_file,get_ds_by_dsid
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper

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
       return {'code': -1, 'msg': '备份标识:`{}`不存在!'.format(p_tag)}

    if await check_backup_task_status(p_tag) > 0:
       return {'code': -1, 'msg': '备份任务已禁用!'}

    st = """SELECT  a.db_tag,
                    c.id         as ds_id,
                    c.related_id as related_id,
                    c.ip         as db_ip,
                    c.port       as db_port,
                    c.service    as db_service,
                    c.user       as db_user,
                    c.password   as db_pass,
                    c.db_type    as db_type,
                    a.expire,a.bk_base,a.script_path,a.script_file,a.bk_cmd,a.run_time,
                    b.server_ip,b.server_port,b.server_user,b.server_pass,b.server_os,
                    a.comments,a.python3_home,a.backup_databases,a.api_server,a.status,a.binlog_status,
                    a.oss_status,a.oss_path,a.oss_cloud,
                    (select dmmc from t_dmmx where dm='36' and dmm='01') as proxy_local_port,
                    (select `value` from t_sys_settings where `key`='send_server') as send_server,
                    (select `value` from t_sys_settings where `key`='send_port') as send_port,
                    (select `value` from t_sys_settings where `key`='sender') as sender,
                    (select `value` from t_sys_settings where `key`='sendpass') as sendpass,
                    (select `value` from t_sys_settings where `key`='receiver') as receiver
            FROM t_db_config a,t_server b,t_db_source c
            WHERE a.server_id=b.id and a.db_id=c.id and a.db_tag='{0}' and b.status='1'""".format(p_tag)

    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    if rs['db_type'] == '0':
        if rs['binlog_status'] == '1':
           try:
                print('from ds read master and slave status failure,try from related_id={} read ...'.format(rs['related_id']))
                ds = await get_ds_by_dsid(rs['related_id'])
                print('ds=', ds)
                ms = await async_processer.query_dict_one_by_ds(ds, 'show master status')
                print('ms=', ms)
                sv = await async_processer.query_dict_one_by_ds(ds, 'show slave status')
                print('sv=', sv)
                ro = await async_processer.query_dict_one_by_ds(ds, "SHOW VARIABLES LIKE 'read_only'")
                print('ro=', ro)
                if ms is not None:
                    rs['ds'] = ms
                if sv is not None:
                    rs['sv'] = sv
                if ro is not None:
                    rs['ro'] = False if ro['value'] == 'OFF' else True
                print('from related_id read master and slave status ok...')
           except:
                try:
                    print('from ds read master and slave status!')
                    ds = await get_ds_by_dsid(rs['ds_id'])
                    ms = await async_processer.query_dict_one_by_ds(ds, 'show master status')
                    sv = await async_processer.query_dict_one_by_ds(ds, 'show slave status')
                    ro = await async_processer.query_dict_one_by_ds(ds, "SHOW VARIABLES LIKE 'read_only'")
                    print('ro=', ro)
                    if ms is not None:
                        rs['ds'] = ms
                    if sv is not None:
                        rs['sv'] = sv
                    if ro is not None:
                        rs['ro'] = False if ro['value'] == 'OFF' else True
                    print('from ds read master and slave status ok!')
                except:
                    traceback.print_exc()
                    print('Read master and slave status failure!')
                    return {'code': 500, 'msg': traceback.format_exc()}
    return {'code': 200, 'msg': rs}

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
    except Exception as e:
       traceback.print_exc()
       return {'code': -1, 'msg': str(e)}

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
    except Exception as e:
        traceback.print_exc()
        return {'code': -1, 'msg': str(e)}

async def run_remote_backup_task(v_tag):
    cfg = await get_db_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd = 'nohup {0}/db_backup.sh {1} {2} &>/tmp/backup.log &>/dev/null &'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],v_tag)
    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd)
    if res['status']:
        res = {'code': 200, 'msg': res['stdout']}
    else:
        res = {'code': -1, 'msg': 'failure!'}
    ssh.close()
    return res

async def stop_remote_backup_task(v_tag):
    cfg = await get_db_config(v_tag)
    if cfg['code']!=200:
       return cfg

    if (await update_backup_status(v_tag, '0'))['code']==-1:
        return {'code': -1, 'msg': 'update_backup_status failure!'}

    cmd1 = "ps -ef | grep $$TAG$$ |grep -v grep | wc -l".replace('$$TAG$$', v_tag)
    cmd2 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}'  | xargs kill -9".replace('$$TAG$$',v_tag)

    ssh  = ssh_helper(cfg)
    res  = ssh.exec(cmd1)
    if res['status']:
        if int(res['stdout'][0].replace('\n','')) == 0:
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

async def update_backup_status(p_tag,p_status):
    cfg = await get_db_config(p_tag)
    if cfg['code']!=200:
       return cfg
    try:
        await async_processer.exec_sql("update t_db_config set task_status={} where db_tag='{}'".format(p_status,p_tag))
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def write_remote_crontab(cfg,ssh):
    v_cmd   = '{0}/db_backup.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],cfg['msg']['db_tag'])
    v_cron0 = '''echo -e "#{0}" >/tmp/config'''.format(cfg['msg']['db_tag'])
    v_cron1 = '''
                 crontab -l >> /tmp/config && sed -i "/{0}/d" /tmp/config && echo -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config && crontab /tmp/config       
              '''.format(cfg['msg']['db_tag'],cfg['msg']['comments'],cfg['msg']['db_tag'],cfg['msg']['run_time'],v_cmd)
    v_cron1_= '''
                 crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(cfg['msg']['db_tag'], cfg['msg']['comments'], cfg['msg']['db_tag'], cfg['msg']['run_time'], v_cmd)
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
       return {'code':200,'msg':res['stdout']}
    else:
       return {'code':-1,'msg':'failure!'}

async def transfer_remote_file(cfg,ssh,ftp):
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'] + '/config')
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local,f_remote = gen_transfer_file(cfg,'backup',cfg['msg']['script_file'])
    if not ftp.transfer(f_local,f_remote):
       return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'backup', 'db_backup.bat')
    if not ftp.transfer(f_local, f_remote):
       return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'backup', 'db_backup.sh')
    if not ftp.transfer(f_local, f_remote):
       return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd(cfg,ssh):
    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_backup.sh')
    cmd3 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'db_backup.bat')

    if not ssh.exec(cmd1)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(cmd2)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(cmd3)['status']:
       return {'code': -1, 'msg': 'failure!'}
    return {'code': 200, 'msg': 'success!'}

async def push(tag):
    cfg = await get_db_config(tag)
    if cfg['code'] != 200:
        return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)

    res = await transfer_remote_file(cfg,ssh,ftp)
    if res['code'] != 200:
        raise Exception('transfer_remote_file error!')

    res = await run_remote_cmd(cfg,ssh)
    if res['code'] != 200:
        raise Exception('run_remote_cmd error!')

    res = await write_remote_crontab(cfg,ssh)
    if res['code'] != 200:
        raise Exception('write_remote_crontab error!')

    ssh.close()
    ftp.close()
    return res

