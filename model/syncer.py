#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 10:33
# @Author : ma.fei
# @File : syncer.py.py
# @Software: PyCharm

import traceback
from utils.common import check_tab_exists,aes_decrypt,get_time2,gen_transfer_file
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper

async def check_server_sync_status(p_tag):
    st = "select count(0) from t_db_sync_config a,t_server b where a.server_id=b.id and a.sync_tag='{0}' and b.status='0'".format(p_tag)
    rs =  await async_processer.query_one(st)
    return rs[0]

async def check_db_sync_config(p_tag):
    st = "select count(0) from t_db_sync_config where sync_tag='{0}'".format(p_tag)
    rs = await async_processer.query_one(st)
    return rs[0]

async def check_sync_task_status(p_tag):
    st = "select count(0) from t_db_sync_config a,t_server b where a.server_id=b.id and a.sync_tag='{0}' and a.status='0'".format(p_tag)
    rs = await async_processer.query_one(st)
    return rs[0]

async def get_real_sync_status(p_tag):
    try:
        st = "select real_sync_status from t_db_sync_config where sync_tag='{}'".format(p_tag)
        rs = await async_processer.query_dict_one(st)
        return {'code':200,'msg':rs}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':str(e)}

async def set_real_sync_status(p_tag,p_status):
    try:
        st = "update t_db_sync_config set real_sync_status='{}' where sync_tag='{}'".format(p_status,p_tag)
        await async_processer.exec_sql(st)
        return {'code':200,'msg':'success'}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':str(e)}


async def get_db_sync_config(p_tag):
    if await check_server_sync_status(p_tag)>0:
       return {'code': -1, 'msg': '同步服务器已禁用!'}

    if await check_db_sync_config(p_tag)==0:
       return {'code': -2, 'msg': '同步标识不存在!'}

    if await check_sync_task_status(p_tag) > 0:
       return {'code': -3, 'msg': '同步任务已禁用!'}

    st = '''
SELECT a.sync_tag,a.sync_ywlx,
        (SELECT dmmc FROM t_dmmx WHERE dm='08' AND dmm=a.sync_ywlx) AS sync_ywlx_name,
        a.sync_type,
        (SELECT dmmc FROM t_dmmx WHERE dm='09' AND dmm=a.sync_type) AS sync_type_name,
        CASE WHEN c.service='' THEN 
          CONCAT(c.ip,':',c.port,':',IFNULL(a.sync_schema,'information_schema'),':',c.user,':',c.password)
        ELSE
          CONCAT(c.ip,':',c.port,':',c.service,':',c.user,':',c.password)
        END AS sync_db_sour,      
        c.id_ro AS id_ro,
        a.log_db_id AS log_db_id,
        a.log_db_name AS log_db_name,
        CASE WHEN d.service='' THEN 
          CONCAT(d.ip,':',d.port,':',IFNULL(a.sync_schema_dest,IFNULL(a.sync_schema,'information_schema')),':',d.user,':',d.password)
        ELSE
          CONCAT(d.ip,':',d.port,':',IFNULL(d.service,'information_schema') ,':',d.user,':',d.password)
        END AS sync_db_dest,
        d.db_type as dest_db_type,
        a.server_id,b.server_desc,a.run_time,a.api_server,
        LOWER(a.sync_table) AS sync_table,a.batch_size,a.batch_size_incr,a.sync_gap,a.sync_col_name,a.sync_repair_day,
        a.sync_col_val,a.sync_time_type,a.script_path,a.script_file,a.comments,a.python3_home,
        a.status,a.process_num,a.apply_timeout,a.desc_db_prefix,a.ch_cluster_name,
        b.server_ip,b.server_port,b.server_user,b.server_pass,c.proxy_server,
        (SELECT dmmc FROM t_dmmx WHERE dm='36' AND dmm='01') AS proxy_local_port,
        (SELECT `value` FROM t_sys_settings WHERE `key`='send_server') AS send_server,
        (SELECT `value` FROM t_sys_settings WHERE `key`='send_port') AS send_port,
        (SELECT `value` FROM t_sys_settings WHERE `key`='sender') AS sender,
        (SELECT `value` FROM t_sys_settings WHERE `key`='sendpass') AS sendpass,
        (SELECT `value` FROM t_sys_settings WHERE `key`='receiver') AS receiver,
        -- (SELECT `value` FROM t_sys_settings WHERE `key`='REAL_SYNC_STATUS') AS real_sync_status
        a.real_sync_status
FROM t_db_sync_config a,t_server b,t_db_source c,t_db_source d
  WHERE a.server_id=b.id AND a.sour_db_id=c.id  AND a.desc_db_id=d.id 
    and a.sync_tag ='{0}' ORDER BY a.id,a.sync_ywlx
'''.format(p_tag)

    try:
        rs = await async_processer.query_dict_one(st)
        rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])

        if rs.get('id_ro') is not None and rs.get('id_ro') != '' and rs.get('id_ro') !='None':
            rs['ds_ro'] = await async_processer.query_dict_one(
                "select * from t_db_source where id={}".format(rs.get('id_ro')))

        if rs.get('log_db_id') is not None and rs.get('log_db_id') != '':
            rs['ds_log'] = await async_processer.query_dict_one(
                "select * from t_db_source where id={}".format(rs['log_db_id']))

        st = "SELECT sync_tag,db_name,schema_name,tab_name,sync_cols,sync_incr_col \
               FROM `t_db_sync_tab_config` \
               WHERE sync_tag='{}' and status='1' ORDER BY id".format(p_tag)
        rs['cols'] = await async_processer.query_dict_list(st)

        return {'code':200,'msg':rs}
    except Exception as e:
        traceback.print_exc()
        return {'code':-1,'msg':str(e)}

async def save_sync_log(config):
    st = '''insert into t_db_sync_tasks_log(sync_tag,create_date,duration,amount) 
             values('{0}','{1}','{2}','{3}')
           '''.format(config['sync_tag'],config['create_date'],config['duration'],config['amount'])
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except Exception as e:
        traceback.print_exc()
        return {'code': -1, 'msg': str(e)}

async def save_sync_real_log(config):
    print('save_sync_real_log=',config)
    st = '''insert into t_db_sync_real_log(sync_tag,create_date,event_amount,insert_amount,update_amount,delete_amount,ddl_amount,binlogfile,binlogpos,c_binlogfile,c_binlogpos) 
             values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}')
           '''.format(config['sync_tag'],
                      config['create_date'],
                      config['event_amount'],
                      config['insert_amount'],
                      config['update_amount'],
                      config['delete_amount'],
                      config['ddl_amount'],
                      config['binlogfile'],
                      config['binlogpos'],
                      config['c_binlogfile'],
                      config['c_binlogpos']
                      )
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except Exception as e:
        traceback.print_exc()
        return {'code': -1, 'msg': str(e)}

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
    except Exception as e:
        traceback.print_exc()
        return {'code': -1, 'msg': str(e)}

async def update_sync_status(p_tag,p_status):
    cfg = await get_db_sync_config(p_tag)
    if cfg['code']!=200:
       return cfg
    try:
        await async_processer.exec_sql("update t_db_sync_config set task_status={},task_create_time=now() where sync_tag='{}'".format(p_status,p_tag))
        return {'code': 200, 'msg': 'success'}
    except Exception as e:
        traceback.print_exc()
        return {'code': -1, 'msg': str(e)}

async def run_remote_sync_task(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    ssh = ssh_helper(cfg)
    cmd1 = 'nohup {0}/db_sync.sh {1} {2} &>/dev/null &'.format(cfg['msg']['script_path'], cfg['msg']['script_file'], v_tag)
    cmd2 = 'nohup {0}/db_agent.sh &>/dev/null &'.format(cfg['msg']['script_path'])
    res  = ssh.exec(cmd1)
    if res['status']:
        res = ssh.exec(cmd2)
        if res['status']:
           res = {'code': 200, 'msg': res['stdout']}
        else:
           res = {'code': -1, 'msg': 'failure!'}
    else:
        res = {'code': -1, 'msg': 'failure!'}
    ssh.close()
    return res

async def stop_remote_sync_task(v_tag):
    cfg = await get_db_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg
    cmd1 = "ps -ef | grep {0} |grep -v grep | wc -l".format(v_tag)
    cmd2 = "ps -ef | grep $$SYNC_TAG$$ |grep -v grep | awk '{print $2}'  | xargs kill -9".replace('$$SYNC_TAG$$',v_tag)

    if (await update_sync_status(v_tag, '0'))['code']==-1:
        return {'code': -1, 'msg': 'update_sync_status failure!'}

    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd1)
    if  res['status']:
        res = ssh.exec(cmd2)
        if res['status']:
           res = {'code': 200, 'msg': res['stdout']}
        else:
           res = {'code': -1, 'msg': 'failure!'}
    else:
        res = {'code': -1, 'msg': 'failure!'}
    return res

def write_remote_crontab_sync(cfg,ssh):
    v_cmd    = '{0}/db_sync.sh {1} {2}'.format(cfg['msg']['script_path'],cfg['msg']['script_file'],cfg['msg']['sync_tag'])

    v_cmd_   = '{0}/db_agent.sh '.format(cfg['msg']['script_path'])

    # v_cls    = '{0}/db_sync.sh {1} {2}'.format(cfg['msg']['script_path'],'mysql2clickhouse_clear.py',cfg['msg']['sync_tag'])
    #
    # v_cls2   = '{0}/db_sync.sh {1} {2}'.format(cfg['msg']['script_path'],'mysql2mysql_real_clear.py',cfg['msg']['sync_tag'])
    #
    # v_cls3 = '{0}/db_sync.sh {1} {2}'.format(cfg['msg']['script_path'], 'mysql2clickhouse_clear_cluster.py',cfg['msg']['sync_tag'])

    v_cron   = '''crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config'''.format(cfg['msg']['sync_tag'],cfg['msg']['comments'],cfg['msg']['sync_tag'],cfg['msg']['run_time'],v_cmd)

    v_cron_  = '''crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config'''.format(cfg['msg']['sync_tag'], cfg['msg']['comments'], cfg['msg']['sync_tag'], cfg['msg']['run_time'], v_cmd)

    v_agent  = '''sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config'''.format('db_agent', '数据库代理服务', 'db_agent.py', '*/1 * * * *', v_cmd_)

    v_agent_ = '''sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config'''.format('db_agent', '数据库代理服务', 'db_agent.py', '*/1 * * * *', v_cmd_)

    # v_clear  = '''echo  -e "\n#{0} tag={1}\n{2} {3} &>/dev/null &" >> /tmp/config'''.format('实时日志清理[mysql->clickhouse]', cfg['msg']['sync_tag'], '0 1 * * * ', v_cls)
    #
    # v_clear_ = '''echo  -e "\n#{0} tag={1}\n#{2} {3} &>/dev/null &" >> /tmp/config'''.format('实时日志清理[mysql->clickhouse]', cfg['msg']['sync_tag'], '0 1 * * * ', v_cls)
    #
    # v_clear2 = '''echo  -e "\n#{0} tag={1}\n{2} {3} &>/dev/null &" >> /tmp/config'''.format('实时日志清理[mysql->mysql]', cfg['msg']['sync_tag'], '0 1 * * * ', v_cls2)
    #
    # v_clear2_ = '''echo -e "\n#{0} tag={1}\n#{2} {3} &>/dev/null &" >> /tmp/config'''.format('实时日志清理[mysql->mysql]', cfg['msg']['sync_tag'], '0 1 * * * ', v_cls2)
    #
    # v_clear3 = '''echo  -e "\n#{0} tag={1}\n{2} {3} &>/dev/null &" >> /tmp/config'''.format('实时日志清理[mysql->mysql]', cfg['msg']['sync_tag'], '0 1 * * * ', v_cls3)
    #
    # v_clear3_ = '''echo -e "\n#{0} tag={1}\n#{2} {3} &>/dev/null &" >> /tmp/config'''.format('实时日志清理[mysql->mysql]', cfg['msg']['sync_tag'], '0 1 * * * ', v_cls3)

    v_cron2  = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''

    v_cron3  = '''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}'''.format(cfg['msg']['script_path'], cfg['msg']['script_path'], get_time2())

    v_cron4  = '''crontab /tmp/config'''

    if cfg['msg']['status']=='1':
       if not ssh.exec(v_cron)['status']:
          return {'code': -1, 'msg': 'failure!'}

       if not ssh.exec(v_agent)['status']:
          return {'code': -1, 'msg': 'failure!'}

       # if cfg['msg']['sync_tag'].count('logger') >0:
       #     if cfg['msg']['sync_type'] == '8':
       #        if cfg['msg']['dest_db_type'] == '10':
       #           if not ssh.exec(v_clear3)['status']:
       #                return {'code': -1, 'msg': 'failure!'}
       #        else:
       #           if not ssh.exec(v_clear)['status']:
       #                return {'code': -1, 'msg': 'failure!'}
       #
       #     if cfg['msg']['sync_type'] == '2':
       #         if not ssh.exec(v_clear2)['status']:
       #            return {'code': -1, 'msg': 'failure!'}

    else:
       if not ssh.exec(v_cron_)['status']:
          return {'code': -1, 'msg': 'failure!'}

       if not ssh.exec(v_agent_)['status']:
          return {'code': -1, 'msg': 'failure!'}

       # if cfg['msg']['sync_tag'].count('logger') > 0:
       #     if cfg['msg']['sync_type'] == '8':
       #         if cfg['msg']['dest_db_type'] == '10':
       #             if not ssh.exec(v_clear3_)['status']:
       #                 return {'code': -1, 'msg': 'failure!'}
       #         else:
       #             if not ssh.exec(v_clear_)['status']:
       #                 return {'code': -1, 'msg': 'failure!'}
       #     if cfg['msg']['sync_type'] == '2':
       #         if not ssh.exec(v_clear2_)['status']:
       #             return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron2)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron3)['status']:
       return {'code': -1, 'msg': 'failure!'}

    if not ssh.exec(v_cron4)['status']:
       return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec('crontab -l')
    print("res['stdout']=",res['stdout'])

    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

def transfer_remote_file_sync(cfg,ssh,ftp):
    res = ssh.exec('mkdir -p {0}'.format(cfg['msg']['script_path']))
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'syncer', cfg['msg']['script_file'])
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'db_sync.sh')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'db_agent.sh')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'db_agent.py')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'mysql2clickhouse_clear_cluster.py')
    # if not ftp.transfer(f_local, f_remote):
    #     return {'code': -1, 'msg': 'failure!'}
    #
    # f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'mysql2clickhouse_clear.py')
    # if not ftp.transfer(f_local, f_remote):
    #     return {'code': -1, 'msg': 'failure!'}
    #
    # f_local, f_remote = gen_transfer_file(cfg, 'syncer', 'mysql2mysql_real_clear.py')
    # if not ftp.transfer(f_local, f_remote):
    #     return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}


def run_remote_cmd_sync(cfg,ssh):
    cmd1 = 'mkdir -p {0}'.format(cfg['msg']['script_path'] + '/config')
    cmd2 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd3 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_sync.sh')
    cmd4 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_agent.sh')
    cmd5 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_agent.py')
    #cmd6 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'mysql2clickhouse_clear.py')
    #cmd7 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'mysql2mysql_real_clear.py')
    #cmd8 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'mysql2clickhouse_clear_cluster.py')

    res = ssh.exec(cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd3)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd4)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd5)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    # res = ssh.exec(cmd6)
    # if not res['status']:
    #     return {'code': -1, 'msg': 'failure!'}
    #
    # res = ssh.exec(cmd7)
    # if not res['status']:
    #     return {'code': -1, 'msg': 'failure!'}
    #
    # res = ssh.exec(cmd8)
    # if not res['status']:
    #     return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}


async def push(tag):
    cfg = await get_db_sync_config(tag)
    if cfg['code'] != 200:
        return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)

    res =  transfer_remote_file_sync(cfg,ssh,ftp)
    if res['code'] != 200:
        raise Exception('transfer_remote_file error!')

    res =  run_remote_cmd_sync(cfg,ssh)
    if res['code'] != 200:
        raise Exception('run_remote_cmd error!')

    res =  write_remote_crontab_sync(cfg,ssh)
    if res['code'] != 200:
        traceback.print_exc()
        raise Exception('write_remote_crontab error!')

    ssh.close()
    ftp.close()
    return res

