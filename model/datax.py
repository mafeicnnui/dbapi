#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:03
# @Author : ma.fei
# @File : datax.py.py
# @Software: PyCharm

import paramiko
import traceback
from utils.common import exec_ssh_cmd,gen_transfer_file,ftp_transfer_file,db_config,aes_decrypt,get_mysql_columns
from utils.mysql_async import async_processer


async def check_datax_server_sync_status(p_tag):
    st = "select count(0) from t_db_sync_config a,t_server b \
              where a.server_id=b.id and a.sync_tag='{0}' and b.status='0'".format(p_tag)
    return  (await async_processer.query_one(st))[0]

async def check_datax_sync_task_status(p_tag):
    st = "select count(0) from t_datax_sync_config a,t_server b \
              where a.server_id=b.id and a.sync_tag='{0}' and a.status='0'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def check_datax_sync_config(p_tag):
    st = "select count(0) from t_datax_sync_config where sync_tag='{0}'".format(p_tag)
    return (await async_processer.query_one(st))[0]

async def save_datax_sync_log(config):
    st = '''insert into t_datax_sync_log(sync_tag,create_date,table_name,duration,amount) 
               values('{0}','{1}','{2}','{3}','{4}')
         '''.format(config['sync_tag'],config['create_date'],config['table_name'],config['duration'],config['amount'])
    try:
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def get_datax_sync_config(p_tag):

    # 检测同步服务器是否有效
    if await check_datax_server_sync_status(p_tag)>0:
       return {'code':-1,'msg':'同步服务器已禁用!'}

    #检测同步标识是否存在
    if await check_datax_sync_config(p_tag)==0:
       return {'code':-2,'msg':'同步标识不存在!'}

    #任务已禁用
    if await check_datax_sync_task_status(p_tag) > 0:
       return {'code': -3, 'msg': '同步任务已禁用!'}

    st = '''SELECT a.id, a.sync_tag,a.sync_ywlx,
                   CASE WHEN c.service='' THEN 
                     CONCAT(c.ip,':',c.port,':',a.sync_schema,':',c.user,':',c.password)
                   ELSE
                     CONCAT(c.ip,':',c.port,':',c.service,':',c.user,':',c.password)
                   END AS sync_db_sour, 
                   a.zk_hosts,a.python3_home,a.server_id,a.run_time,a.api_server,
                   LOWER(a.sync_table) AS sync_table,a.sync_gap,
                   a.sync_time_type,a.script_path,a.comments,a.status,
                   b.server_ip,b.server_port,b.server_user,b.server_pass,
                   a.hbase_thrift,a.sync_hbase_table,a.datax_home,a.sync_incr_col,a.sync_table,a.sync_incr_where
             FROM t_datax_sync_config a,t_server b,t_db_source c
             WHERE a.server_id=b.id 
               AND a.sour_db_id=c.id
               AND a.sync_tag ='{0}' ORDER BY a.id,a.sync_ywlx'''.format(p_tag)

    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    return { 'code':200,'msg':rs }

async def run_remote_datax_task(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    cmd = 'nohup {0}/datax_sync.sh {1} {2} &>/dev/null &'.format(cfg['msg']['script_path'], 'datax_sync.py', v_tag)
    if cfg['code']!=200:
       return cfg

    res = exec_ssh_cmd(cfg, cmd)
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def stop_datax_sync_task(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd1 = "ps -ef | grep $$TAG$$ |grep -v grep | wc -l".replace('$$TAG$$',v_tag)
    cmd2 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}' | xargs kill -9".replace('$$TAG$$',v_tag)
    res  = exec_ssh_cmd(cfg, cmd1)
    if res['status'] :
       if int(res['stdout']) ==  0:
          return {'code': -2, 'msg': 'task not running!'}
       else:
          res = exec_ssh_cmd(cfg, cmd2)
          if res['status']:
              return {'code': 200,'msg': 'success'}
          else:
              return {'code': -1, 'msg': 'failure!'}
    else:
       return {'code': -1, 'msg': 'failure!'}

async def query_datax_by_id(sync_id):
    st = """SELECT
                 a.sync_tag,a.server_id,a.sour_db_id,
                 a.sync_schema,a.sync_table,a.sync_incr_col,
                 e.user,e.password,a.sync_columns,a.sync_table,
                 CONCAT(e.ip,':',e.port,'/',a.sync_schema) AS mysql_url,
                 a.zk_hosts,a.sync_hbase_table,a.sync_hbase_rowkey,
                 a.sync_hbase_rowkey_sour,a.sync_hbase_rowkey_separator,
                 a.sync_hbase_columns,a.sync_incr_where,a.sync_ywlx,
                 a.sync_type,a.script_path,a.run_time,a.comments,
                 a.datax_home,a.sync_time_type,a.sync_gap,
                 a.api_server,a.status,a.python3_home
            FROM t_datax_sync_config a,t_server b ,t_dmmx c,t_dmmx d,t_db_source e
            WHERE a.server_id=b.id and b.status='1' 
             AND a.sour_db_id=e.id and c.dm='08' and d.dm='09'
             AND a.sync_ywlx=c.dmm and a.sync_type=d.dmm and a.id='{0}'""".format(sync_id)
    return await async_processer.query_dict_one(st)

async def process_templete(p_sync_id,p_templete):
    cfg = await query_datax_by_id(p_sync_id)
    p_templete['full'].replace('$$USERNAME$$',cfg['user'])
    p_templete['full'].replace('$$PASSWORD$$',await aes_decrypt(cfg['password'],cfg['user']))
    p_templete['full'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns(cfg))
    p_templete['full'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
    p_templete['full'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
    p_templete['full'].replace('$$USERNAME$$', cfg['user'])
    p_templete['full'].replace('$$ZK_HOSTS', cfg['zk_hosts'])
    p_templete['full'].replace('$$HBASE_TABLE_NAME$$', cfg['sync_hbase_table'])
    p_templete['full'].replace('$$HBASE_ROWKEY$$', cfg['sync_hbase_rowkey'])
    p_templete['full'].replace('$$HBASE_COLUMN_NAMES$$', cfg['sync_hbase_columns'])
    p_templete['incr'].replace('$$USERNAME$$', cfg['user'])
    p_templete['incr'].replace('$$PASSWORD$$', cfg)
    p_templete['incr'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns(cfg))
    p_templete['incr'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
    p_templete['incr'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
    p_templete['incr'].replace('$$USERNAME$$', cfg['user'])
    p_templete['incr'].replace('$$ZK_HOSTS', cfg['zk_hosts'])
    p_templete['incr'].replace('$$HBASE_TABLE_NAME$$', cfg['sync_hbase_table'])
    p_templete['incr'].replace('$$HBASE_ROWKEY$$', cfg['sync_hbase_rowkey'])
    p_templete['incr'].replace('$$HBASE_COLUMN_NAMES$$', cfg['sync_hbase_columns'])
    p_templete['incr'].replace('$$MYSQL_WHERE$$', cfg['sync_incr_where'])

async def query_datax_sync_dataxTemplete(sync_id):
    templete   = {
        'full' : await async_processer.query_one('select contents from t_templete where templete_id=1'),
        'incr' : await async_processer.query_one('select contents from t_templete where templete_id=2')
    }
    process_templete(sync_id,templete)
    return templete

async def get_datax_sync_templete(id):
    try:
      return {'code':200, 'msg':await query_datax_sync_dataxTemplete(id)}
    except Exception as e:
      return {'code': -1, 'msg': str(e)}

async def write_datax_sync_TempleteFile(sync_id,):
    # 获取 datax 配置
    cfg = await query_datax_by_id(sync_id)['sync_tag']

    # 获取模板内容至templete字典中
    templete = await query_datax_sync_dataxTemplete(sync_id)

    # 生成全量json文件
    v_datax_full_file = './script/{0}_full.json'.format(cfg)
    with open(v_datax_full_file, 'w') as f:
        f.write(templete['full'])

    # 生成增量json文件
    v_datax_incr_file = './script/{0}_incr.json'.format(cfg)
    with open(v_datax_incr_file, 'w') as f:
        f.write(templete['incr'])

    return  v_datax_full_file, v_datax_incr_file

def gen_datax_transfer_file(p_cfg,f_path):
    f_local  = '{0}'.format(f_path)
    f_remote = '{0}/{1}'.format(p_cfg['msg']['script_path'], f_path.split('/')[-1])
    return f_local,f_remote

async def transfer_datax_remote_file_sync(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = exec_ssh_cmd(cfg, cmd)
    if not res['status']:
       return {'code': -1, 'msg': 'failure!'}
    else:
       print("remote sync directory '{0}' created!".format(cfg['msg']['script_path']))

    # write json file
    f_datax_full,f_datax_incr = write_datax_sync_TempleteFile(cfg['msg']['id'])

    # send full json file
    f_local, f_remote = gen_datax_transfer_file(cfg, f_datax_full)
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # send incr json file
    f_local, f_remote = gen_datax_transfer_file(cfg, f_datax_incr)
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # replace and send datax_sync.py file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'datax_sync.py')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # replace repstr.sh file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'repstr.sh')
    if not ftp_transfer_file(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def run_datax_remote_cmd_sync(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x {0}'.format(cfg['msg']['script_path']+'/repstr.sh')
    cmd2 = 'chmod +x {0}'.format(cfg['msg']['script_path']+'/datax_sync.sh')
    cmd3 = 'chmod +x {0}'.format(cfg['msg']['script_path']+'/datax_sync.py')
    cmd4 = '{0}/repstr.sh {1}'.format(cfg['msg']['script_path'],cfg['msg']['script_path']+'/'+v_tag+'_full.json')
    cmd5 = '{0}/repstr.sh {1}'.format(cfg['msg']['script_path'],cfg['msg']['script_path']+'/'+v_tag+'_full.json')

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
    res = exec_ssh_cmd(cfg, cmd5)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}


async def write_datax_remote_crontab_sync(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg

    v_cmd   = '{0}/datax_sync.sh {1} {2}'.format(cfg['msg']['script_path'],'datax_sync.py', v_tag)

    v_cron  = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag,cfg['msg']['comments'],v_tag,cfg['msg']['run_time'],v_cmd)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag, cfg['msg']['comments'], v_tag, cfg['msg']['run_time'], v_cmd)

    v_cron2 = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''
    v_cron3 = '''crontab /tmp/config'''

    if cfg['msg']['status'] == '1':
        exec_ssh_cmd(cfg, v_cron)
    else:
        exec_ssh_cmd(cfg, v_cron_)
    exec_ssh_cmd(cfg, v_cron2)
    exec_ssh_cmd(cfg, v_cron3)
    res = exec_ssh_cmd(cfg, 'crontab -l')
    if res['status']:
       return {'code':200,'msg':res['stdout']}
    else:
       return {'code':-1,'msg':'failure!'}