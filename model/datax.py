#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:03
# @Author : ma.fei
# @File : datax.py.py
# @Software: PyCharm

import json
import time
import traceback
from utils.common import gen_transfer_file,aes_decrypt,get_mysql_columns
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper


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

    if await check_datax_server_sync_status(p_tag)>0:
       return {'code':-1,'msg':'同步服务器已禁用!'}

    if await check_datax_sync_config(p_tag)==0:
       return {'code':-2,'msg':'同步标识不存在!'}

    if await check_datax_sync_task_status(p_tag) > 0:
       return {'code': -3, 'msg': '同步任务已禁用!'}

    st = '''
SELECT a.id, a.sync_tag,a.sync_ywlx,a.sync_type,
       CASE WHEN c.service='' THEN 
         CONCAT(c.ip,':',c.port,':',a.sync_schema,':',c.user,':',c.password)
       ELSE
         CONCAT(c.ip,':',c.port,':',c.service,':',c.user,':',c.password)
       END AS sync_db_sour, 
       a.zk_hosts,a.python3_home,a.server_id,a.run_time,a.api_server,
       LOWER(a.sync_table) AS sync_table,a.sync_gap,
       a.sync_time_type,a.script_path,a.comments,a.status,
       b.server_ip,b.server_port,b.server_user,b.server_pass,
       a.hbase_thrift,a.sync_hbase_table,a.datax_home,a.sync_incr_col,a.sync_incr_where,
       a.es_service,
       a.es_index_name,
       a.es_type_name,
       a.sync_es_columns,
       a.doris_id,
       (select ip from t_db_source x where x.id=a.doris_id) as doris_ip,
       (select port from t_db_source x where x.id=a.doris_id) as doris_port,
       (select user from t_db_source x where x.id=a.doris_id) as doris_user,
       (select password from t_db_source x where x.id=a.doris_id) as doris_password,
       (select stream_load from t_db_source x where x.id=a.doris_id) as doris_stream_load,
       (select concat(x.ip,':',x.port) from t_db_source x where x.id=a.doris_id) as doris_jbdc_url,
       a.doris_db_name,
       a.doris_tab_name,
       a.doris_batch_size,
       a.doris_jvm,
       a.doris_tab_config,
       a.doris_sync_type
FROM t_datax_sync_config a,t_server b,t_db_source c
WHERE a.server_id=b.id 
AND a.sour_db_id=c.id
AND a.sync_tag ='{0}' ORDER BY a.id,a.sync_ywlx
'''.format(p_tag)
    rs = await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    return { 'code':200,'msg':rs }

async def run_remote_datax_task(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    if cfg['msg']['sync_type'] == '7':
       cmd = '{0}/datax_sync.sh {1} {2}'.format(cfg['msg']['script_path'], 'datax_sync_doris.py', v_tag)
    else:
       cmd = 'nohup {0}/datax_sync.sh {1} {2} &>/dev/null &'.format(cfg['msg']['script_path'], 'datax_sync.py', v_tag)
    print('cmd=',cmd)
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

async def stop_datax_sync_task(v_tag):
    cfg = await get_datax_sync_config(v_tag)
    if cfg['code']!=200:
       return cfg
    cmd1 = "ps -ef | grep $$TAG$$ |grep -v grep | wc -l".replace('$$TAG$$',v_tag)
    cmd2 = "ps -ef | grep $$TAG$$ |grep -v grep | awk '{print $2}' | xargs kill -9".replace('$$TAG$$',v_tag)

    ssh  = ssh_helper(cfg)
    res  = ssh.exec(cmd1)
    if res['status']:
       if int(res['stdout']) ==  0:
          res = {'code': -2, 'msg': 'task not running!'}
       else:
          res = ssh.exec(cmd2)
          if res['status']:
             res = {'code': 200,'msg': 'success!'}
          else:
             res = {'code': -1, 'msg': 'failure!'}
    else:
       res = {'code': -1, 'msg': 'failure!'}
    ssh.close()
    return res

async def query_datax_by_id(sync_id):
    st = """
SELECT
     a.sync_tag,a.server_id,a.sour_db_id,
     a.sync_schema,a.sync_table,a.sync_incr_col,
     e.user,e.password,a.sync_columns,a.sync_table,
     CONCAT(e.ip,':',e.port,'/',a.sync_schema) AS mysql_url,
     a.zk_hosts,a.sync_hbase_table,a.sync_hbase_rowkey,
     a.sync_hbase_rowkey_sour,a.sync_hbase_rowkey_separator,
     a.sync_hbase_columns,a.sync_incr_where,a.sync_ywlx,
     a.sync_type,a.script_path,a.run_time,a.comments,
     a.datax_home,a.sync_time_type,a.sync_gap,
     a.api_server,a.status,a.python3_home,
     a.hbase_thrift,
     a.es_service,
     a.es_index_name,
     a.es_type_name,
     a.sync_es_columns,
     a.doris_id,
     (select user from t_db_source x where x.id=a.doris_id) as doris_user,
     (select password from t_db_source x where x.id=a.doris_id) as doris_password,
     (select stream_load from t_db_source x where x.id=a.doris_id) as doris_stream_load,
     (select concat(x.ip,':',x.port) from t_db_source x where x.id=a.doris_id) as doris_jbdc_url,
     a.doris_db_name,
     a.doris_tab_name,
     a.doris_batch_size
FROM t_datax_sync_config a,t_server b ,t_dmmx c,t_dmmx d,t_db_source e
 WHERE a.server_id=b.id and b.status='1' and a.sour_db_id=e.id and c.dm='08' and d.dm='09'
    AND a.sync_ywlx=c.dmm and a.sync_type=d.dmm and a.id='{0}'
""".format(sync_id)
    return await async_processer.query_dict_one(st)


async def get_templete_by_sync_type(p_type):
    if p_type == '5':
        templete = {
            'full': (await async_processer.query_one('select contents from t_templete where templete_id=1'))[0],
            'incr': (await async_processer.query_one('select contents from t_templete where templete_id=2'))[0]
        }
    elif p_type == '6':
        templete = {
            'full': (await async_processer.query_one('select contents from t_templete where templete_id=3'))[0],
            'incr': (await async_processer.query_one('select contents from t_templete where templete_id=4'))[0]
        }
    elif p_type == '7':
        templete = {
            'full': (await async_processer.query_one('select contents from t_templete where templete_id=5'))[0],
            'incr': (await async_processer.query_one('select contents from t_templete where templete_id=6'))[0]
        }
    else:
        templete = {
            'full': '',
            'incr': '',
        }
    return  templete

def get_mysql_columns_doris(p_sync):
    v = ''
    for i in p_sync['sync_columns'].split(','):
       v=v+'''"{}",'''.format(i)
    return v[0:-1]


async def update_templete(cfg,templete):
    if  cfg['sync_type'] == '5':
        npass = await aes_decrypt(cfg['password'], cfg['user'])
        templete['full'] = templete['full'].replace('$$USERNAME$$', cfg['user'])
        templete['full'] = templete['full'].replace('$$PASSWORD$$', npass)
        templete['full'] = templete['full'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns(cfg))
        templete['full'] = templete['full'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
        templete['full'] = templete['full'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
        templete['full'] = templete['full'].replace('$$USERNAME$$', cfg['user'])
        templete['full'] = templete['full'].replace('$$ZK_HOSTS', cfg['zk_hosts'])
        templete['full'] = templete['full'].replace('$$HBASE_TABLE_NAME$$', cfg['sync_hbase_table'])
        templete['full'] = templete['full'].replace('$$HBASE_ROWKEY$$', cfg['sync_hbase_rowkey'])
        templete['full'] = templete['full'].replace('$$HBASE_COLUMN_NAMES$$', cfg['sync_hbase_columns'])
        templete['incr'] = templete['incr'].replace('$$USERNAME$$', cfg['user'])
        templete['incr'] = templete['incr'].replace('$$PASSWORD$$', npass)
        templete['incr'] = templete['incr'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns(cfg))
        templete['incr'] = templete['incr'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
        templete['incr'] = templete['incr'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
        templete['incr'] = templete['incr'].replace('$$USERNAME$$', cfg['user'])
        templete['incr'] = templete['incr'].replace('$$ZK_HOSTS', cfg['zk_hosts'])
        templete['incr'] = templete['incr'].replace('$$HBASE_TABLE_NAME$$', cfg['sync_hbase_table'])
        templete['incr'] = templete['incr'].replace('$$HBASE_ROWKEY$$', cfg['sync_hbase_rowkey'])
        templete['incr'] = templete['incr'].replace('$$HBASE_COLUMN_NAMES$$', cfg['sync_hbase_columns'])
        templete['incr'] = templete['incr'].replace('$$MYSQL_WHERE$$', cfg['sync_incr_where'])
    elif cfg['sync_type'] == '6':
        templete['full'] = templete['full'].replace('$$USERNAME$$', cfg['user'])
        templete['full'] = templete['full'].replace('$$PASSWORD$$', await aes_decrypt(cfg['password'], cfg['user']))
        templete['full'] = templete['full'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns(cfg))
        templete['full'] = templete['full'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
        templete['full'] = templete['full'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
        templete['full'] = templete['full'].replace('$$USERNAME$$', cfg['user'])
        templete['full'] = templete['full'].replace('$$ES_SERVICE$$', cfg['es_service'])
        templete['full'] = templete['full'].replace('$$ES_INDEX_NAME$$', cfg['es_index_name'])
        templete['full'] = templete['full'].replace('$$ES_TYPE_NAME$$', cfg['es_type_name'])
        templete['full'] = templete['full'].replace('$$ES_COLUMN_NAMES$$', cfg['sync_es_columns'])
        # replacre incr templete
        templete['incr'] = templete['incr'].replace('$$USERNAME$$', cfg['user'])
        templete['incr'] = templete['incr'].replace('$$PASSWORD$$', await aes_decrypt(cfg['password'], cfg['user']))
        templete['incr'] = templete['incr'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns(cfg))
        templete['incr'] = templete['incr'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
        templete['incr'] = templete['incr'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
        templete['incr'] = templete['incr'].replace('$$MYSQL_WHERE$$', cfg['sync_incr_where'])
        templete['full'] = templete['incr'].replace('$$ES_SERVICE$$', cfg['zk_hosts'])
        templete['full'] = templete['incr'].replace('$$ES_INDEX_NAME$$', cfg['sync_hbase_table'])
        templete['full'] = templete['incr'].replace('$$ES_TYPE_NAME$$', cfg['sync_hbase_rowkey'])
        templete['full'] = templete['incr'].replace('$$ES_COLUMN_NAMES$$', cfg['sync_es_columns'])
    elif cfg['sync_type'] == '7':
        print('cfg=',cfg)
        # replace full templete
        templete['full'] = templete['full'].replace('$$USERNAME$$', cfg['user'])
        templete['full'] = templete['full'].replace('$$PASSWORD$$', await aes_decrypt(cfg['password'], cfg['user']))
        templete['full'] = templete['full'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns_doris(cfg))
        templete['full'] = templete['full'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
        templete['full'] = templete['full'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
        templete['full'] = templete['full'].replace('$$USERNAME$$', cfg['user'])
        templete['full'] = templete['full'].replace('$$DORIS_FE_LOAD_URL$$', cfg['doris_stream_load'])
        templete['full'] = templete['full'].replace('$$DORIS_JDBC_URL$$', cfg['doris_jbdc_url'])
        templete['full'] = templete['full'].replace('$$DORIS_DATABASE$$', cfg['doris_db_name'])
        templete['full'] = templete['full'].replace('$$DORIS_TABLE$$', cfg['doris_tab_name'])
        templete['full'] = templete['full'].replace('$$DORIS_USER$$', cfg['doris_user'])
        templete['full'] = templete['full'].replace('$$DORIS_PASSWORD$$', await aes_decrypt(cfg['doris_password'],cfg['doris_user']))
        templete['full'] = templete['full'].replace('$$MAX_BATCH_ROWS$$', cfg['doris_batch_size'])
        # replacre incr templete
        templete['incr'] = templete['incr'].replace('$$USERNAME$$', cfg['user'])
        templete['incr'] = templete['incr'].replace('$$PASSWORD$$', await aes_decrypt(cfg['password'], cfg['user']))
        templete['incr'] = templete['incr'].replace('$$MYSQL_COLUMN_NAMES$$', get_mysql_columns_doris(cfg))
        templete['incr'] = templete['incr'].replace('$$MYSQL_TABLE_NAME$$', cfg['sync_table'])
        templete['incr'] = templete['incr'].replace('$$MYSQL_URL$$', cfg['mysql_url'])
        templete['incr'] = templete['incr'].replace('$$MYSQL_WHERE$$', cfg['sync_incr_where'])
        templete['incr'] = templete['incr'].replace('$$DORIS_FE_LOAD_URL$$', cfg['doris_stream_load'])
        templete['incr'] = templete['incr'].replace('$$DORIS_JDBC_URL$$', cfg['doris_jbdc_url'])
        templete['incr'] = templete['incr'].replace('$$DORIS_DATABASE$$', cfg['doris_db_name'])
        templete['incr'] = templete['incr'].replace('$$DORIS_TABLE$$', cfg['doris_tab_name'])
        templete['incr'] = templete['incr'].replace('$$DORIS_USER$$', cfg['doris_user'])
        templete['incr'] = templete['incr'].replace('$$DORIS_PASSWORD$$', await aes_decrypt(cfg['doris_password'],cfg['doris_user']))
        templete['incr'] = templete['incr'].replace('$$MAX_BATCH_ROWS$$', cfg['doris_batch_size'])
    return templete

async def process_templete(cfg):
    # get templete by type
    templete = await get_templete_by_sync_type(cfg['sync_type'])
    # replace templete
    templete = await  update_templete(cfg,templete)
    return templete

async def query_datax_sync_dataxTemplete(id):
    cfg = await query_datax_by_id(id)
    templete = await process_templete(cfg)
    return templete

async def write_datax_sync_TempleteFile(sync_id):
    # # 获取 datax 配置
    # cfg = await query_datax_by_id(sync_id)
    # print('write_datax_sync_TempleteFile=',cfg)

    tag = (await query_datax_by_id(sync_id))['sync_tag']

    # 获取模板内容至templete字典中
    templete = await query_datax_sync_dataxTemplete(sync_id)

    # 生成全量json文件
    v_datax_full_file = './script/{0}_full.json'.format(tag)
    with open(v_datax_full_file, 'w') as f:
        f.write(templete['full'])

    # 生成增量json文件
    v_datax_incr_file = './script/{0}_incr.json'.format(tag)
    with open(v_datax_incr_file, 'w') as f:
        f.write(templete['incr'])

    return  v_datax_full_file, v_datax_incr_file

async def get_datax_sync_templete(id):
    try:
      return {'code':200, 'msg':await query_datax_sync_dataxTemplete(id)}
    except Exception as e:
      return {'code': -1, 'msg': str(e)}

def gen_datax_transfer_file(p_cfg,f_path):
    f_local  = '{0}'.format(f_path)
    f_remote = '{0}/{1}'.format(p_cfg['msg']['script_path'], f_path.split('/')[-1])
    return f_local,f_remote

async def transfer_datax_remote_file_sync(cfg,ssh,ftp):
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = ssh.exec(cmd)
    if not res['status']:
       return {'code': -1, 'msg': 'failure!'}

    # write json file
    f_datax_full,f_datax_incr = await write_datax_sync_TempleteFile(cfg['msg']['id'])

    # send full json file
    f_local, f_remote = gen_datax_transfer_file(cfg, f_datax_full)
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # send incr json file
    f_local, f_remote = gen_datax_transfer_file(cfg, f_datax_incr)
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # replace and send datax_sync.sh file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'datax_sync.sh')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # replace and send datax_sync.py file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'datax_sync.py')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # replace and send datax_sync_es.py file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'datax_sync_es.py')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    # replace and send datax_sync_doris.py file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'datax_sync_doris.py')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}


    # replace repstr.sh file
    f_local, f_remote = gen_transfer_file(cfg, 'datax', 'repstr.sh')
    if not ftp.transfer(f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def run_datax_remote_cmd_sync(cfg,ssh):
    cmd1 = 'chmod +x {0}'.format(cfg['msg']['script_path']+'/repstr.sh')
    cmd2 = 'chmod +x {0}'.format(cfg['msg']['script_path']+'/datax_sync.sh')
    cmd3 = 'chmod +x {0}'.format(cfg['msg']['script_path']+'/datax_sync.py')
    cmd4 = 'chmod +x {0}'.format(cfg['msg']['script_path'] + '/datax_sync_es.py')
    cmd5 = 'chmod +x {0}'.format(cfg['msg']['script_path'] + '/datax_sync_doris.py')
    cmd6 = '{0}/repstr.sh {1}'.format(cfg['msg']['script_path'],cfg['msg']['script_path']+'/'+cfg['msg']['sync_tag']+'_full.json')
    cmd7 = '{0}/repstr.sh {1}'.format(cfg['msg']['script_path'],cfg['msg']['script_path']+'/'+cfg['msg']['sync_tag']+'_full.json')

    if not ssh.exec(cmd1)['status']:
        return {'code': -1, 'msg': 'failure!'}
    if not ssh.exec(cmd2)['status']:
        return {'code': -1, 'msg': 'failure!'}
    if not ssh.exec(cmd3)['status']:
        return {'code': -1, 'msg': 'failure!'}
    if not ssh.exec(cmd4)['status']:
        return {'code': -1, 'msg': 'failure!'}
    if not ssh.exec(cmd5)['status']:
        return {'code': -1, 'msg': 'failure!'}
    if not ssh.exec(cmd6)['status']:
        return {'code': -1, 'msg': 'failure!'}
    if not ssh.exec(cmd7)['status']:
        return {'code': -1, 'msg': 'failure!'}

    return {'code': 200, 'msg': 'success!'}

async def write_datax_remote_crontab_sync(cfg,ssh):

    print('write_datax_remote_crontab_sync=',cfg)

    if cfg['msg']['sync_type'] == '5':
        v_cmd = '{0}/datax_sync.sh {1} {2}'.format(cfg['msg']['script_path'], 'datax_sync.py', cfg['msg']['sync_tag'])
    elif cfg['msg']['sync_type']  == '6':
        v_cmd = '{0}/datax_sync.sh {1} {2}'.format(cfg['msg']['script_path'], 'datax_sync_es.py', cfg['msg']['sync_tag'])
    elif cfg['msg']['sync_type']  == '7':
        v_cmd = '{0}/datax_sync.sh {1} {2}'.format(cfg['msg']['script_path'], 'datax_sync_doris.py', cfg['msg']['sync_tag'])

    v_cron  = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(cfg['msg']['sync_tag'],cfg['msg']['comments'],cfg['msg']['sync_tag'],cfg['msg']['run_time'],v_cmd)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(cfg['msg']['sync_tag'], cfg['msg']['comments'], cfg['msg']['sync_tag'], cfg['msg']['run_time'], v_cmd)

    v_cron2 = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''
    v_cron3 = '''crontab /tmp/config'''

    if cfg['msg']['status'] == '1':
        if not ssh.exec(v_cron)['status']:
           return {'code': -1, 'msg': 'failure!'}
    else:
        if not ssh.exec(v_cron_)['status']:
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

async def push(tag):
    cfg = await get_datax_sync_config(tag)
    if cfg['code'] != 200:
        return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)

    res = await transfer_datax_remote_file_sync(cfg,ssh,ftp)
    if res['code'] != 200:
        raise Exception('transfer_datax_remote_file_sync error!')

    res = await run_datax_remote_cmd_sync(cfg,ssh)
    if res['code'] != 200:
        raise Exception('run_datax_remote_cmd_sync error!')

    res = await write_datax_remote_crontab_sync(cfg,ssh)
    if res['code'] != 200:
        raise Exception('write_datax_remote_crontab_sync error!')

    ssh.close()
    ftp.close()
    return res
