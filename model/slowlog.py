#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:06
# @Author : ma.fei
# @File : slowlog.py.py
# @Software: PyCharm

import  traceback
from utils.common import get_time2,aes_decrypt,format_sql,gen_transfer_file
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper

async def get_slow_config(p_slow_id):
    if await check_server_slow_status(p_slow_id)>0:
       return {'code': -1, 'msg': '采集服务器已禁用!'}

    if await check_slow_config(p_slow_id)==0:
       return {'code': -1, 'msg': '慢日志标识不存在!'}

    st = '''SELECT   a.id as slow_id, concat(a.inst_id,'') as inst_id,
                     a.python3_home,a.script_path, a.script_file,
                     a.api_server,a.log_file,a.query_time,
                     a.exec_time,a.run_time,a.status,
                     c.server_ip as db_ip, b.inst_port as db_port,'' as db_service, b.mgr_user as db_user,
                     b.mgr_pass as db_pass, b.inst_type as db_type,
                     b.inst_name, b.inst_ver,b.inst_ip_in,
                     b.is_rds,c.server_ip,c.server_port,
                     c.server_user,c.server_pass,c.server_desc
            FROM t_slow_log a ,t_db_inst b,t_server c
             where a.inst_id = b.id and a.server_id=c.id and a.id='{0}'  ORDER BY a.id'''.format(p_slow_id)
    rs= await async_processer.query_dict_one(st)
    rs['server_pass'] = await aes_decrypt(rs['server_pass'], rs['server_user'])
    rs_cfg = await async_processer.query_dict_list("SELECT TYPE,VALUE,NAME FROM `t_db_inst_parameter` WHERE inst_id={}".format(rs['inst_id']))

    if rs['inst_ver'] == '1':
        rs_dm = await async_processer.query_dict_list("SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.6_download_url'")
    else:
        rs_dm = await async_processer.query_dict_list("SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.7_download_url'")
    step_slow = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='7' and version='{}' ORDER BY id'''.format(rs['inst_ver']))

    rs['cfg']  = rs_cfg
    rs['dpath'] = rs_dm
    rs['step_slow'] = step_slow
    return {'code': 200, 'msg': rs}

async def check_slow_config(p_slow_id):
    st = "select count(0) from t_slow_log where id='{0}'".format(p_slow_id)
    return  (await async_processer.query_one(st))[0]

async def check_server_slow_status(p_slow_id):
    st = "select count(0) from t_slow_log a,t_db_inst b ,t_server c \
                  where a.inst_id=b.id and b.server_id=c.id  and a.id='{0}' and c.status='0'".format(p_slow_id)
    return  (await async_processer.query_one(st))[0]

async def save_slow_log(config):
        st = '''insert into t_slow_detail
                   (inst_id,sql_id,templete_id,finish_time,USER,HOST,ip,thread_id,query_time,lock_time,
                    rows_sent,rows_examined,db,sql_text,finger,bytes,cmd,pos_in_log)
                 values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
             '''.format(config.get('inst_id'),
                        config.get('sql_id'),
                        config.get('templete_id'),
                        config.get('finish_time'),
                        config.get('user'),
                        config.get('host'),
                        config.get('ip'),
                        config.get('thread_id'),
                        config.get('query_time'),
                        config.get('lock_time'),
                        config.get('rows_sent'),
                        config.get('rows_examined'),
                        config.get('db'),
                        format_sql(config.get('sql_text')),
                        format_sql(config.get('finger')),
                        config.get('bytes'),
                        config.get('cmd'),
                        config.get('pos_in_log'))
        try:
            await async_processer.exec_sql(st)
            return {'code': 200, 'msg': 'success'}
        except:
            traceback.print_exc()
            return {'code': -1, 'msg': 'failure'}

async def write_remote_crontab_slow(v_flow_id):
    cfg = await get_slow_config(v_flow_id)
    if cfg['code']!=200:
       return cfg

    v_cmd_c = '{0}/gather_slow.sh cut {1}'.format(cfg['msg']['script_path'],v_flow_id)
    v_cmd_s = '{0}/gather_slow.sh stats {1}'.format(cfg['msg']['script_path'],v_flow_id)

    v_cron0 = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} slow_id={2}\n{3} {4} &>/dev/null & #slow_id={5}" >> /tmp/config  && crontab /tmp/config
              '''.format("slow_id="+v_flow_id,cfg['msg']['inst_name']+'日志切割任务',v_flow_id,'0 0 * * *',v_cmd_c,v_flow_id)

    v_cron1 = '''
                echo  -e "\n#{} slow_id={}\n{} {} &>/dev/null & #slow_id={}" >> /tmp/config  && crontab /tmp/config
              '''.format(cfg['msg']['inst_name']+'慢日志采集任务', v_flow_id, cfg['msg']['run_time'], v_cmd_s, v_flow_id)

    v_cron2 = '''
                crontab -l > /tmp/config && sed -i "/{}/d" /tmp/config && echo  -e "\n#{} slow_id={}\n{} {} &>/dev/null & #slow_id={}" >> /tmp/config  && crontab /tmp/config
              '''.format("slow_id="+v_flow_id,cfg['msg']['inst_name'] + '慢日志采集任务', v_flow_id, cfg['msg']['run_time'], v_cmd_s,v_flow_id)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config >> /tmp/config && crontab /tmp/config
              '''.format(v_flow_id)

    v_cron3 = '''crontab -l > /tmp/config && sed -i '/^$/{N;/\\n$/D};' /tmp/config && crontab /tmp/config'''

    v_cron4 = '''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}
              '''.format(cfg['msg']['script_path'],cfg['msg']['script_path'],get_time2())

    ssh = ssh_helper(cfg)
    if cfg['msg']['status'] == '0':
       if ssh.exec(v_cron_)['status']:
          return {'code': -1, 'msg': 'failure!'}

       if ssh.exec(v_cron3)['status']:
          return {'code': -1, 'msg': 'failure!'}

    if cfg['msg']['status'] == '1':
       if cfg['msg']['is_rds'] == 'N':
          if ssh.exec(v_cron0)['status']:
             return {'code': -1, 'msg': 'failure!'}

          if ssh.exec(v_cron1)['status']:
             return {'code': -1, 'msg': 'failure!'}

          if ssh.exec( v_cron3)['status']:
             return {'code': -1, 'msg': 'failure!'}

          if ssh.exec(v_cron4)['status']:
             return {'code': -1, 'msg': 'failure!'}
       else:
          if ssh.exec(v_cron2)['status']:
             return {'code': -1, 'msg': 'failure!'}

          if ssh.exec(v_cron3)['status']:
             return {'code': -1, 'msg': 'failure!'}

          if ssh.exec(v_cron4)['status']:
             return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec('crontab -l')
    ssh.close()
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def transfer_remote_file_slow(v_tag):
    cfg = await get_slow_config(v_tag)
    if cfg['code']!=200:
       return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'transfer', cfg['msg']['script_file'])
    if not ftp.transfer(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'transfer', 'gather_slow.sh')
    if not ftp.transfer(cfg, f_local, f_remote):
        return {'code': -1, 'msg': 'failure!'}

    ssh.close()
    ftp.close()
    return {'code': 200, 'msg': 'success!'}

async def run_remote_cmd_slow(v_tag):
    cfg = await get_slow_config(v_tag)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x {0}/{1}'.format(cfg['msg']['script_path'], 'gather_slow.sh')
    cmd3 = 'nohup {0}/gather_slow.sh update &>/tmp/gather_slow.log &'.format(cfg['msg']['script_path'])
    cmd4 = 'nohup  {0}/gather_slow.sh cut {1} &>>/tmp/gather_slow.log &'.format(cfg['msg']['script_path'], v_tag)

    ssh = ssh_helper(cfg)
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

    ssh.close()
    return {'code': 200, 'msg': 'success!'}
