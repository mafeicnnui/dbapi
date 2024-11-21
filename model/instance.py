#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:06
# @Author : ma.fei
# @File : instance.py.py
# @Software: PyCharm

import traceback
from utils.common import aes_decrypt,exec_ssh_cmd,gen_transfer_file,ftp_transfer_file,format_sql,get_time2
from utils.mysql_async import async_processer
from utils.common import ssh_helper,ftp_helper

async def get_db_inst_config(p_inst_id):
    st = '''SELECT  b.server_ip,b.server_port,b.server_user,
                    b.server_pass,b.server_desc, b.market_id,
                    b.server_ip as db_ip, a.inst_port as db_port,'' as db_service,
                    a.mgr_user as db_user,a.mgr_pass as db_pass,a.inst_type as db_type,
                    a.inst_name,a.inst_status,a.python3_home,
                    a.api_server,a.script_path,a.script_file,
                    concat(a.id,'')  as inst_id,a.inst_ver
            FROM t_db_inst a,t_server b WHERE a.`server_id`=b.id AND a.id={}'''.format(p_inst_id)

    rs = await async_processer.query_dict_one(st)
    rs['server_pass']    = await aes_decrypt(rs['server_pass'], rs['server_user'])
    rs_cfg               = await async_processer.query_dict_list('''SELECT TYPE,VALUE,NAME FROM `t_db_inst_parameter` WHERE inst_id={}'''.format(p_inst_id))
    rs_step_create       = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='1' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_destroy      = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='2' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_start        = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='3' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_stop         = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='4' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    step_auostart        = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='5' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    step_cancel_auostart = await async_processer.query_dict_list('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='6' and version='{}' ORDER BY id'''.format(rs['inst_ver']))

    if rs['inst_ver'] == '1':
      rs_dm = await async_processer.query_dict_list("SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.6_download_url'")
    else:
      rs_dm = await async_processer.query_dict_list("SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.7_download_url'")

    rs['cfg']           = rs_cfg
    rs['step_create']   = rs_step_create
    rs['step_destroy']  = rs_step_destroy
    rs['step_start']    = rs_step_start
    rs['step_stop']     = rs_step_stop
    rs['step_auostart'] = step_auostart
    rs['step_cancel_auostart'] = step_cancel_auostart
    rs['dpath'] = rs_dm
    return {'code': 200, 'msg': rs}

async def save_inst_log(config):
     try:
         st ='''insert into t_db_inst_log(inst_id,type,message,create_date) 
                  values('{0}','{1}','{2}',now())'''.format(config['inst_id'],
                                                            config['type'],format_sql(config['message']))
         await async_processer.exec_sql(st)
         return {'code': 200, 'msg': 'success'}
     except:
         traceback.print_exc()
         return {'code': -1, 'msg': 'failure'}

async def upd_inst_status(config):
    try:
        st = "update t_db_inst set inst_status='{}',last_update_date=now() where id={}".format(config['status'],config['inst_id'])
        await async_processer.exec_sql(st)
        return {'code': 200, 'msg': 'success'}
    except:
        traceback.print_exc()
        return {'code': -1, 'msg': 'failure'}

async def upd_inst_reboot_status(config):
        try:
            st = "update t_db_inst set inst_reboot_flag='{}' where id={}".format(config['reboot_status'],config['inst_id'])
            await async_processer.exec_sql(st)
            return {'code': 200, 'msg': 'success'}
        except:
            traceback.print_exc()
            return {'code': -1, 'msg': 'failure'}

async def write_remote_crontab_inst(v_inst_id,v_flag):
    cfg = await get_db_inst_config(v_inst_id)
    if cfg['code']!=200:
       return cfg

    v_cmd   = '{0}/db_creator.sh status '.format(cfg['msg']['script_path'])

    v_cron  = '''
               crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null & #tag={5}" >> /tmp/config
              '''.format(v_inst_id,cfg['msg']['inst_name'],v_inst_id,'*/1 * * * *',v_cmd,v_inst_id)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config >> /tmp/config
              '''.format(v_inst_id)

    v_cron2 = '''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''

    v_cron3 = '''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}'''.\
             format(cfg['msg']['script_path'],cfg['msg']['script_path'],get_time2())

    v_cron4 = '''crontab /tmp/config'''

    if v_flag == 'destroy':
       exec_ssh_cmd(cfg, v_cron_)
       exec_ssh_cmd(cfg, v_cron4)

    if v_flag == 'create':
       exec_ssh_cmd(cfg, v_cron)
       exec_ssh_cmd(cfg, v_cron2)
       exec_ssh_cmd(cfg, v_cron3)
       exec_ssh_cmd(cfg, v_cron4)

    res = exec_ssh_cmd(cfg, 'crontab -l')
    if res['status']:
        return {'code': 200, 'msg': res['stdout']}
    else:
        return {'code': -1, 'msg': 'failure!'}

async def transfer_remote_file_inst(v_inst_id):
    cfg = await get_db_inst_config(v_inst_id)
    if cfg['code']!=200:
       return cfg

    ssh = ssh_helper(cfg)
    ftp = ftp_helper(cfg)
    cmd = 'mkdir -p {0}'.format(cfg['msg']['script_path'])
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'instance', cfg['msg']['script_file'])
    if not ftp.transfer(f_local, f_remote):
       return {'code': -1, 'msg': 'failure!'}

    f_local, f_remote = gen_transfer_file(cfg, 'instance', 'db_creator.sh')
    if not ftp.transfer(f_local, f_remote):
       return {'code': -1, 'msg': 'failure!'}

    ssh.close()
    ftp.close()
    return {'code': 200, 'msg': 'success!'}


async def run_remote_cmd_inst(v_inst_id):
    cfg = await get_db_inst_config(v_inst_id)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_creator.sh')
    cmd3 = 'nohup {0}/db_creator.sh create &>/tmp/db_create.log &'.format(cfg['msg']['script_path'])

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

    ssh.close()
    return {'code': 200, 'msg': 'success!'}

async def set_remote_cmd_inst(v_inst_id):
    cfg = await get_db_inst_config(v_inst_id)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_creator.sh')

    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    ssh.close()
    return {'code': 200, 'msg': 'success!'}

async def mgr_remote_cmd_inst(v_inst_id,v_flag):
    cfg = await get_db_inst_config(v_inst_id)
    if cfg['code'] != 200:
        return cfg
    cmd = 'nohup {0}/db_creator.sh {1} &>/tmp/db_manager.log &'.format(cfg['msg']['script_path'], v_flag)
    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}
    ssh.close()
    return {'code': 200, 'msg': 'success!'}

async def destroy_remote_cmd_inst(v_inst_id):
    cfg = await get_db_inst_config(v_inst_id)
    if cfg['code'] != 200:
        return cfg

    cmd1 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], cfg['msg']['script_file'])
    cmd2 = 'chmod +x  {0}/{1}'.format(cfg['msg']['script_path'], 'db_creator.sh')
    cmd3 = 'nohup {0}/db_creator.sh destroy &>/tmp/db_create.log &'.format(cfg['msg']['script_path'])

    ssh = ssh_helper(cfg)
    res = ssh.exec(cmd1)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cfg, cmd2)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    res = ssh.exec(cfg, cmd3)
    if not res['status']:
        return {'code': -1, 'msg': 'failure!'}

    ssh.close()
    return {'code': 200, 'msg': 'success!'}