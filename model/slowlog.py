#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:06
# @Author : ma.fei
# @File : slowlog.py.py
# @Software: PyCharm

import os
import  paramiko
import  traceback

from utils.common import db_config,get_time2,aes_decrypt,format_sql,get_file_contents

def get_slow_config(p_slow_id):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    result = {}
    result['code'] = 200
    result['msg'] = ''

    #检测传输服务器是否有效
    if check_server_slow_status(p_slow_id)>0:
       result['code'] = -1
       result['msg'] = '采集服务器已禁用!'
       return result

    #检测慢日志标识是否存在
    if check_slow_config(p_slow_id)==0:
       result['code'] = -1
       result['msg'] = '慢日志标识不存在!'
       return result

    cr.execute('''SELECT  
                         a.id as slow_id,
                         concat(a.inst_id,'')  as inst_id,
                         a.python3_home,
                         a.script_path,
                         a.script_file,
                         a.api_server,
                         a.log_file,
                         a.query_time,
                         a.exec_time,
                         a.run_time,
                         a.status,
                         c.server_ip   AS db_ip,
                         b.inst_port   AS db_port,
                         ''            AS db_service,
                         b.mgr_user    AS db_user,
                         b.mgr_pass    AS db_pass,
                         b.inst_type   AS db_type,
                         b.inst_name,
                         b.inst_ver,
                         b.inst_ip_in,
                         b.is_rds,
                         c.server_ip ,
                         c.server_port,
                         c.server_user,
                         c.server_pass,
                         c.server_desc
                FROM t_slow_log a ,t_db_inst b,t_server c
                 where a.inst_id = b.id and a.server_id=c.id
                   and a.id='{0}'  ORDER BY a.id
            '''.format(p_slow_id))
    rs=cr.fetchone()

    cr.execute('''SELECT TYPE,VALUE,NAME FROM `t_db_inst_parameter` WHERE inst_id={}'''.format(rs['inst_id']))
    rs_cfg = cr.fetchall()

    if rs['inst_ver'] == '1':
        cr.execute(
            """SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.6_download_url'""")
    else:
        cr.execute(
            """SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.7_download_url'""")
    rs_dm = cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='7' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    step_slow = cr.fetchall()

    rs['cfg']  = rs_cfg
    rs['dpath'] = rs_dm
    rs['step_slow'] = step_slow
    result['msg']=rs
    cr.close()
    return result

def check_slow_config(p_slow_id):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    cr.execute('''select count(0) from t_slow_log where id='{0}'
               '''.format(p_slow_id))
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

def check_server_slow_status(p_slow_id):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    cr.execute('''select count(0) from t_slow_log a,t_db_inst b ,t_server c
                  where a.inst_id=b.id and b.server_id=c.id  and a.id='{0}' and c.status='0'
               '''.format(p_slow_id))
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

def save_slow_log(config):
    result = {}
    result['code'] = 200
    result['msg'] = 'success'
    try:
        # STR_TO_DATE('{}', '%Y%m%d %H:%i:%s')
        db = db_config()['db_mysql']
        cr = db.cursor()
        st = '''insert into t_slow_detail
                   (inst_id,sql_id,templete_id,finish_time,USER,HOST,ip,thread_id,query_time,lock_time,
                    rows_sent,rows_examined,db,sql_text,finger,bytes,cmd,pos_in_log)
                 values('{}','{}','{}','{}','{}','{}','{}',
                        '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
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
                        config.get('pos_in_log')
                       )
        print(st)
        cr.execute(st)
        db.commit()
        cr.close()
        return result
    except:
        print(traceback.print_exc())
        result['code'] = -1
        result['msg'] = '保存失败!'
        return result

def write_remote_crontab_slow(v_flow_id):
    result = get_slow_config(v_flow_id)
    if result['code']!=200:
       return result

    v_cmd_c = '{0}/gather_slow.sh cut {1}'.format(result['msg']['script_path'],v_flow_id)
    v_cmd_s = '{0}/gather_slow.sh stats {1}'.format(result['msg']['script_path'],v_flow_id)

    v_cron0 = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} slow_id={2}\n{3} {4} &>/dev/null & #slow_id={5}" >> /tmp/config  && crontab /tmp/config
              '''.format("slow_id="+v_flow_id,result['msg']['inst_name']+'日志切割任务',v_flow_id,'0 0 * * *',v_cmd_c,v_flow_id)

    v_cron1 = '''
                echo  -e "\n#{} slow_id={}\n{} {} &>/dev/null & #slow_id={}" >> /tmp/config  && crontab /tmp/config
              '''.format(result['msg']['inst_name']+'慢日志采集任务', v_flow_id, result['msg']['run_time'], v_cmd_s, v_flow_id)

    v_cron2 = '''
                crontab -l > /tmp/config && sed -i "/{}/d" /tmp/config && echo  -e "\n#{} slow_id={}\n{} {} &>/dev/null & #slow_id={}" >> /tmp/config  && crontab /tmp/config
              '''.format("slow_id="+v_flow_id,result['msg']['inst_name'] + '慢日志采集任务', v_flow_id, result['msg']['run_time'], v_cmd_s,v_flow_id)


    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config >> /tmp/config && crontab /tmp/config
              '''.format(v_flow_id)

    v_cron3 = '''crontab -l > /tmp/config && sed -i '/^$/{N;/\\n$/D};' /tmp/config && crontab /tmp/config'''

    v_cron4 = '''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}
              '''.format(result['msg']['script_path'],result['msg']['script_path'],get_time2())

    # Decryption password
    config = db_config()
    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('write_remote_crontab_slow ->v_password=', v_password)

    #connect server
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'], port=int(result['msg']['server_port']),
                username=result['msg']['server_user'], password=v_password)
    #exec v_cron
    if result['msg']['status'] == '0':
       ssh.exec_command(v_cron_)
       ssh.exec_command(v_cron3)

    if result['msg']['status'] == '1':
       if result['msg']['is_rds'] == 'N':
          ssh.exec_command(v_cron0)
          ssh.exec_command(v_cron1)
          ssh.exec_command(v_cron3)
          ssh.exec_command(v_cron4)
       else:
          ssh.exec_command(v_cron2)
          ssh.exec_command(v_cron3)
          ssh.exec_command(v_cron4)

    print('Remote crontab update complete!')
    ssh.close()
    return result

def transfer_remote_file_slow(v_tag):
    print('transfer_remote_file_slow!')
    result = {}
    result['code'] = 200
    result['msg']  = ''
    result = get_slow_config(v_tag)
    print('transfer_remote_file_monitor=',result)
    if result['code']!=200:
       return result

    #Decryption password
    config = db_config()
    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('transfer_remote_file_slow ->v_password=', v_password)

    transport = paramiko.Transport((result['msg']['server_ip'], int(result['msg']['server_port'])))
    transport.connect(username=result['msg']['server_user'], password=v_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    #replace script file
    templete_file = './templete/{0}'.format(result['msg']['script_file'])
    local_file    = './script/{0}'.format(result['msg']['script_file'])
    os.system('cp -f {0} {1}'.format(templete_file, local_file))
    with open(local_file, 'w') as obj_file:
        obj_file.write(get_file_contents(templete_file).
                       replace('$$API_SERVER$$', result['msg']['api_server']))

    #create sync directory
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'], port=int(result['msg']['server_port']),
                username=result['msg']['server_user'], password=v_password)
    ssh.exec_command('mkdir -p {0}'.format(result['msg']['script_path']))
    print("remote sync directory '{0}' created!".format(result['msg']['script_path']))

    #send .py file
    local_file  = './script/{0}'.format(result['msg']['script_file'])
    remote_file = '{0}/{1}'.format(result['msg']['script_path'],result['msg']['script_file'])
    print('transfer_remote_file_monitor'+'$'+local_file+'$'+remote_file)
    sftp.put(localpath=local_file, remotepath=remote_file)
    print('Script:{0} send to {1} ok.'.format(local_file, remote_file))

    #send mysql_transfer.sh file
    templete_file = 'shell/gather_slow.sh'
    local_file    = './script/gather_slow.sh'
    remote_file   = '{0}/gather_slow.sh'.format(result['msg']['script_path'])
    os.system('cp -f {0} {1}'.format(templete_file, local_file))
    print('templete_file=',templete_file)
    print('local_file=',local_file)
    print('remote_file=',remote_file)
    with open(local_file, 'w') as obj_file:
        obj_file.write(get_file_contents(templete_file).
                       replace('$$PYTHON3_HOME$$', result['msg']['python3_home']).
                       replace('$$SCRIPT_PATH$$' , result['msg']['script_path']).
                       replace('$$SCRIPT_FILE$$' , result['msg']['script_file']))
    sftp.put(localpath=local_file, remotepath=remote_file)
    print('Script:{0} send to {1} ok.'.format(local_file, remote_file))
    transport.close()
    ssh.close()
    return result

def run_remote_cmd_slow(v_tag):
    # Init dict
    result = {}
    result['code'] = 200
    result['msg'] = ''
    result = get_slow_config(v_tag)
    if result['code'] != 200:
        return result

    # Decryption password
    config = db_config()
    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('run_remote_cmd_monitor ->v_password=', v_password)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'] ,port=int(result['msg']['server_port']),
                username=result['msg']['server_user'],password=v_password)
    print('run_remote_cmd_archive! connect!')
    remote_file1 = '{0}/{1}'.format(result['msg']['script_path'], result['msg']['script_file'])
    remote_file2 = '{0}/{1}'.format(result['msg']['script_path'], 'gather_slow.sh')
    ssh.exec_command('chmod +x {0}'.format(remote_file1))
    ssh.exec_command('chmod +x {0}'.format(remote_file2))

    # 更新慢查询参数配置
    v_cmd = 'nohup {0}/gather_slow.sh update &>/tmp/gather_slow.log &'.format(result['msg']['script_path'])
    print(v_cmd)
    ssh.exec_command(v_cmd)
    print('run gather_slow.sh update success!')

    v_cmd= 'nohup  {0}/gather_slow.sh cut {1} &>>/tmp/gather_slow.log &'.format(result['msg']['script_path'], v_tag)
    ssh.exec_command(v_cmd)
    print('run gather_slow.sh cut success!')

    stdin, stdout, stderr = ssh.exec_command('crontab -l')
    ret = stdout.read()
    ret = str(ret, encoding='utf-8')
    print('remote crontab query =', ret, type(ret))
    result['msg']['crontab'] = ret

    ssh.close()
    return result