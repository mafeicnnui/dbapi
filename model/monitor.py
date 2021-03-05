#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:05
# @Author : ma.fei
# @File : monitor.py.py
# @Software: PyCharm

import os
import paramiko
import traceback

from utils.common import db_config,get_itmes_from_templete_ids,aes_decrypt,get_file_contents,get_time2

def get_db_monitor_config(p_tag):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    result = {}
    result['code'] = 200
    result['msg'] = ''

    #检测传输服务器是否有效
    if check_server_monitor_status(p_tag)>0:
       result['code'] = -1
       result['msg'] = '采集服务器已禁用!'
       return result

    #检测同步标识是否存在
    if check_db_monitor_config(p_tag)==0:
       result['code'] = -1
       result['msg'] = '监控标识不存在!'
       return result

    cr.execute('''SELECT  a.task_tag,
                        a.comments,
                        a.templete_id,
                        a.server_id,
                        a.db_id,
                        a.run_time,
                        a.python3_home,
                        a.api_server,
                        a.script_path,
                        a.script_file,
                        a.status,
                        b.server_ip,
                        b.server_port,
                        b.server_user,
                        b.server_pass,
                        b.server_desc,   
                        b.market_id,
                        c.ip        AS db_ip,
                        c.port      AS db_port,
                        c.service   AS db_service,
                        c.user      AS db_user,
                        c.password  AS db_pass,
                        c.db_type   AS db_type              
                FROM t_monitor_task a 
                   JOIN t_server b ON a.server_id=b.id 
                   LEFT JOIN t_db_source c  ON  a.db_id=c.id  
                where a.task_tag ='{0}' 
                ORDER BY a.id
            '''.format(p_tag))

    rs=cr.fetchone()
    cr.close()
    rs['templete_indexes'] = get_itmes_from_templete_ids(rs['templete_id'])
    result['msg']=rs
    return result

def check_db_monitor_config(p_tag):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    cr.execute('''select count(0) from t_monitor_task where task_tag='{0}'
               '''.format(p_tag))
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

def check_server_monitor_status(p_tag):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    cr.execute('''select count(0) from t_monitor_task a,t_server b 
                  where a.server_id=b.id and a.task_tag='{0}' and b.status='0'
               '''.format(p_tag))
    rs=cr.fetchone()
    cr.close()
    return  rs['count(0)']

def save_monitor_log(config):
    result = {}
    result['code'] = 200
    result['msg'] = 'success'
    db = db_config()['db_mysql']
    cr = db.cursor()
    v_sql = ''
    if config['db_id']!='':
        v_sql = '''insert into t_monitor_task_db_log 
                   (task_tag,server_id,db_id,total_connect,active_connect,db_available,db_tbs_usage,db_qps,db_tps,create_date) 
                      values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}',now())
                '''.format(config.get('task_tag',''), config.get('server_id',''),config.get('db_id',''),
                           config.get('total_connect',''),config.get('active_connect',''),config.get('db_available',''),
                           config.get('db_tbs_usage',''),config.get('db_qps',''),config.get('db_tps',''))
    else:
        v_sql = '''insert into t_monitor_task_server_log
                      (task_tag,server_id,cpu_total_usage,cpu_core_usage,mem_usage,disk_usage,disk_read,disk_write,net_in,net_out,market_id,create_date) 
                        values('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',now())
                '''.format(config.get('task_tag',''), config.get('server_id',''),
                           config.get('cpu_total_usage',''), config.get('cpu_core_usage',''), config.get('mem_usage',''),
                           config.get('disk_usage',''), config.get('disk_read',''), config.get('disk_write',''),
                           config.get('net_in',''), config.get('net_out',''), config.get('market_id',''))
    print('save_monitor_log=', v_sql)
    cr.execute(v_sql)
    db.commit()
    cr.close()
    return result

def write_remote_crontab_monitor(v_tag):
    result = get_db_monitor_config(v_tag)
    if result['code']!=200:
       return result

    v_cmd   = '{0}/db_monitor.sh {1} {2}'.format(result['msg']['script_path'],result['msg']['script_file'], v_tag)

    v_cron  = '''
               crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag,result['msg']['comments'],v_tag,result['msg']['run_time'],v_cmd)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n#{3} {4} &>/dev/null &" >> /tmp/config
              '''.format(v_tag, result['msg']['comments'], v_tag, result['msg']['run_time'], v_cmd)

    v_cron2 ='''sed -i '/^$/{N;/\\n$/D};' /tmp/config'''

    v_cron3 ='''mkdir -p {}/crontab && crontab -l >{}/crontab/crontab.{}'''.\
             format(result['msg']['script_path'],result['msg']['script_path'],get_time2())

    v_cron4 ='''crontab /tmp/config'''

    # Decryption password
    config = db_config()
    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('write_remote_crontab_sync ->v_password=', v_password)

    #connect server
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'], port=int(result['msg']['server_port']),
                username=result['msg']['server_user'], password=v_password)
    #exec v_cron
    if result['msg']['status']=='1':
       ssh.exec_command(v_cron)
    else:
       ssh.exec_command(v_cron_)

    ssh.exec_command(v_cron2)
    ssh.exec_command(v_cron3)
    ssh.exec_command(v_cron4)
    print('Remote crontab update complete!')
    ssh.close()
    return result

def transfer_remote_file_monitor(v_tag):
    print('transfer_remote_file_monitor!')
    result = {}
    result['code'] = 200
    result['msg']  = ''
    result = get_db_monitor_config(v_tag)
    print('transfer_remote_file_monitor=',result)
    if result['code']!=200:
       return result

    #Decryption password
    config = db_config()
    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('transfer_remote_file_monitor ->v_password=', v_password)

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
    templete_file = 'shell/db_monitor.sh'
    local_file    = './script/db_monitor.sh'
    remote_file   = '{0}/db_monitor.sh'.format(result['msg']['script_path'])
    os.system('cp -f {0} {1}'.format(templete_file, local_file))
    print('templete_file=',templete_file)
    print('local_file=',local_file)
    print('remote_file=',remote_file)
    with open(local_file, 'w') as obj_file:
        obj_file.write(get_file_contents(templete_file).
                       replace('$$PYTHON3_HOME$$', result['msg']['python3_home']).
                       replace('$$SCRIPT_PATH$$' , result['msg']['script_path']))
    sftp.put(localpath=local_file, remotepath=remote_file)
    print('Script:{0} send to {1} ok.'.format(local_file, remote_file))
    transport.close()
    ssh.close()
    return result

def run_remote_cmd_monitor(v_tag):
    # Init dict
    result = {}
    result['code'] = 200
    result['msg'] = ''
    result = get_db_monitor_config(v_tag)
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
    remote_file2 = '{0}/{1}'.format(result['msg']['script_path'], 'db_monitor.sh')
    ssh.exec_command('chmod +x {0}'.format(remote_file1))
    ssh.exec_command('chmod +x {0}'.format(remote_file2))
    print('run_remote_cmd_monitor! exec_command!')

    stdin, stdout, stderr = ssh.exec_command('crontab -l')
    ret = stdout.read()
    ret = str(ret, encoding='utf-8')
    print('remote crontab query =', ret, type(ret))
    result['msg']['crontab'] = ret

    ssh.close()
    return result