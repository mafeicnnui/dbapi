#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/23 11:06
# @Author : ma.fei
# @File : instance.py.py
# @Software: PyCharm

import os
import paramiko
import traceback
from utils.common import db_config,aes_decrypt,get_time2,get_file_contents,format_sql

def get_db_inst_config(p_inst_id):
    config=db_config()
    db=config['db_mysql']
    cr=db.cursor()
    result = {}
    result['code'] = 200
    result['msg'] = ''
    cr.execute('''SELECT 
                    b.server_ip,
                    b.server_port,
                    b.server_user,
                    b.server_pass,
                    b.server_desc,   
                    b.market_id,
                    b.server_ip   AS db_ip,
                    a.inst_port   AS db_port,
                    ''            AS db_service,
                    a.mgr_user    AS db_user,
                    a.mgr_pass    AS db_pass,
                    a.inst_type   AS db_type,
                    a.inst_name,
                    a.inst_status,
                    a.python3_home,
                    a.api_server,
                    a.script_path,
                    a.script_file,
                    concat(a.id,'')  as inst_id,
                    a.inst_ver   
                FROM t_db_inst a,t_server b
                 WHERE a.`server_id`=b.id
                   AND a.id={}
            '''.format(p_inst_id))
    rs=cr.fetchone()

    cr.execute('''SELECT TYPE,VALUE,NAME 
                  FROM `t_db_inst_parameter` 
                  WHERE inst_id={}
                    -- and (value not like 'slow_query_log%' and value not like 'long_query_time%')
               '''.format(p_inst_id))
    rs_cfg=cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='1' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_create = cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='2' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_destroy = cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='3' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_start = cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='4' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    rs_step_stop = cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='5' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    step_auostart = cr.fetchall()

    cr.execute('''SELECT id,cmd,message FROM `t_db_inst_step` WHERE flag='6' and version='{}' ORDER BY id'''.format(rs['inst_ver']))
    step_cancel_auostart = cr.fetchall()

    if rs['inst_ver'] == '1':
       cr.execute("""SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.6_download_url'""")
    else:
       cr.execute("""SELECT dmm as mysql_download_url FROM t_dmmx WHERE flag='1' and dm='33' and dmmc='mysql5.7_download_url'""")
    rs_dm = cr.fetchall()

    rs['cfg']           = rs_cfg
    rs['step_create']   = rs_step_create
    rs['step_destroy']  = rs_step_destroy
    rs['step_start']    = rs_step_start
    rs['step_stop']     = rs_step_stop
    rs['step_auostart'] = step_auostart
    rs['step_cancel_auostart'] = step_cancel_auostart

    rs['dpath'] = rs_dm
    cr.close()
    result['msg']=rs
    return result

def save_inst_log(config):
    result = {}
    result['code'] = 200
    result['msg'] = 'success'
    try:
        db=db_config()['db_mysql']
        cr=db.cursor()
        v_sql='''insert into t_db_inst_log(inst_id,type,message,create_date)  values('{0}','{1}','{2}',now())
              '''.format(config['inst_id'],config['type'],format_sql(config['message']))
        cr.execute(v_sql)
        db.commit()
        cr.close()
        return result
    except:
        print(traceback.print_exc())
        result['code'] = -1
        result['msg'] = traceback.print_exc()
        return result

def upd_inst_status(config):
    result = {}
    result['code'] = 200
    result['msg'] = 'success'
    try:
        db = db_config()['db_mysql']
        cr = db.cursor()
        v_sql = "update t_db_inst set inst_status='{}',last_update_date=now() where id={}".format(config['status'],config['inst_id'])
        cr.execute(v_sql)
        db.commit()
        cr.close()
        return result
    except:
        print(traceback.print_exc())
        result['code'] = -1
        result['msg'] = 'failure'
        return result

def upd_inst_reboot_status(config):
        result = {}
        result['code'] = 200
        result['msg'] = 'success'
        try:
            db = db_config()['db_mysql']
            cr = db.cursor()
            v_sql = "update t_db_inst set inst_reboot_flag='{}' where id={}".format(config['reboot_status'],
                                                                                    config['inst_id'])
            cr.execute(v_sql)
            db.commit()
            cr.close()
            return result
        except:
            print(traceback.print_exc())
            result['code'] = -1
            result['msg'] = 'failure'
            return result

def write_remote_crontab_inst(v_inst_id,v_flag):
    result = get_db_inst_config(v_inst_id)
    if result['code']!=200:
       return result

    v_cmd   = '{0}/db_creator.sh status '.format(result['msg']['script_path'])

    v_cron  = '''
               crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config && echo  -e "\n#{1} tag={2}\n{3} {4} &>/dev/null & #tag={5}" >> /tmp/config
              '''.format(v_inst_id,result['msg']['inst_name'],v_inst_id,'*/1 * * * *',v_cmd,v_inst_id)

    v_cron_ = '''
                crontab -l > /tmp/config && sed -i "/{0}/d" /tmp/config >> /tmp/config
              '''.format(v_inst_id)

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
    if v_flag == 'destroy':
       ssh.exec_command(v_cron_)
       ssh.exec_command(v_cron4)

    if v_flag == 'create':
       ssh.exec_command(v_cron)
       ssh.exec_command(v_cron2)
       ssh.exec_command(v_cron3)
       ssh.exec_command(v_cron4)

    print('Remote crontab update complete!')
    ssh.close()
    return result

def transfer_remote_file_inst(v_inst_id):
    result = {}
    result['code'] = 200
    result['msg']  = ''
    result = get_db_inst_config(v_inst_id)
    print('read_db_inst_config=',result)
    if result['code']!=200:
       return result

    #Decryption password
    config = db_config()
    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('transfer_remote_file_inst ->v_password=', v_password)

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
    print('transfer_remote_file_archive'+'$'+local_file+'$'+remote_file)
    sftp.put(localpath=local_file, remotepath=remote_file)
    print('Script:{0} send to {1} ok.'.format(local_file, remote_file))

    #send db_creator.sh file
    templete_file = 'shell/db_creator.sh'
    local_file    = './script/db_creator.sh'
    remote_file   = '{0}/db_creator.sh'.format(result['msg']['script_path'])
    os.system('cp -f {0} {1}'.format(templete_file, local_file))
    print('templete_file=',templete_file)
    print('local_file=',local_file)
    print('remote_file=',remote_file)
    with open(local_file, 'w') as obj_file:
        obj_file.write(get_file_contents(templete_file).
                       replace('$$PYTHON3_HOME$$', result['msg']['python3_home']).
                       replace('$$SCRIPT_PATH$$' , result['msg']['script_path']).
                       replace('$$SCRIPT_FILE$$' , result['msg']['script_file']).
                       replace('$$INST_ID$$'     , result['msg']['inst_id']))
    sftp.put(localpath=local_file, remotepath=remote_file)
    print('Script:{0} send to {1} ok.'.format(local_file, remote_file))
    transport.close()
    ssh.close()
    return result

def run_remote_cmd_inst(v_inst_id):
    # Init dict
    result = {}
    result['code'] = 200
    result['msg'] = ''
    result = get_db_inst_config(v_inst_id)
    if result['code'] != 200:
        return result

    # Decryption password
    config = db_config()

    # delete inst log
    # cr = config['db_mysql'].cursor()
    # cr.execute("delete from t_db_inst_log where inst_id={} and type='create'".format(result['msg']['inst_id']))
    # config['db_mysql'].commit()

    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('run_remote_cmd_inst ->v_password=', v_password)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'] ,port=int(result['msg']['server_port']),
                username=result['msg']['server_user'],password=v_password)
    print('run_remote_cmd_archive! connect!')
    remote_file1 = '{0}/{1}'.format(result['msg']['script_path'], result['msg']['script_file'])
    remote_file2 = '{0}/{1}'.format(result['msg']['script_path'], 'db_creator.sh')
    ssh.exec_command('chmod +x {0}'.format(remote_file1))
    ssh.exec_command('chmod +x {0}'.format(remote_file2))
    print('run_remote_cmd_inst! chmod success!')

    # 启动创建实例任务
    v_cmd = 'nohup {0}/db_creator.sh create &>/tmp/db_create.log &'.format(result['msg']['script_path'])
    print(v_cmd)
    ssh.exec_command(v_cmd)
    print('run_remote_cmd_inst! db_creator.sh success!')
    ssh.close()
    return result

def mgr_remote_cmd_inst(v_inst_id,v_flag):
    # Init dict
    result = {}
    result['code'] = 200
    result['msg'] = ''
    result = get_db_inst_config(v_inst_id)
    if result['code'] != 200:
        return result

    # Decryption password
    config = db_config()

    # delete inst log
    # cr = config['db_mysql'].cursor()
    # cr.execute("delete from t_db_inst_log where inst_id={} and type='create'".format(result['msg']['inst_id']))
    # config['db_mysql'].commit()

    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('run_remote_cmd_inst ->v_password=', v_password)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'] ,port=int(result['msg']['server_port']),
                username=result['msg']['server_user'],password=v_password)
    print('run_remote_cmd_archive! connect!')
    remote_file1 = '{0}/{1}'.format(result['msg']['script_path'], result['msg']['script_file'])
    remote_file2 = '{0}/{1}'.format(result['msg']['script_path'], 'db_creator.sh')
    ssh.exec_command('chmod +x {0}'.format(remote_file1))
    ssh.exec_command('chmod +x {0}'.format(remote_file2))
    print('run_remote_cmd_inst! chmod success!')

    # 管理远程实例（启动，停止，重启）
    v_cmd = 'nohup {0}/db_creator.sh {1} &>/tmp/db_manager.log &'.format(result['msg']['script_path'],v_flag)
    print(v_cmd)
    ssh.exec_command(v_cmd)
    print('run_remote_cmd_inst! db_creator.sh success!')
    ssh.close()
    return result

def destroy_remote_cmd_inst(v_inst_id):
    # Init dict
    result = {}
    result['code'] = 200
    result['msg'] = ''
    result = get_db_inst_config(v_inst_id)
    if result['code'] != 200:
        return result

    # Decryption password
    config = db_config()

    # delete inst log
    cr = config['db_mysql'].cursor()
    cr.execute("delete from t_db_inst_log where inst_id={} and type='destroy'".format(result['msg']['inst_id']))
    config['db_mysql'].commit()

    print('config[db_mysql=', config['db_mysql'])
    print(result['msg']['server_pass'], result['msg']['server_user'])
    v_password = aes_decrypt(config['db_mysql'], result['msg']['server_pass'], result['msg']['server_user'])
    print('run_remote_cmd_inst ->v_password=', v_password)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=result['msg']['server_ip'] ,port=int(result['msg']['server_port']),
                username=result['msg']['server_user'],password=v_password)
    print('run_remote_cmd_archive! connect!')
    remote_file1 = '{0}/{1}'.format(result['msg']['script_path'], result['msg']['script_file'])
    remote_file2 = '{0}/{1}'.format(result['msg']['script_path'], 'db_creator.sh')
    ssh.exec_command('chmod +x {0}'.format(remote_file1))
    ssh.exec_command('chmod +x {0}'.format(remote_file2))
    print('run_remote_cmd_inst! chmod success!')

    # 销毁创建实例任务
    v_cmd = 'nohup {0}/db_creator.sh destroy &>/tmp/db_create.log &'.format(result['msg']['script_path'])
    print(v_cmd)
    ssh.exec_command(v_cmd)
    print('run_remote_cmd_inst! db_creator.sh success!')
    ssh.close()
    return result