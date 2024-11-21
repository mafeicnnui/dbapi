#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/10 8:39
# @Author : 马飞
# @File : mysql_backup.py.py
# @Func : MySQL数据库备份工具
# @Software: PyCharm

import datetime
import json
import os
import sys
import traceback
import warnings
import time
import pymysql
import requests


def get_now():
    return datetime.datetime.now()


def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_stop_time():
    return (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M")


def get_binlog_name():
    return 'mysql-bin-{}-{}' \
        .format((datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y%m%d%H%M%S"),
                (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y%m%d%H%M%S"))


def get_time2(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_date2():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def get_year():
    return datetime.datetime.now().strftime("%Y")


def exception_info():
    e_str = traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]


def get_ds_mysql(ip, port, service, user, password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8',
                           autocommit=True)
    return conn


def get_db_mysql(config):
    v_password = aes_decrypt(config['db_pass'], config['db_user'])
    config['newpass'] = v_password
    return get_ds_mysql(config['db_ip'], config['db_port'], '', config['db_user'], v_password)


def get_seconds(b):
    a = datetime.datetime.now()
    return int((a - b).total_seconds())


def get_seconds(a, b):
    return int((a - b).total_seconds())


def print_dict(config):
    print('-'.ljust(85, '-'))
    print(' '.ljust(3, ' ') + "name".ljust(20, ' ') + 'value')
    print('-'.ljust(85, '-'))
    for key in config:
        print(' '.ljust(3, ' ') + key.ljust(20, ' ') + '=', config[key])
    print('-'.ljust(85, '-'))


def aes_decrypt(p_password, p_key):
    data = {
        'password': p_password,
        'key': p_key
    }
    url = 'http://$$API_SERVER$$/read_db_decrypt'
    res = requests.post(url, data=data).json()

    if res['code'] == 200:
        print('call interface aes_decrypt success!')
        return res['msg']
    else:
        print('call interface aes_decrypt error:{}'.format(res['msg']))
        sys.exit(0)


def read_config(tag):
    data = {
        'tag': tag
    }
    url = 'http://$$API_SERVER$$/read_config_backup'
    res = requests.post(url, data=data).json()

    if res['code'] == 200:
        print('read_config is success!')
        config = res['msg']
        print('config=', config)
        config['year'] = get_year()
        config['day'] = get_date()
        config['bk_path'] = config['bk_base'] + '/' + get_date()
        config['db_mysql'] = get_db_mysql(config)
        return config
    else:
        print('call interface read_config error:{}'.format(res['msg']))
        sys.exit(0)


def write_backup_total(config):
    data = {
        'db_tag': config['db_tag'],
        'create_date': config['create_date'],
        'total_size': config['total_size'],
        'start_time': config['start_time'],
        'end_time': config['end_time'],
        'elaspsed_backup': config['elaspsed_backup'],
        'elaspsed_gzip': config['elaspsed_gzip'],
        'bk_base': config['bk_base'],
        'status': config['status']
    }
    url = 'http://$$API_SERVER$$/write_backup_total'
    res = requests.post(url, data={'tag': json.dumps(data)}).json()

    if res['code'] == 200:
        print('call interface write_backup_total is success!')
    else:
        print('call interface write_backup_total error:{}'.format(res['msg']))


def write_backup_detail(config):
    data = {
        'db_tag': config['db_tag'],
        'db_name': config['db_name'],
        'create_date': config['create_date'],
        'bk_path': config['bk_path'],
        'file_name': config['file_name'],
        'db_size': config['db_size'],
        'start_time': config['start_time'],
        'end_time': config['end_time'],
        'elaspsed_backup': config['elaspsed_backup'],
        'elaspsed_gzip': config['elaspsed_gzip'],
        'status': config['status'],
        'error': config['error']
    }
    url = 'http://$$API_SERVER$$/write_backup_detail'
    res = requests.post(url, data={'tag': json.dumps(data)}).json()

    if res['code'] == 200:
        print('call interface write_backup_detail is success!')
    else:
        print('call interface write_backup_detail error:{}'.format(res['msg']))


def update_backup_status(db_tag, status):
    data = {
        'tag': db_tag,
        'status': status
    }
    url = 'http://$$API_SERVER$$/update_backup_status'
    res = requests.post(url, data=data).json()
    if res['code'] == 200:
        print('call interface update_backup_status {}!'.format('running' if status == '1' else 'complete'))
    else:
        print('call interface update_backup_status error :{}'.format(res['msg']))
        sys.exit(0)


def get_file_contents(filename):
    file_handle = open(filename, 'r')
    line = file_handle.readline()
    lines = ''
    while line:
        lines = lines + line
        line = file_handle.readline()
    lines = lines + line
    file_handle.close()
    return lines[0:-1]


def format_sql(v_sql):
    return v_sql.replace("\\", "\\\\").replace("'", "\\'")


def get_path_size(path):
    r = os.popen('du -sh {0}'.format(path)).read()
    return r.split('\t')[0]


def get_file_size(file):
    try:
        r = os.popen('ls -lh {0}'.format(file)).read()
        return r.split(' ')[4]
    except:
        return 0


def write_log(msg):
    file_name = '/tmp/mysql_backup.log'
    file_handle = open(file_name, 'a+')
    file_handle.write(msg + '\n')
    file_handle.close()


def check_binlog_server(config):
    if config.get('binlog_status') == '0':
        return True
    c = 'ps -ef | grep {} | grep mysqlbinlog | grep -v grep | wc -l'.format(config['db_tag'])
    r = os.popen(c)
    f = r.readlines()
    if int(f[0].replace('\n', '')) == 1:
        return True
    elif config.get('ds') is None:
        return True
    else:
        return False


def db_backup(config):
    print_dict(config)
    db = config['db_mysql']
    cr = db.cursor()
    if config['backup_databases'] is not None:
        if ',' in config['backup_databases']:
            v_sql = '''SELECT schema_name 
                          FROM information_schema.schemata 
                         WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')
                          and instr('{0}',schema_name)>0
                    '''.format(config['backup_databases'])
        else:
            v_sql = '''SELECT schema_name 
                                      FROM information_schema.schemata 
                                     WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')
                                      and instr(schema_name,'{0}')>0
                    '''.format(config['backup_databases'])
    else:
        v_sql = '''SELECT schema_name 
                     FROM information_schema.schemata 
                    WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')                            
                '''
    update_backup_status(config['db_tag'], '1')
    cr.execute(v_sql)
    rs = cr.fetchall()

    # init variables
    bk_begin_time = get_now()
    n_elaspsed_backup_total = 0
    n_elaspsed_gzip_total = 0
    g_status = '0'

    # binlog backup
    if not check_binlog_server(config):
        print('starting mysqlbinlog backup task for {}...'.format(config['db_tag']))
        os.system('mkdir -p {0}/mysqlbinlog'.format(config['bk_base']))
        binlog_name = config['bk_base'] + '/mysqlbinlog/{}_'.format(config['db_tag'])
        err_name = '/tmp/' + config['db_tag'] + '_binlog_' + get_date() + '.err'
        bk_cmd = """{}/mysqlbinlog --no-defaults --read-from-remote-server --host={} --port={} --user={} --password='{}' --raw --stop-never -r {} {} &>{} &
               """.format(os.path.dirname(config['bk_cmd']),
                          config['db_ip'],
                          config['db_port'],
                          config['db_user'],
                          config['newpass'],
                          binlog_name,
                          config['ds']['file'],
                          err_name)
        print(bk_cmd)
        try:
            r = os.system(bk_cmd)
            if r != 0:
                error = format_sql(get_file_contents(err_name).
                                   replace('Warning: Using a password on the command line interface can be insecure.',
                                           ''))
                print('mysqlbinlog error=', error)
        except Exception as e:
            traceback.print_exc()
    elif config.get('binlog_status') == '0':
        print('mysqlbinlog backup is diabled for {}...'.format(config['db_tag']))
    elif config.get('ds') is None:
        print('mysql binlog is closed for {}...'.format(config['db_tag']))
    else:
        print('mysqlbinlog backup task already running for {}...'.format(config['db_tag']))

    os.system('mkdir -p {0}'.format(config['bk_path']))
    for db in list(rs):
        error = ''
        status = '0'
        print('Performing backup database {0}...'.format(db[0]))
        start_time = get_now()
        file_name = config['bk_path'] + '/' + db[0] + '_' + get_date() + '.sql'
        err_name = '/tmp/' + db[0] + '_' + get_date() + '.err'
        full_gzip_name = file_name + '.gz'
        gzip_name = db[0] + '_' + get_date() + '.sql.gz'
        bk_cmd = ''
        if config.get('ds') is not None and not config.get('ro'):
            bk_cmd = '{0} -u{1} -p{2} -h{3} --port {4} --single-transaction --set-gtid-purged=OFF ' \
                     '--routines --force --quick --master-data=2 --databases {5} -r {6} &>{7}'. \
                format(config['bk_cmd'], config['db_user'], config['newpass'],
                       config['db_ip'], config['db_port'], db[0], file_name, err_name)

        if config.get('sv') is not None and config.get('ro'):
            bk_cmd = '{0} -u{1} -p{2} -h{3} --port {4} --single-transaction ' \
                     '--routines --force --quick --dump-slave=2 --databases {5} -r {6} &>{7}'. \
                format(config['bk_cmd'], config['db_user'], config['newpass'],
                       config['db_ip'], config['db_port'], db[0], file_name, err_name)

        if config.get('ds') is None and config.get('sv') is None:
            bk_cmd = '{0} -u{1} -p{2} -h{3} --port {4} --single-transaction ' \
                     '--routines --force --quick --master-data=2 --databases {5} -r {6} &>{7}'. \
                format(config['bk_cmd'], config['db_user'], config['newpass'],
                       config['db_ip'], config['db_port'], db[0], file_name, err_name)

        print(bk_cmd)
        try:
            r = os.system(bk_cmd)
            if r != 0:
                error = format_sql(get_file_contents(err_name).
                                   replace('Warning: Using a password on the command line interface can be insecure.',
                                           ''))
                status = '1'
                os.system('rm {0}'.format(err_name))

            end_time = get_now()
            os.system('cd {0} && gzip -f {1}'.format(config['bk_path'], file_name))
            end_zip_time = get_now()
        except Exception as e:
            traceback.print_exc()
            r = -1
            error = format_sql(str(e))
            status = '1'
            end_time = get_now()
            end_zip_time = get_now()

        config['db_name'] = db[0]
        config['create_date'] = get_date2()
        config['file_name'] = gzip_name
        config['db_size'] = get_file_size(full_gzip_name)
        config['start_time'] = get_time2(start_time)
        config['end_time'] = get_time2(end_zip_time)
        config['elaspsed_backup'] = get_seconds(end_time, start_time)
        config['elaspsed_gzip'] = get_seconds(end_zip_time, end_time)
        config['status'] = status
        config['error'] = error
        if status == '1':
            g_status = '1'
        write_backup_detail(config)
        n_elaspsed_backup_total = n_elaspsed_backup_total + config['elaspsed_backup']
        n_elaspsed_gzip_total = n_elaspsed_gzip_total + config['elaspsed_gzip']
        os.system('rm -f {0}'.format(err_name))

    bk_end_time = get_now()
    config['create_date'] = get_date2()
    config['start_time'] = get_time2(bk_begin_time)
    config['end_time'] = get_time2(bk_end_time)
    config['total_size'] = get_path_size(config['bk_path'])
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip'] = n_elaspsed_gzip_total
    config['status'] = g_status
    write_backup_total(config)
    update_backup_status(config['db_tag'], '0')

    # check oss backup
    if config['oss_status'] == '1':
        if config['oss_cloud'] == '1':
            oss_cmd = 'ossutil64 cp -r -u {}/ {}/{}/'.format(config['bk_path'], config['oss_path'], get_date())
            os.system(oss_cmd)

            # check binlog backup
            if config.get('binlog_status') == '1':
                oss_cmd = 'ossutil64 cp -r -u {}/mysqlbinlog/ {}/mysqlbinlog/'.format(config['bk_base'],
                                                                                      config['oss_path'])
                os.system(oss_cmd)

        if config['oss_cloud'] == '2':
            oss_cmd = '/usr/local/bin/coscmd upload -r -s --skipmd5 {}/ {}/{}/'. \
                format(config['bk_path'], config['oss_path'], get_date())
            os.system(oss_cmd)

            if config.get('binlog_status') == '1':
                oss_cmd = '/usr/local/bin/coscmd upload -r -s --skipmd5 {}/mysqlbinlog/ {}/mysqlbinlog/'. \
                    format(config['bk_base'], config['oss_path'])
                os.system(oss_cmd)

    # delete recent 7 day data
    v_del = '''find {0} -name "*{1}*" -type d -mtime +{2} -exec rm -rf '''.format(config['bk_base'], config['year'],
                                                                                  config['expire']) + '''{} \; -prune'''
    print(v_del)
    os.system(v_del)

    # delete recent 7 day binlog
    v_del = '''find {0} -name "*mysql-bin*" -type f -mtime +{2} -exec rm -rf '''.format(config['bk_base'],
                                                                                        config['year'], config[
                                                                                            'expire']) + '''{} \; -prune'''
    print(v_del)
    os.system(v_del)


def db_backup_mydumper(config):
    print_dict(config)
    db = config['db_mysql']
    cr = db.cursor()
    if config['backup_databases'] is not None:
        if ',' in config['backup_databases']:
            v_sql = '''SELECT schema_name 
                          FROM information_schema.schemata 
                         WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')
                          and instr('{0}',schema_name)>0
                    '''.format(config['backup_databases'])
        else:
            v_sql = '''SELECT schema_name 
                                      FROM information_schema.schemata 
                                     WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')
                                      and instr(schema_name,'{0}')>0
                    '''.format(config['backup_databases'])
    else:
        v_sql = '''SELECT schema_name 
                     FROM information_schema.schemata 
                    WHERE schema_name not IN('information_schema','performance_schema','test','sys','mysql')                            
                '''
    update_backup_status(config['db_tag'], '1')
    cr.execute(v_sql)
    rs = cr.fetchall()

    # init variables
    bk_begin_time = get_now()
    n_elaspsed_backup_total = 0
    n_elaspsed_gzip_total = 0
    g_status = '0'

    # binlog backup
    if not check_binlog_server(config):
        print('starting mysqlbinlog backup task for {}...'.format(config['db_tag']))
        os.system('mkdir -p {0}/mysqlbinlog'.format(config['bk_base']))
        binlog_name = config['bk_base'] + '/mysqlbinlog/{}_'.format(config['db_tag'])
        err_name = '/tmp/' + config['db_tag'] + '_binlog_' + get_date() + '.err'
        bk_cmd = """$MYSQL_HOME/bin/mysqlbinlog --no-defaults --read-from-remote-server --host={} --port={} --user={} --password='{}' --raw --stop-never -r {} {} &>{} &
                   """.format(config['db_ip'],
                              config['db_port'],
                              config['db_user'],
                              config['newpass'],
                              binlog_name,
                              config['ds']['file'],
                              err_name)
        print(bk_cmd)
        try:
            r = os.system(bk_cmd)
            if r != 0:
                error = format_sql(get_file_contents(err_name).
                                   replace('Warning: Using a password on the command line interface can be insecure.',
                                           ''))
                print('mysqlbinlog error=', error)
        except Exception as e:
            traceback.print_exc()
    elif config.get('binlog_status') == '0':
        print('mysqlbinlog backup is diabled for {},skip binlog backup...'.format(config['db_tag']))
    elif config.get('ds') is None:
        print('mysql binlog is closed for {},skip binlog backup...'.format(config['db_tag']))
    else:
        print('mysqlbinlog backup task already running for {}...'.format(config['db_tag']))

    # backup database
    os.system('mkdir -p {0}'.format(config['bk_path']))
    for db in list(rs):
        error = ''
        status = '0'
        print('Performing backup database {0}...'.format(db[0]))
        start_time = get_now()
        dir_name = config['bk_path'] + '/' + db[0]
        os.system('mkdir -p {0}'.format(dir_name))
        err_name = '/tmp/' + db[0] + '_' + get_date() + '.err'
        bk_cmd = "{0} -u {1} -p '{2}' -h {3} -P {4} --trx-consistency-only -R -E -B -k -c -l 30 -K -t 4 -B {5} -o {6} &>{7}" \
            .format(config['bk_cmd'], config['db_user'], config['newpass'],
                    config['db_ip'], config['db_port'], db[0], dir_name, err_name)
        print(bk_cmd)
        try:
            r = os.system(bk_cmd)
            if r != 0:
                error = format_sql(get_file_contents(err_name))
                status = '1'
                os.system('rm {0}'.format(err_name))

            end_time = get_now()
        except Exception as e:
            traceback.print_exc()
            r = -1
            error = format_sql(str(e))
            status = '1'
            end_time = get_now()

        config['db_name'] = db[0]
        config['create_date'] = get_date2()
        config['file_name'] = dir_name
        config['db_size'] = get_path_size(dir_name)
        config['start_time'] = get_time2(start_time)
        config['end_time'] = get_time2(end_time)
        config['elaspsed_backup'] = get_seconds(end_time, start_time)
        config['elaspsed_gzip'] = 0
        config['status'] = status
        config['error'] = error
        if status == '1':
            g_status = '1'
        write_backup_detail(config)
        n_elaspsed_backup_total = n_elaspsed_backup_total + config['elaspsed_backup']
        n_elaspsed_gzip_total = n_elaspsed_gzip_total + config['elaspsed_gzip']
        os.system('rm -f {0}'.format(err_name))

    bk_end_time = get_now()
    config['create_date'] = get_date2()
    config['start_time'] = get_time2(bk_begin_time)
    config['end_time'] = get_time2(bk_end_time)
    config['total_size'] = get_path_size(config['bk_path'])
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip'] = n_elaspsed_gzip_total
    config['status'] = g_status
    write_backup_total(config)
    update_backup_status(config['db_tag'], '0')

    # check oss backup
    if config['oss_status'] == '1':
        if config['oss_cloud'] == '1':
            oss_cmd = 'ossutil64 cp -r -u {}/ {}/{}/'.format(config['bk_path'], config['oss_path'], get_date())
            print(oss_cmd)
            os.system(oss_cmd)
            # check binlog backup
            if config.get('binlog_status') == '1':
                oss_cmd = 'ossutil64 cp -r -u {}/mysqlbinlog/ {}/mysqlbinlog/'.format(config['bk_base'],
                                                                                      config['oss_path'])
                print(oss_cmd)
                os.system(oss_cmd)

        if config['oss_cloud'] == '2':
            oss_cmd = 'coscmd upload -r -s {}/ {}/{}/'.format(config['bk_path'], config['oss_path'], get_date())
            print(oss_cmd)
            os.system(oss_cmd)
            # check binlog backup
            if config.get('binlog_status') == '1':
                oss_cmd = 'coscmd upload -r -s {}/mysqlbinlog/ {}/mysqlbinlog/'.format(config['bk_base'],
                                                                                       config['oss_path'])
                print(oss_cmd)
                os.system(oss_cmd)

    #delete recent 7 day data
    v_del = '''find {0} -name "*{1}*" -type d -mtime +{2} -exec rm -rf '''.format(config['bk_base'], config['year'],
                                                                                  config['expire']) + '''{} \; -prune'''
    print(v_del)
    os.system(v_del)

    # delete recent 7 day binlog
    v_del = '''find {0} -name "*mysql-bin*" -type f -mtime +{2} -exec rm -rf '''.format(config['bk_base'],
                                                                                        config['year'], config[
                                                                                            'expire']) + '''{} \; -prune'''
    print(v_del)
    os.system(v_del)


def main():
    warnings.filterwarnings("ignore")
    tag = ''
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    if tag == '':
        print('Please input tag value!')
        sys.exit(0)

    config = read_config(tag)

    if config['bk_cmd'].find('mysqldump') > 0:
        db_backup(config)

    if config['bk_cmd'].find('mydumper') >= 0:
        db_backup_mydumper(config)


if __name__ == "__main__":
    main()
