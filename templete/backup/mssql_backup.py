#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/9/10 8:39
# @Author : 马飞
# @File : mysql_backup.py.py
# @Func : MySQL数据库备份工具
# @Software: PyCharm

import sys, os
from os.path import join, getsize
import traceback
import warnings
import pymssql
import datetime
import json
import urllib.parse
import urllib.request
import ssl


def get_now():
    return datetime.datetime.now()


def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_time2(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def get_backup_time():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_year():
    return datetime.datetime.now().strftime("%Y")


def exception_info():
    e_str = traceback.format_exc()
    return e_str[e_str.find("pymysql.err."):]


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
    values = {
        'password': p_password,
        'key': p_key
    }
    url = 'http://$$API_SERVER$$/read_db_decrypt'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    if res['code'] == 200:
        print('接口read_db_decrypt 调用成功!')
        config = res['msg']
        return config
    else:
        print('接口read_db_decrypt 调用失败!,{0}'.format(res['msg']))
        sys.exit(0)


def read_config(tag):
    values = {
        'tag': tag
    }
    url = 'http://$$API_SERVER$$/read_config_backup'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    if res['code'] == 200:
        print('接口调用成功!')
        config = res['msg']
        config['year'] = get_year()
        config['day'] = get_date()
        config['bk_path'] = config['bk_base'] + '/' + get_date()
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        sys.exit(0)


def write_backup_total(config):
    v_tag = {
        'db_tag': config['db_tag'],
        'create_date': config['create_date'],
        'total_size': config['total_size'],
        'start_time': config['start_time'],
        'end_time': config['end_time'],
        'elaspsed_backup': config['elaspsed_backup'],
        'elaspsed_gzip': 0,
        'bk_base': config['bk_base'],
        'status': config['status']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('values=', values)
    url = 'http://$$API_SERVER$$/write_backup_total'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        print('接口调用成功!')
    else:
        print('接口调用失败!')


def write_backup_detail(config):
    v_tag = {
        'db_tag': config['db_tag'],
        'db_name': config['db_name'],
        'create_date': config['create_date'],
        'bk_path': config['bk_path'],
        'file_name': config['file_name'],
        'db_size': config['db_size'],
        'start_time': config['start_time'],
        'end_time': config['end_time'],
        'elaspsed_backup': config['elaspsed_backup'],
        'elaspsed_gzip': 0,
        'status': config['status'],
        'error': config['error']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('values=', values)
    url = 'http://$$API_SERVER$$/write_backup_detail'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    if res['code'] == 200:
        print('接口调用成功!')
    else:
        print('接口调用失败!')


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


def format_file_size(filesize):
    try:
        if filesize / 1024 / 1024 / 1024 > 1:
            return str(round(filesize / 1024 / 1024 / 1024, 2)) + 'G'
        elif filesize / 1024 / 1024 > 1:
            return str(round(filesize / 1024 / 1024, 2)) + 'M'
        elif filesize / 1024 > 1:
            return str(round(filesize / 1024, 2)) + 'K'
    except:
        return 0


def get_file_size(file):
    try:
        return getsize(file)
    except:
        return 0


def get_filename(db, config):
    return '{}\{}_backup_{}'.format(config['bk_base'], db, get_backup_time())


def gen_sql_file(db, config):
    file_name = get_filename(db, config)
    with open(config['script_path'] + '\\db_backup.sql', 'w') as f:
        sql = '''
BACKUP DATABASE [{}] TO  DISK = N'{}.bak' WITH CHECKSUM,COMPRESSION,BUFFERCOUNT = 50, MAXTRANSFERSIZE = 4194304,NAME = N'{}', SKIP, REWIND, NOUNLOAD,  STATS = 10 
'''.format(db, file_name, file_name)
        f.write(sql)


def get_filename_linux(db, config):
    return '{}/{}_backup_{}'.format(config['bk_base'], db, get_backup_time())


def gen_sql_file_linux(db, config):
    file_name = get_filename_linux(db, config)
    with open(config['script_path'] + '/db_backup.sql', 'w') as f:
        sql = '''
BACKUP DATABASE [{}] TO  DISK = N'{}.bak' WITH CHECKSUM,COMPRESSION,BUFFERCOUNT = 50, MAXTRANSFERSIZE = 4194304,NAME = N'{}', SKIP, REWIND, NOUNLOAD,  STATS = 10 
'''.format(db, file_name, file_name)
        f.write(sql)


def db_backup_windows(config):
    print_dict(config)
    bk_begin_time = get_now()
    n_elaspsed_backup_total = 0
    n_total_size = 0
    g_status = '0'
    for db in config['backup_databases'].split(','):
        status = '0'
        print('Performing backup database {0}...'.format(db))
        start_time = get_now()
        file_name = get_filename(db, config)
        config['newpass'] = aes_decrypt(config['db_pass'], config['db_user'])
        gen_sql_file(db, config)
        bk_cmd = '''
{} -S {} -U {} -P {} -d {} -i {}\\db_backup.sql -o {}.log
'''.format(config['bk_cmd'],
           config['db_ip'],
           config['db_user'],
           config['newpass'],
           db,
           config['script_path'],
           file_name,
           )

        print(bk_cmd)
        try:
            os.system(bk_cmd)
        except:
            status = '1'

        end_time = get_now()
        file_size = get_file_size(file_name + '.bak')
        config['db_name'] = db
        config['create_date'] = get_date()
        config['file_name'] = file_name + '.bak'
        config['db_size'] = format_file_size(file_size)
        config['start_time'] = get_time2(start_time)
        config['end_time'] = get_time2(end_time)
        config['elaspsed_backup'] = get_seconds(end_time, start_time)
        config['status'] = status
        config['error'] = 'success' if status == '1' else 'failure'

        if status == '1':
            g_status = '1'
        write_backup_detail(config)
        n_elaspsed_backup_total = n_elaspsed_backup_total + config['elaspsed_backup']
        n_total_size = n_total_size + config['elaspsed_backup'] + file_size

    bk_end_time = get_now()
    config['create_date'] = get_date()
    config['start_time'] = get_time2(bk_begin_time)
    config['end_time'] = get_time2(bk_end_time)
    config['total_size'] = format_file_size(n_total_size)
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip'] = 0
    config['status'] = g_status
    write_backup_total(config)

    print('delete recent {} day backup...'.format(config['expire']))
    v_del = '''forfiles /p "{}" /s /m *.* /d -{} /c "cmd /c del @path"'''.format(config['bk_base'], config['expire'])
    print(v_del)
    os.system(v_del)

    print('remote backup...')
    v_remote = """D:\\cwRsync\\rsync.bat '{}'""".format(datetime.datetime.now().strftime("%Y%m"))
    print(v_remote)
    os.system(v_remote)


def db_backup_linux(config):
    print_dict(config)
    bk_begin_time = get_now()
    n_elaspsed_backup_total = 0
    n_total_size = 0
    g_status = '0'
    for db in config['backup_databases'].split(','):
        status = '0'
        print('Performing backup database {0}...'.format(db))
        start_time = get_now()
        file_name = get_filename_linux(db, config)
        config['newpass'] = aes_decrypt(config['db_pass'], config['db_user'])
        gen_sql_file_linux(db, config)
        bk_cmd = '''
{} -S {} -U {} -P {} -d {} -i {}/db_backup.sql -o {}.log
'''.format(config['bk_cmd'],
           config['db_ip'],
           config['db_user'],
           config['newpass'],
           db,
           config['script_path'],
           file_name,
           )

        print(bk_cmd)
        try:
            os.system(bk_cmd)
        except:
            status = '1'

        end_time = get_now()
        file_size = get_file_size(file_name + '.bak')
        config['db_name'] = db
        config['create_date'] = get_date()
        config['file_name'] = file_name + '.bak'
        config['db_size'] = format_file_size(file_size)
        config['start_time'] = get_time2(start_time)
        config['end_time'] = get_time2(end_time)
        config['elaspsed_backup'] = get_seconds(end_time, start_time)
        config['status'] = status
        config['error'] = 'success' if status == '1' else 'failure'

        if status == '1':
            g_status = '1'
        write_backup_detail(config)
        n_elaspsed_backup_total = n_elaspsed_backup_total + config['elaspsed_backup']
        n_total_size = n_total_size + config['elaspsed_backup'] + file_size

    bk_end_time = get_now()
    config['create_date'] = get_date()
    config['start_time'] = get_time2(bk_begin_time)
    config['end_time'] = get_time2(bk_end_time)
    config['total_size'] = format_file_size(n_total_size)
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip'] = 0
    config['status'] = g_status
    write_backup_total(config)

    print('delete recent {} day backup...'.format(config['expire']))
    v_del = '''find {0} -name "*{1}*" -type d -mtime +{2} -exec rm -rf '''.format(config['bk_base'], config['year'],
                                                                                  config['expire']) + '''{} \; -prune'''
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
    if config['server_os'] == 'CentOS':
        db_backup_linux(config)
    else:
        db_backup_windows(config)


if __name__ == "__main__":
    main()
