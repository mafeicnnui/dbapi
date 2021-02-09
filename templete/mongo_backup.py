#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2019/1/30 9:31
# @Author : 马飞
# @File : sync_mysql2mongo.py
# @Software: PyCharm
import sys,time
import configparser
import warnings
import pymongo
import os
import datetime
import json
import urllib.parse
import urllib.request
import ssl

def get_now():
    return datetime.datetime.now()

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_date():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_time2(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")

def get_year():
    return datetime.datetime.now().strftime("%Y")

def get_ds_mongo(ip,port,replset):
    conn = pymongo.MongoClient(host=ip, port=int(port),replicaSet=replset)
    return conn

def get_ds_mongo(ip,port):
    conn = pymongo.MongoClient(host=ip, port=int(port))
    return conn

def get_config(fname):
    config = {}
    cfg=configparser.ConfigParser()
    cfg.read(fname,encoding="utf-8-sig")
    #get mail parameter
    config['send_user']                = cfg.get("sync", "send_mail_user")
    config['send_pass']                = cfg.get("sync", "send_mail_pass")
    config['acpt_user']                = cfg.get("sync", "acpt_mail_user")
    config['mail_title']               = cfg.get("sync", "mail_title")
    #get mongodb parameter
    db_mongo                           = cfg.get("sync", "db_mongo")
    db_mongo_ip                        = db_mongo.split(':')[0]
    db_mongo_port                      = db_mongo.split(':')[1]
    db_mongo_replset                   = db_mongo.split(':')[2]
    config['db_mongo_ip']              = db_mongo_ip
    config['db_mongo_port']            = db_mongo_port
    config['db_mongo_replset']         = db_mongo_replset
    #config['db_mongo']                 = get_ds_mongo(db_mongo_ip, db_mongo_port,db_mongo_replset)
    config['db_mongo']                 = get_ds_mongo(db_mongo_ip, db_mongo_port)
    config['mongodump']                = cfg.get("sync", "mongodump")
    config['backup_path']              = cfg.get("sync", "backup_path")
    return config

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_seconds(a,b):
    return int((a-b).total_seconds())

def print_dict(config):
    print('-'.ljust(125,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(125,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(125,'-'))

def init(config,debug):
    #config = get_config(config)
    config = read_config(config)
    #print dict
    if debug:
       print_dict(config)
    return config

def read_config(tag):
    values = {
        'tag': tag
    }
    #print('values=', values)
    url = 'http://$$API_SERVER$$/read_config_backup'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    #write_log(res+','+str(res['code']))
    if res['code'] == 200:
        print('接口调用成功!')
        config=res['msg']
        config['year'] = get_year()
        config['day']  = get_date()
        config['bk_path']=config['bk_base']+'/'+get_date()
        config['db_mongo'] = get_ds_mongo(config['db_ip'],config['db_port'])
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        sys.exit(0)

def write_backup_total(config):
    v_tag = {
        'db_tag'          : config['db_tag'],
        'create_date'     : config['create_date'],
        'total_size'      : config['total_size'],
        'start_time'      : config['start_time'],
        'end_time'        : config['end_time'],
        'elaspsed_backup' : config['elaspsed_backup'],
        'elaspsed_gzip'   : config['elaspsed_gzip'],
        'bk_base'         : config['bk_base'],
        'status'          : config['status']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('values=',values)
    url = 'http://$$API_SERVER$$/write_backup_total'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        write_log('接口调用成功!')
    else:
        write_log('接口调用失败!')

def write_backup_detail(config):
    v_tag = {
        'db_tag'          : config['db_tag'],
        'db_name'         : config['db_name'],
        'create_date'     : config['create_date'],
        'bk_path'         : config['bk_path'],
        'file_name'       : config['file_name'],
        'db_size'         : config['db_size'],
        'start_time'      : config['start_time'],
        'end_time'        : config['end_time'],
        'elaspsed_backup' : config['elaspsed_backup'],
        'elaspsed_gzip'   : config['elaspsed_gzip'],
        'status'          : config['status'],
        'error'           : config['error']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    print('values=',values)
    url = 'http://$$API_SERVER$$/write_backup_detail'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    #write_log(res+','+str(res['code']))
    if res['code'] == 200:
        write_log('接口调用成功!')
    else:
        write_log('接口调用失败!')

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

def get_path_size(path):
    r = os.popen('du -sh {0}'.format(path)).read()
    return r.split('\t')[0]

def get_file_size(file):
    r = os.popen('ls -lh {0}'.format(file)).read()
    return r.split(' ')[4]

def write_log(msg):
    file_name   = '/tmp/mysql_backup.log'
    file_handle = open(file_name, 'a+')
    file_handle.write(msg + '\n')
    file_handle.close()

def db_backup(config):
    db_mongodb              = config['db_mongo']
    db_list                 = db_mongodb.list_database_names()
    bk_begin_time           = get_now()
    n_elaspsed_backup_total = 0
    n_elaspsed_gzip_total   = 0
    g_status                = '0'

    for db in db_list:
        error  = ''
        status = '0'
        os.system('mkdir -p {0}'.format(config['bk_path']))

        if db not in ('admin', 'local', 'push'):
            if  config['backup_databases'] is not None and config['backup_databases']!='':
                if ',' in config['backup_databases'] and config['backup_databases'].find(db)>=0:
                    print('backup database {0}...'.format(db))
                    start_time = get_now()
                    file_name  = db + '.tar.gz'
                    full_name  = config['bk_path'] + '/' + file_name
                    err_name   = '/tmp/' + db + '_' + get_date() + '.err'
                    cmd = "{0} -h {1}:{2} -d {3} -o {4} &>{5}". \
                        format(config['bk_cmd'], config['db_ip'], config['db_port'], db, config['bk_path'], err_name)
                    print(cmd)
                    result = os.system(cmd)
                    if result != 0:
                        error = get_file_contents(err_name)
                        status = '1'
                        os.system('rm {0}'.format(err_name))

                    end_time = get_now()
                    os.system('cd {0} && tar czf {1} {2}'.format(config['bk_path'], file_name, db))
                    os.system('rm -rf {0}'.format(config['bk_path'] + '/' + db))
                    filesize = os.path.getsize(full_name)
                    end_zip_time = get_now()
                    print(file_name, full_name, filesize)

                    #write backup detail
                    config['db_name'] = db
                    config['create_date'] = get_date()
                    config['file_name'] = file_name
                    config['db_size'] = get_file_size(full_name)
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

                if ',' in config['backup_databases'] and db.find(config['backup_databases'])>=0:
                    print('backup database {0}...'.format(db))
                    start_time = get_now()
                    file_name  = db + '.tar.gz'
                    full_name  = config['bk_path'] + '/' + file_name
                    err_name   = '/tmp/' + db + '_' + get_date() + '.err'
                    cmd = "{0} -h {1}:{2} -d {3} -o {4} &>{5}". \
                        format(config['bk_cmd'], config['db_ip'], config['db_port'], db, config['bk_path'], err_name)
                    print(cmd)

                    result = os.system(cmd)
                    if result != 0:
                        error  = get_file_contents(err_name)
                        status = '1'
                        os.system('rm {0}'.format(err_name))

                    end_time = get_now()
                    os.system('cd {0} && tar czf {1} {2}'.format(config['bk_path'], file_name, db))
                    os.system('rm -rf {0}'.format(config['bk_path'] + '/' + db))
                    filesize = os.path.getsize(full_name)
                    end_zip_time = get_now()
                    print(file_name, full_name, filesize)

                    #write backup detail
                    config['db_name'] = db
                    config['create_date'] = get_date()
                    config['file_name'] = file_name
                    config['db_size'] = get_file_size(full_name)
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

            else:
                print('backup database {0}...'.format(db))
                start_time = get_now()
                file_name = db + '.tar.gz'
                full_name = config['bk_path'] + '/' + file_name
                err_name = '/tmp/' + db + '_' + get_date() + '.err'
                cmd = "{0} -h {1}:{2} -d {3} -o {4} &>{5}". \
                    format(config['bk_cmd'], config['db_ip'], config['db_port'], db, config['bk_path'], err_name)
                print(cmd)

                result = os.system(cmd)
                if result != 0:
                    error = get_file_contents(err_name)
                    status = '1'
                    os.system('rm {0}'.format(err_name))

                end_time = get_now()
                os.system('cd {0} && tar czf {1} {2}'.format(config['bk_path'], file_name, db))
                os.system('rm -rf {0}'.format(config['bk_path'] + '/' + db))
                filesize = os.path.getsize(full_name)
                end_zip_time = get_now()
                print(file_name, full_name, filesize)

                config['db_name']     = db
                config['create_date'] = get_date()
                config['file_name']   = file_name
                config['db_size']     = get_file_size(full_name)
                config['start_time']  = get_time2(start_time)
                config['end_time']    = get_time2(end_zip_time)
                config['elaspsed_backup'] = get_seconds(end_time, start_time)
                config['elaspsed_gzip']   = get_seconds(end_zip_time, end_time)
                config['status'] = status
                config['error']  = error

                if status == '1':
                    g_status = '1'
                write_backup_detail(config)
                n_elaspsed_backup_total = n_elaspsed_backup_total + config['elaspsed_backup']
                n_elaspsed_gzip_total = n_elaspsed_gzip_total + config['elaspsed_gzip']
                os.system('rm -f {0}'.format(err_name))

    #write backup total
    bk_end_time = get_now()
    config['create_date'] = get_date()
    config['start_time']  = get_time2(bk_begin_time)
    config['end_time']    = get_time2(bk_end_time)
    config['total_size']  = get_path_size(config['bk_path'])
    config['elaspsed_backup'] = n_elaspsed_backup_total
    config['elaspsed_gzip']   = n_elaspsed_gzip_total
    config['status'] = g_status
    write_backup_total(config)

    #delete recent 7 day data
    v_del = '''find {0} -name "*{1}*" -type d -mtime +{2} -exec rm -rf ''' \
                .format(config['bk_base'], config['year'], config['expire']) + '''{} \;'''
    print(v_del)
    os.system(v_del)


def main():
    #init variable
    config = ""
    debug = False
    warnings.filterwarnings("ignore")
    # get parameter from console
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        elif sys.argv[p] == "-debug":
            debug = True

    #初始化
    config = init(config, debug)

    #备份
    db_backup(config)


if __name__ == "__main__":
     main()

