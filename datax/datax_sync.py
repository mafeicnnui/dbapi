#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/1/14 14:46
# @Author : 马飞
# @File : datax_sync.py.py
# @Software: PyCharm

import json
import urllib.parse
import urllib.request
import ssl
import os,happybase
import warnings
import sys

def get_ds_hbase(ip,port):
    conn = happybase.Connection(host=ip,
                                port=int(port),
                                timeout=3600000,
                                autoconnect=True,
                                table_prefix=None,
                                table_prefix_separator=b'_',
                                compat='0.98',
                                transport='buffered',
                                protocol='binary')
    conn.open()
    return conn

def get_hbase_tab_rows(db,tab):
    table = db.table(tab)
    i_counter =0
    for key, data in table.scan():
        i_counter=i_counter+1
        if i_counter>=1:
           break
    return i_counter

def get_config(tag):
    try:
        values = {
            'tag': tag
        }
        print('values=', values)
        url = 'http://10.2.39.18:8290/read_datax_config_sync'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        print('data=', data)
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        print(res, res['code'])
        if res['code'] == 200:
            print('read_datax_config_sync:接口调用成功!')
            print(res['msg'])
            config = res['msg']
            return config
        else:
            print('接口调用失败:'+res['msg'])

    except Exception as e :
        print(e)


def get_templete(id):
    try:
        values = {
            'id': id
        }
        print('values=', values)
        url = 'http://10.2.39.18:8290/read_datax_templete'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        print('data=', data)
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        print(res, res['code'])
        if res['code'] == 200:
            print('read_datax_templete:接口调用成功!')
            print(res['msg'])
            config = res['msg']
            return config
        else:
            print('read_datax_templete:接口调用失败:'+res['msg'])

    except Exception as e :
        print(e)

def get_file_contents(filename):
    file_handle = open(filename, 'r')
    line = file_handle.readline()
    lines = ''
    while line:
        lines = lines + line
        line = file_handle.readline()
    lines = lines + line
    file_handle.close()
    return lines

def main():
    sync_tag=''
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
           sync_tag = sys.argv[p + 1]

    config = get_config(sync_tag)

    thrift_host  = config['hbase_thrift'].split(':')[0]
    thrift_port  = int(config['hbase_thrift'].split(':')[1])
    hbase_table  = config['sync_hbase_table']
    datax_home   = config['datax_home']
    datax_script = config['script_path']
    datax_incr   = config['sync_incr_col']
    sync_id      = config['id']
    db           =  get_ds_hbase(thrift_host,thrift_port)
    rows         =  get_hbase_tab_rows(db,hbase_table)

    v_full_json = '{0}/{1}_full.json'.format(datax_script,sync_tag)
    v_incr_json = '{0}/{1}_incr.json'.format(datax_script,sync_tag)

    v_full_scp  = '{0}/bin/datax.py {1}/{2}'.format(datax_home, datax_script, sync_tag + '_full.json')
    v_incr_scp  = '{0}/bin/datax.py {1}/{2}'.format(datax_home, datax_script, sync_tag + '_incr.json')

    v_templete  = get_templete(sync_id)
    print('full_templete=',v_templete['full'])
    print('incr_templete=',v_templete['incr'])

    #替换模板操作
    with open(v_full_json, 'w') as obj_file:
        obj_file.write(v_templete['full'])

    with open(v_incr_json, 'w') as obj_file:
        obj_file.write(v_templete['incr'])

    #替换^M字符
    os.system('{0}/repstr.sh {1}'.format(datax_script,v_full_json))
    os.system('{0}/repstr.sh {1}'.format(datax_script,v_incr_json))

    print('n_rows=',rows)
    if rows == 0:
        print(v_full_scp)
        os.system(v_full_scp)
    else:
        if datax_incr is not None or datax_incr != '':
            print(v_incr_scp)
            os.system(v_incr_scp)
        else:
            print(v_full_scp)
            os.system(v_full_scp)



if __name__ == "__main__":
     main()
