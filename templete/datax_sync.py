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
import datetime
import smtplib
from email.mime.text import MIMEText
import traceback
import pymysql

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

def send_mail25(p_from_user,p_from_pass,p_to_user,p_title,p_content):
    to_user=p_to_user.split(",")
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        print(e)

def exception_interface(v_title,v_content):
    v_templete = '''
           <html>
              <head>
                 <style type="text/css">
                     .xwtable {width: 100%;border-collapse: collapse;border: 1px solid #ccc;}
                     .xwtable thead td {font-size: 12px;color: #333333;
                                        text-align: center;background: url(table_top.jpg) repeat-x top center;
                                        border: 1px solid #ccc; font-weight:bold;}
                     .xwtable thead th {font-size: 12px;color: #333333;
                                        text-align: center;background: url(table_top.jpg) repeat-x top center;
                                        border: 1px solid #ccc; font-weight:bold;}
                     .xwtable tbody tr {background: #fff;font-size: 12px;color: #666666;}
                     .xwtable tbody tr.alt-row {background: #f2f7fc;}
                     .xwtable td{line-height:20px;text-align: left;padding:4px 10px 3px 10px;height: 18px;border: 1px solid #ccc;}
                 </style>
              </head>
              <body>             
                 $$TABLE$$           
              </body>
           </html>
          '''
    v_templete = v_templete.replace('$$TABLE$$',v_content)
    send_mail25('190343@lifeat.cn','Hhc5HBtAuYTPGHQ8','190343@lifeat.cn', v_title,v_templete)

def aes_decrypt(p_password,p_key):
    values = {
        'password': p_password,
        'key':p_key
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


def get_ds_mysql(ip,port,service ,user,password):
    conn = pymysql.connect(host=ip, port=int(port), user=user, passwd=password, db=service, charset='utf8')
    return conn


def get_config(tag):
    try:
        values = {
            'tag': tag
        }
        print('values=', values)
        url = 'http://$$API_SERVER$$/read_datax_config_sync'
        context = ssl._create_unverified_context()
        data    = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        print('data=', data)
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        print(res, res['code'])
        if res['code'] == 200:
            print('read_datax_config_sync:接口调用成功!')
            print(res['msg'])
            config                          = res['msg']
            config['db_mysql_sour_ip']      = config['sync_db_sour'].split(':')[0]
            config['db_mysql_sour_port']    = config['sync_db_sour'].split(':')[1]
            config['db_mysql_sour_service'] = config['sync_db_sour'].split(':')[2]
            config['db_mysql_sour_user']    = config['sync_db_sour'].split(':')[3]
            config['db_mysql_sour_pass']    = aes_decrypt(config['sync_db_sour'].split(':')[4], config['db_mysql_sour_user'])
            config['db_mysql_sour_string']  = config['db_mysql_sour_ip']  + ':' + config['db_mysql_sour_port'] + '/' + config['db_mysql_sour_service']
            config['db_mysql_sour']         = get_ds_mysql(config['db_mysql_sour_ip'],
                                                           config['db_mysql_sour_port'],
                                                           config['db_mysql_sour_service'],
                                                           config['db_mysql_sour_user'],
                                                           config['db_mysql_sour_pass'])

            return config
        else:
            print('dataX接口调用失败!,{0}'.format(res['msg']))  # 发异常邮件
            v_title = 'dataX数据同步接口异常[★]'
            v_content = '''<table class='xwtable'>
                           <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                           <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                           <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr>            
                       </table>'''
            v_content = v_content.replace('$$interface$$', url)
            v_content = v_content.replace('$$parameter$$', json.dumps(values))
            v_content = v_content.replace('$$error$$', res['msg'])
            if res['code'] != -3:
                exception_interface(v_title, v_content)
                sys.exit(0)
            else:
                print(res['msg'])
                sys.exit(0)
    except Exception as e :
        v_title = 'dataX数据同步接口异常[★★]'
        v_content = '''<table class='xwtable'>
                         <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                         <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                         <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr> 
                     </table>'''
        v_content = v_content.replace('$$interface$$', url)
        v_content = v_content.replace('$$parameter$$', json.dumps(values))
        v_content = v_content.replace('$$error$$', traceback.format_exc())
        exception_interface(v_title, v_content)
        print(traceback.format_exc())
        sys.exit(0)

def get_templete(id):
    try:
        values = {
            'id': id
        }
        print('values=', values)
        url = 'http://$$API_SERVER$$/read_datax_templete'
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
            v_title = 'dataX数据同步接口异常[★]'
            v_content = '''<table class='xwtable'>
                                       <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                                       <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                                       <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr>            
                                   </table>'''
            v_content = v_content.replace('$$interface$$', url)
            v_content = v_content.replace('$$parameter$$', json.dumps(values))
            v_content = v_content.replace('$$error$$', res['msg'])
            if res['code'] != -3:
                exception_interface(v_title, v_content)
                sys.exit(0)
            else:
                print(res['msg'])
                sys.exit(0)

    except Exception as e :
        v_title = 'dataX数据同步接口异常[★★]'
        v_content = '''<table class='xwtable'>
                                <tr><td  width="30%">接口地址</td><td  width="70%">$$interface$$</td></tr>
                                <tr><td  width="30%">接口参数</td><td  width="70%">$$parameter$$</td></tr>
                                <tr><td  width="30%">错误信息</td><td  width="70%">$$error$$</td></tr> 
                            </table>'''
        v_content = v_content.replace('$$interface$$', url)
        v_content = v_content.replace('$$parameter$$', json.dumps(values))
        v_content = v_content.replace('$$error$$', traceback.format_exc())
        exception_interface(v_title, v_content)
        print(traceback.format_exc())
        sys.exit(0)

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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

def write_datax_sync_log(config):
    v_tag = {
        'sync_tag'        : config['sync_tag'],
        'create_date'     : get_time(),
        'table_name'      : config['table_name'],
        'duration'        : config['sync_duration'],
        'amount'          : config['sync_amount']
    }
    v_msg = json.dumps(v_tag)
    values = {
        'tag': v_msg
    }
    url = 'http://$$API_SERVER$$/write_datax_sync_log'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res)
    print(res['code'])
    if res['code'] == 200:
        print('Interface write_datax_sync_log call successful!')
    else:
        print('Interface write_datax_sync_log call failed!')


def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())


def get_sync_table_rows(config,hbase_rows):
    db     = config['db_mysql_sour']
    cr     = db.cursor()
    tab    = config['sync_table']
    where  = config['sync_incr_where']
    sql    = ''
    if where  is None or where =='':
       sql  = "select count(0) from {0}".format(tab)
    else:
       if hbase_rows== 0 :
          sql = "select count(0) from {0}".format(tab)
       else:
          sql = "select count(0) from {0} where {1}".format(tab, where)
    cr.execute(sql)
    rs=cr.fetchone()
    cr.close()
    print('get_sync_table_rows=', sql,rs[0])
    return  rs[0]


def main():
    sync_tag=''
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
           sync_tag = sys.argv[p + 1]

    config = get_config(sync_tag)
    print_dict(config)

    print('config=',config)
    thrift_host  = config['hbase_thrift'].split(':')[0]
    thrift_port  = int(config['hbase_thrift'].split(':')[1])
    hbase_table  = config['sync_hbase_table']
    datax_home   = config['datax_home']
    datax_script = config['script_path']
    datax_incr   = config['sync_incr_col']
    sync_id      = config['id']
    db           =  get_ds_hbase(thrift_host,thrift_port)
    hbase_rows   =  get_hbase_tab_rows(db,hbase_table)

    v_full_json  = '{0}/{1}_full.json'.format(datax_script,sync_tag)
    v_incr_json  = '{0}/{1}_incr.json'.format(datax_script,sync_tag)

    v_full_scp   = '{0}/bin/datax.py {1}/{2}'.format(datax_home, datax_script, sync_tag + '_full.json')
    v_incr_scp   = '{0}/bin/datax.py {1}/{2}'.format(datax_home, datax_script, sync_tag + '_incr.json')

    v_templete   = get_templete(sync_id)
    start_time   = datetime.datetime.now()

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

    if hbase_rows == 0:
        print(v_full_scp)
        os.system(v_full_scp)
    else:
        if datax_incr is not None or datax_incr != '':
            print(v_incr_scp)
            os.system(v_incr_scp)
        else:
            print(v_full_scp)
            os.system(v_full_scp)

    config['table_name']    = config['sync_table']
    config['sync_duration'] = str(get_seconds(start_time))
    config['sync_amount']   = str(get_sync_table_rows(config,hbase_rows))
    write_datax_sync_log(config)
    print('hbase_table=', hbase_table)
    print('hbase_rows=', hbase_rows)


if __name__ == "__main__":
     main()

