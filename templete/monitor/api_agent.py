import os
import sys
import json
import psutil
import pymysql
import traceback
import requests
import datetime
import warnings
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent,)

SEND_USER='190343'
ALERT_TITLE = '停简单接口告警'
ALERT_MESSAGE = '''项目编码：{}
项目名称：{}
采集主机：{}
接口地址：{}
失败时间：{}
失败时长：{}
告警时间：{}
响应编码：{}
响应内容：{}
'''

RECOVER_TITLE = "停简单接口恢复通知"
RECOVER_MESSAGE = '''项目编码：{}
项目名称：{}
采集主机：{}
接口地址：{}
响应编码：{}
响应内容：{}
恢复时间：{}'''

MYSQL_SETTINGS = {
    "host": '10.2.39.18',
    "port": 3306,
    "user": "canal2021",
    "passwd": "canal@Hopson2018",
    "db": 'puppet'
}

def get_db():
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',autocommit=True)
    return conn

def get_db_dict():
    conn = pymysql.connect(host=MYSQL_SETTINGS['host'],
                           port=int(MYSQL_SETTINGS['port']),
                           user=MYSQL_SETTINGS['user'],
                           passwd=MYSQL_SETTINGS['passwd'],
                           db=MYSQL_SETTINGS['db'],
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True)
    return conn

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def get_readable_time(seconds):
    m,s = divmod(seconds,60)
    h,m = divmod(m,60)
    return '{}时{}分{}秒'.format(h,m,s)

def get_file_and_pos():
    cr = get_db().cursor()
    cr.execute('show master status')
    rs = cr.fetchone()
    cr.close()
    return rs

def get_market_mame(p_market_id):
    cr = get_db().cursor()
    cr.execute("select dmmc from t_dmmx where dm='05' and dmm='{}'".format(p_market_id))
    rs = cr.fetchone()
    cr.close()
    return rs[0]

def get_server_desc(p_server_id):
    cr = get_db().cursor()
    cr.execute("select server_desc2 from t_server where  id='{}'".format(p_server_id))
    rs = cr.fetchone()
    cr.close()
    return rs[0]

def send_message(toUser,title,message):
    msg = {
        "title":title,
        "toUser":toUser,
        "description":message,
        "url":"ops.zhitbar.cn",
        "msgType":"textcard",
        "agentId":1000093
    }

    headers = {
        "User-Agent":"PostmanRuntime/7.26.8",
        "Accept":"*/*",
        "Accept-Encoding":"gzip, deflate, br",
        "Connection":"keep-alive",
        "Content-Type": "application/json"
    }

    try:
        print('send_message>>:',msg)
        chat_interface = "https://api.hopsontone.com/wxcp/message/template/send"
        r = requests.post(chat_interface, data=json.dumps(msg),headers=headers)
        print(r.text)
    except:
        print(traceback.print_exc())

def get_warn_info(server_id,index_code):
    db = get_db_dict()
    cr = db.cursor()
    st = '''select * from t_monitor_server_warn_log where server_id={} and index_code='{}' '''.format(server_id,index_code)
    cr.execute(st)
    rs = cr.fetchone()
    return rs

def write_warn_log(server_id,index_code,index_name,index_value,flag):
    db = get_db_dict()
    cr = db.cursor()
    if flag == 'failure':
        st ='''select count(0) as rec from t_monitor_server_warn_log where server_id={} and index_code='{}' '''.format(server_id,index_code)
        print(st)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec']==0:
            st = '''insert into t_monitor_server_warn_log(
                      server_id,server_desc,fail_times,succ_times,create_time,first_failure_time,is_send_rcv_mail,is_send_alt_mail,index_code,index_name,index_value) 
                     values({},'{}',{},{},'{}','{}',
                           '{}','{}','{}','{}','{}')
                 '''.format(server_id,
                            get_server_desc(server_id),
                            1,
                            0,
                            get_time(),
                            get_time(),
                            'N',
                            'N',
                            index_code,
                            index_name,
                            index_value
                            )
        else:
            st = '''update  t_monitor_server_warn_log 
                       set 
                          index_value='{}',
                          fail_times=fail_times+1,
                          first_failure_time=case when first_failure_time is null  then now() else first_failure_time end,
                          succ_times = 0,
                          is_send_rcv_mail='N',
                          -- is_send_alt_mail='N',
                          update_time=now() 
                      where server_id={} and index_code='{}'
                 '''.format(index_value,server_id,index_code)
        print('write_warn_log=>failure=',st)
        cr.execute(st)
        db.commit()

    if flag =='success':
        st ='''select count(0) as rec from t_monitor_server_warn_log 
                 where server_id={} and index_code='{}' '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec']==0:
            st = '''insert into t_monitor_server_warn_log(
                      server_id,server_desc,fail_times,succ_times,create_time,first_failure_time,is_send_rcv_mail,is_send_alt_mail,index_code,index_name,index_value) 
                     values({},'{}',{},{},'{}','{}',
                           '{}','{}','{}','{}','{}')
                 '''.format(server_id,
                            get_server_desc(server_id),
                            1,
                            0,
                            get_time(),
                            get_time(),
                            'N',
                            'N',
                            index_code,
                            index_name,
                            index_value
                            )
        else:
            st = '''update  t_monitor_server_warn_log 
                               set 
                                  index_value='{}',
                                  succ_times=succ_times+1,
                                  fail_times=0,
                                  update_time=now() ,
                                  first_failure_time=null,
                                  is_send_alt_mail_times=0
                              where server_id={} and index_code='{}'
                         '''.format(index_value,server_id,index_code)
        print('write_warn_log=>success=', st)
        cr.execute(st)
        db.commit()

    if flag =='recover':
        st ='''select count(0) as rec from t_monitor_server_warn_log 
                  where server_id={} and index_code='{}' '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_server_warn_log 
                       set 
                          is_send_alt_mail='N',
                          is_send_rcv_mail='Y',
                          fail_times  = 0,
                          first_failure_time=null,
                          is_send_alt_mail_times=0,
                          update_time = now() 
                      where server_id = {} and index_code='{}'
                         '''.format(server_id,index_code)
            print('write_warn_log=>recover=', st)
            cr.execute(st)
            db.commit()

    if flag == 'warning':
        st = '''select count(0) as rec from t_monitor_server_warn_log 
                  where server_id={} and index_code='{}' '''.format(server_id,index_code)
        cr.execute(st)
        rs = cr.fetchone()
        if rs['rec'] > 0:
            st = '''update  t_monitor_server_warn_log 
                       set 
                          is_send_alt_mail='Y',
                          is_send_alt_mail_times=is_send_alt_mail_times+1,
                          update_time = now() 
                      where server_id = {} and index_code='{}'
                         '''.format(server_id, index_code)
            print('write_warn_log=>warning=', st)
            cr.execute(st)
            db.commit()

def monitor(cfg):
    try:
        file, pos = get_file_and_pos()[0:2]
        fail_times = int(600 / int(cfg['api_request_gap_sleep']))
        stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                    only_events=(DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent),
                                    server_id=9999,
                                    blocking=True,
                                    resume_stream=True,
                                    log_file=file,
                                    log_pos=int(pos))
        for binlogevent in stream:
            if  isinstance(binlogevent, UpdateRowsEvent) or  isinstance(binlogevent, WriteRowsEvent):
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema, "table": binlogevent.table}
                    if event['schema'] == 'puppet' and event['table']=='t_monitor_api_log':
                        if isinstance(binlogevent, WriteRowsEvent):
                                event["action"] = "insert"
                                event["data"] = row["values"]
                                print('insert:', event)
                                if event["data"]['api_status'] !='200':
                                   write_warn_log(event['data']['server_id'],
                                                  event['data']['index_code'],
                                                  event['data']['index_name'],
                                                  event["data"]['api_interface'],'failure')
                                   warn_info = get_warn_info(event['data']['server_id'], event['data']['index_code'])

                                   if (warn_info['is_send_alt_mail'] == 'N' and warn_info['fail_times'] > 1
                                       or  warn_info['is_send_alt_mail'] == 'Y' and warn_info['fail_times']>0
                                          and warn_info['fail_times'] % fail_times == 0  and warn_info['is_send_alt_mail_times'] <=10 ) :
                                       print('warn_fail_times=',warn_info['fail_times'])
                                       print('fail_times=',fail_times)
                                       print('fail_times-2=',warn_info['fail_times'] % fail_times)
                                       write_warn_log(event['data']['server_id'],
                                                      event['data']['index_code'],
                                                      event['data']['index_name'],
                                                      event["data"]['api_interface'],'warning')
                                       message = ALERT_MESSAGE. \
                                                     format(event["data"]['market_id'],
                                                            get_market_mame(event["data"]['market_id']),
                                                            get_server_desc(event['data']['server_id']),
                                                            event["data"]['api_interface'],
                                                            warn_info['first_failure_time'],
                                                            get_readable_time(get_seconds(warn_info['first_failure_time'])),
                                                            event["data"]['update_time'],
                                                            event["data"]['api_status'],
                                                            event["data"]['api_message']
                                                           )
                                       print(message)
                                       send_message(cfg['api_interface_mail'], ALERT_TITLE, message)

                                else:
                                    write_warn_log(event['data']['server_id'],
                                                   event['data']['index_code'],
                                                   event['data']['index_name'],
                                                   event["data"]['api_interface'], 'success')

                                    warn_info = get_warn_info(event['data']['server_id'], event['data']['index_code'])
                                    if warn_info['is_send_alt_mail'] == 'Y' and warn_info['succ_times'] == 2  and warn_info['is_send_rcv_mail'] == 'N':
                                       write_warn_log(event['data']['server_id'],
                                                      event['data']['index_code'],
                                                      event['data']['index_name'],
                                                      event["data"]['api_interface'], 'recover')
                                       message = RECOVER_MESSAGE. \
                                                    format(event["data"]['market_id'],
                                                           get_market_mame(event["data"]['market_id']),
                                                           get_server_desc(event['data']['server_id']),
                                                           event["data"]['api_interface'],
                                                           event["data"]['api_status'],
                                                           event["data"]['api_message'],
                                                           event["data"]['update_time'])
                                       print(message)
                                       send_message(cfg['api_interface_mail'], RECOVER_TITLE, message)



                        if isinstance(binlogevent, UpdateRowsEvent):
                            event["action"] = "update"
                            event["after_values"] = row["after_values"]
                            event["before_values"] = row["before_values"]
                            print('update:',event)
                            if event["after_values"]['api_status'] != '200':
                                write_warn_log(event['after_values']['server_id'],
                                               event['after_values']['index_code'],
                                               event['after_values']['index_name'],
                                               event["after_values"]['api_interface'], 'failure')
                                warn_info = get_warn_info(event['after_values']['server_id'],
                                                          event['after_values']['index_code'])

                                if (warn_info['is_send_alt_mail'] == 'N' and warn_info['fail_times'] > 1 or \
                                        warn_info['is_send_alt_mail'] == 'Y' and warn_info['fail_times'] % 60 == 0):
                                    write_warn_log(event['after_values']['server_id'],
                                                   event['after_values']['index_code'],
                                                   event['after_values']['index_name'],
                                                   event["after_values"]['api_interface'], 'warning')
                                    message = ALERT_MESSAGE. \
                                        format(event["after_values"]['market_id'],
                                               get_market_mame(event["after_values"]['market_id']),
                                               get_server_desc(event['after_values']['server_id']),
                                               event["after_values"]['api_interface'],
                                               event["after_values"]['api_status'],
                                               event["after_values"]['api_message'],
                                               event["after_values"]['update_time'],
                                               get_seconds(warn_info['first_failure_time']))
                                    print(message)
                                    send_message(SEND_USER, ALERT_TITLE, message)

                            else:
                                write_warn_log(event['after_values']['server_id'],
                                               event['after_values']['index_code'],
                                               event['after_values']['index_name'],
                                               event["after_values"]['api_interface'], 'success')

                                warn_info = get_warn_info(event['after_values']['server_id'],
                                                          event['after_values']['index_code'])
                                if warn_info['is_send_alt_mail'] == 'Y' \
                                        and warn_info['succ_times'] == 2 and warn_info['is_send_rcv_mail'] == 'N':
                                    message = RECOVER_MESSAGE. \
                                        format(event["after_values"]['market_id'],
                                               get_market_mame(event["after_values"]['market_id']),
                                               get_server_desc(event['after_values']['server_id']),
                                               event["after_values"]['api_interface'],
                                               event["after_values"]['api_status'],
                                               event["after_values"]['api_message'],
                                               event["after_values"]['update_time'])
                                    print(message)
                                    send_message(SEND_USER, RECOVER_TITLE, message)



    except Exception as e:
        traceback.print_exc()
    finally:
        stream.close()

def check_pid(cfg):
    return os.path.isfile('{}/{}'.format(cfg['script_path'],'api_agent.json'))

def read_pid(cfg):
    with open('{}/{}'.format(cfg['script_path'],'api_agent.json'), 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        return json.loads(contents)

def upd_pid(cfg):
    ckpt = {
       'pid':cfg['pid']
    }
    with open('{}/{}'.format(cfg['script_path'],'api_agent.json'), 'w') as f:
        f.write(json.dumps(ckpt, ensure_ascii=False, indent=4, separators=(',', ':')))

def get_task_status(cfg):
    if check_pid(cfg):
        pid = read_pid(cfg).get('pid')
        if pid :
            if pid == os.getpid():
               return False

            if pid in psutil.pids():
              return True
            else:
              return False
        else:
            return False
    else:
        return False

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def get_config_from_db(tag):
    url = 'http://$$API_SERVER$$/read_config_alert'
    res = requests.post(url, data={'tag': tag})
    if res.status_code == 200:
        return res.json()['msg']
    else:
        print('get_config_from_db api failure! - ',res.text)
        sys.exit(0)

def main():
    tag=''
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            tag = sys.argv[p + 1]

    # get config
    cfg = get_config_from_db(tag)

    # check task
    if not get_task_status(cfg):

        # print cfg
        print_dict(cfg)

        # refresh pid
        cfg['pid'] = os.getpid()
        upd_pid(cfg)

        # monitor and alert
        monitor(cfg)
    else:
       print('api_agent.py already running!')


if __name__ == '__main__':
    main()
