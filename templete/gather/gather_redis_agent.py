#!/usr/bin/env python3
import os
import sys
import time
import warnings
import datetime
import json
import requests
import psutil
import redis
import decimal

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(20,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(20,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

def get_config_from_db(tag):
    url = 'http://$$API_SERVER$$/read_config_monitor'
    res = requests.post(url,data={'tag': tag},timeout=3)
    if res.status_code == 200:
       cfg =  res.json()['msg']
       cfg['db_redis'] = get_redis_db(cfg)
       return cfg
    else:
        print('get_config_from_db failed,message:{}'.res.text)
        sys.exit(0)

def aes_decrypt(p_password,p_key):
    par = { 'password': p_password,  'key':p_key }
    try:
        url = 'http://$$API_SERVER$$/read_db_decrypt'
        res = requests.post(url, data=par,timeout=1).json()
        if res['code'] == 200:
            config = res['msg']
            return config
        else:
            print('Api read_db_decrypt call failure!,{0}'.format(res['msg']))
    except:
        print('aes_decrypt api not available!')

def get_redis_db(cfg):
    db_ip = cfg['db_ip']
    db_port = cfg['db_port']
    db_user = cfg['db_user']
    db_pass = aes_decrypt(cfg['db_pass'], db_user)
    r= redis.StrictRedis(host=db_ip, port=int(db_port), password=db_pass)
    return r

def get_redis_slowlog(cfg):
    r = cfg['db_redis']
    v = r.slowlog_get(8192)
    return v

def write_redis_log(res):
    msg = json.dumps({'data':res}, cls=DateEncoder)
    url = 'http://$$API_SERVER$$/write_redis_log'
    res= requests.post(url,data={'tag': msg})
    if res.status_code != 200:
       print('Api write_redis_log call failed!')
    else:
       print('Api write_redis_log call success!')

def timestamp_datetime(value):
     dt = datetime.datetime.fromtimestamp(value)
     return dt

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def gather(cfg):
    redis_slowlog_timeout = cfg['templete_monitor_indexes'][0]['index_threshold']
    while True:
        res = []
        batch_id = int(time.time())
        for key in get_redis_slowlog(cfg):
            key['batch_id'] = batch_id
            key['start_time'] = timestamp_datetime(key['start_time'])
            key['duration'] = round(key['duration'] / 1000, 2)
            del key['id']
            if get_seconds(key['start_time'])<=600:
                if key['duration']>int(redis_slowlog_timeout):
                   res.append({
                       'dbid': cfg.get('db_id'),
                       'batch_id': key.get('batch_id'),
                       'start_time': key.get('start_time'),
                       'duration': key.get('duration'),
                       'command': str(key.get('command'),'UTF-8'),
                       'index_code': cfg['templete_indexes_values'][0].get('index_code'),
                       'index_name': cfg['templete_indexes_values'][0].get('index_name'),
                      }
                   )
        if len(res)>0:
            write_redis_log(res)
        print('redis_agent_sleep:{}'.format(cfg['redis_agent_sleep']))
        time.sleep(int(cfg['redis_agent_sleep']))


def check_pid(cfg):
    return os.path.isfile('{}/{}'.format(cfg['script_path'],'redis_agent_{}.json'.format(cfg['task_tag'])))

def read_pid(cfg):
    with open('{}/{}'.format(cfg['script_path'],'redis_agent_{}.json'.format(cfg['task_tag'])), 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        return json.loads(contents)

def upd_pid(cfg):
    ckpt = {
       'pid':cfg['pid']
    }
    with open('{}/{}'.format(cfg['script_path'],'redis_agent_{}.json'.format(cfg['task_tag'])), 'w') as f:
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

def main():
    config = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
    # get cfg
    cfg=init(config)

    # check task
    if not get_task_status(cfg):
        # print cfg
        print_dict(cfg)

        # refresh pid
        cfg['pid'] = os.getpid()
        upd_pid(cfg)

        # call api and write log
        gather(cfg)
    else:
        print('gather_redis_agent.py already running!')

if __name__ == "__main__":
     main()
