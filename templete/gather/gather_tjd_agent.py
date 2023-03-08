#!/usr/bin/env python3
import os
import sys
import time
import warnings
import datetime
import json
import requests
import psutil

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
       return res.json()['msg']
    else:
        print('get_config_from_db failed,message:{}'.res.text)
        sys.exit(0)

def get_api_status(cfg,idx):
    try:
        api = idx['index_threshold']
        data = json.loads(cfg['tjd_api_request_body']['data'])
        begin_time=datetime.datetime.now()
        print('api=',api)
        res= requests.post(url=api,data=data,timeout=3)
        print('res.json=',res.json())
        end_time = datetime.datetime.now()
        return {
            'market_id' : cfg['market_id'],
            'server_id' : cfg['server_id'],
            'index_code' : idx['index_code'],
            'index_name' : idx['index_name'],
            'api_interface': idx['index_threshold'],
            'api_status': res.json()['code'],
            'response_time': round((end_time-begin_time).microseconds/1000,2),
            'response_text': res.json()['msg'],
            'request_body' : json.dumps(data),
        }
    except:
        try:
            return {
                'market_id': cfg['market_id'],
                'server_id': cfg['server_id'],
                'index_code': idx['index_code'],
                'index_name': idx['index_name'],
                'api_interface': idx['index_threshold'],
                'api_status': 201,
                'response_time': 0,
                'response_text': res.text,
                'request_body': json.dumps(data)
            }
        except:
            return {
                'market_id': cfg['market_id'],
                'server_id': cfg['server_id'],
                'index_code': idx['index_code'],
                'index_name': idx['index_name'],
                'api_interface': idx['index_threshold'],
                'api_status': 202,
                'response_time': 0,
                'response_text': '接口不可用!',
                'request_body': json.dumps(data)
            }

def write_api_log(item,Timeout=False):
    if Timeout:
        par = {
            'market_id'     : item.get('market_id'),
            'server_id'     : item.get('server_id'),
            'index_code'    : item.get('index_code'),
            'index_name'    : item.get('index_name'),
            'api_interface' : item.get('api_interface'),
            'api_status'    : 408,
            'response_time' : item.get('response_time'),
            'response_text' : '请求超时!',
            'request_body'  : item.get('request_body'),
        }
    else:
        par = {
            'market_id': item.get('market_id'),
            'server_id': item.get('server_id'),
            'index_code': item.get('index_code'),
            'index_name': item.get('index_name'),
            'api_interface': item.get('api_interface'),
            'api_status': item.get('api_status'),
            'response_time': item.get('response_time'),
            'response_text': item.get('response_text'),
            'request_body': item.get('request_body'),
        }
    msg = json.dumps(par)
    print('msg=',msg)
    url = 'http://$$API_SERVER$$/write_api_log'
    res= requests.post(url,data={'tag': msg})
    if res.status_code != 200:
       print('Api write_api_log call failed!')
    else:
       print('Api write_api_log call success!')

def gather(cfg):
    for idx in cfg['templete_indexes_values']:
        while True:
            # first req
            res = get_api_status(cfg,idx)
            if res['api_status'] == 200:
                if res['response_time'] > int(cfg['api_request_timeout']):
                    write_api_log(res,True)
                    print('first request timeout,sleep {}s'.format(int(cfg['api_request_timeout_sleep'])))
                    time.sleep(int(cfg['api_request_timeout_sleep']))
                    # second req
                    res = get_api_status(cfg, idx)
                    if res['api_status'] == 200:
                        if res['response_time'] > int(cfg['api_request_timeout']):
                            write_api_log(res,True)
                            print('second request timeout,sleep {}s'.format(2*int(cfg['api_request_timeout_sleep'])))
                            time.sleep(2*int(cfg['api_request_timeout_sleep']))
                            # third req
                            res = get_api_status(cfg, idx)
                            if res['api_status'] == 200:
                                if res['response_time'] > int(cfg['api_request_timeout']):
                                    write_api_log(res, True)
                                    print('third request timeout,sleep {}s'.format(3 * int(cfg['api_request_timeout_sleep'])))
                                    time.sleep(3*int(cfg['api_request_timeout_sleep']))
                                else:
                                    write_api_log(res)
                    else:
                        write_api_log(res)
                else:
                     write_api_log(res)

            else:
                write_api_log(res)

            # req gap sleep
            print('request gap sleep {}s'.format(cfg['api_request_gap_sleep']))
            time.sleep(int(cfg['api_request_gap_sleep']))

def check_pid(cfg):
    return os.path.isfile('{}/{}'.format(cfg['script_path'],'api_agent_{}.json'.format(cfg['market_id'])))

def read_pid(cfg):
    with open('{}/{}'.format(cfg['script_path'],'api_agent_{}.json'.format(cfg['market_id'])), 'r') as f:
        contents = f.read()
    if  contents == '':
        return ''
    else:
        return json.loads(contents)

def upd_pid(cfg):
    ckpt = {
       'pid':cfg['pid']
    }
    with open('{}/{}'.format(cfg['script_path'],'api_agent_{}.json'.format(cfg['market_id'])), 'w') as f:
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
        print('gather_tjd_agent.py already running!')

if __name__ == "__main__":
     main()
