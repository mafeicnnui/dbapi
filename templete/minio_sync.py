#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : 马飞
# @File : analysis_slowlog_detail_ecs.py.py
# @Software: PyCharm

'''
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/python3.6.0
export LD_LIBRARY_PATH=$PYTHON3_HOME/lib
cd /home/hopson/apps/usr/webserver/python3.6.0/bin
./pip3 install BeautifulSoup4==4.9.1
./pip3 install html5lib
./pip3 install requests
./pip3 install minio==6.0

'''

import json
from urllib import parse
import traceback
import warnings
import sys
import urllib.parse
import urllib.request
import ssl
import os
import datetime
import requests
from bs4 import BeautifulSoup
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,BucketAlreadyExists)

def get_day():
    return datetime.datetime.now().strftime("%Y%m%d")

def get_time():
    now_time = datetime.datetime.now()
    time1_str = datetime.datetime.strftime(now_time, '%Y-%m-%d %H:%M:%S')
    return time1_str

def get_nday_list(n):
    before_n_days = []
    for i in range(0, n + 1)[::-1]:
        rq = datetime.date.today() - datetime.timedelta(days=i)
        vrq = datetime.datetime.strftime(rq,'%Y')+'/'+str(int(datetime.datetime.strftime(rq,'%m')))+'/'+str(int(datetime.datetime.strftime(rq,'%d')))
        before_n_days.append(vrq)
    return before_n_days

def get_nday_list2(n):
    before_n_days = []
    for i in range(0, n + 1)[::-1]:
        rq = datetime.date.today() - datetime.timedelta(days=i)
        vrq = datetime.datetime.strftime(rq,'%Y%m%d')
        before_n_days.append(vrq)
    return before_n_days

def get_config_from_db(p_tag):
    values = {
        'tag': p_tag
    }
    print('values=', values)
    url = 'http://$$API_SERVER$$/read_minio_config'
    context = ssl._create_unverified_context()
    data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
    print('data=', data)
    req = urllib.request.Request(url, data=data)
    res = urllib.request.urlopen(req, context=context)
    res = json.loads(res.read())
    print(res, res['code'])
    if res['code'] == 200:
        print('接口调用成功!')
        config = res['msg']
        config['download_rq']   = get_nday_list(config['minio_incr']-1) if config['sync_type']=='2' else get_nday_list2(config['minio_incr']-1)
        config['download_dir']  =  config['minio_dpath'] if config['sync_type']=='2' else config['sync_path']
        config['upload_root']   = '{}/Upload/Images/'.format(config['download_dir']) if config['sync_type']=='2' else config['sync_path']+'/'
        config['bucket_name']   = config['minio_bucket']
        config['minio_client']  = Minio(config['minio_server'],
                                        access_key=config['minio_user'],
                                        secret_key=config['minio_pass'],secure=False)
        return config
    else:
        print('接口调用失败!,{0}'.format(res['msg']))
        return None

def print_dict(config):
    print('-'.ljust(85,'-'))
    print(' '.ljust(3,' ')+"name".ljust(30,' ')+'value')
    print('-'.ljust(85,'-'))
    for key in config:
      print(' '.ljust(3,' ')+key.ljust(30,' ')+'=',config[key])
    print('-'.ljust(85,'-'))

def init(config):
    config = get_config_from_db(config)
    print_dict(config)
    return config

def write_minio_log(config):
    try:
        v_tag = {
            'sync_tag'     : config.get('sync_tag'),
            'server_id'    : config.get('server_id'),
            'download_time': config.get('download_time',0),
            'upload_time'  : config.get('upload_time',0),
            'sync_day'     : config.get('minio_incr',0),
            'total_time'   : config.get('download_time',0)+config.get('upload_time',0),
            'transfer_file': config.get('images_amount',0),
        }
        v_msg = json.dumps(v_tag)
        values = {
            'tag': v_msg
        }
        url = 'http://$$API_SERVER$$/write_minio_log'
        context = ssl._create_unverified_context()
        data = urllib.parse.urlencode(values).encode(encoding='UTF-8')
        req = urllib.request.Request(url, data=data)
        res = urllib.request.urlopen(req, context=context)
        res = json.loads(res.read())
        if res['code'] != 200:
            print('Interface write_minio_log call failed!',res['msg'])
        else:
            print('Interface write_minio_log call success!')
    except:
        print(traceback.print_exc())

def get_url(p_root, p_url, p_img):
    html_contents = requests.get(p_url).text
    soup = BeautifulSoup(html_contents, 'html5lib')
    a_tag = soup.find_all('a')
    if a_tag == []:
        return
    for k in a_tag:
        n_url = p_root + k['href'][1:]
        if len(n_url) > len(p_url):
            if n_url.find('.jpg') > 0 or n_url.find('.png') > 0:
                p_img.append(parse.unquote(n_url))
            else:
                get_url(p_root, n_url, p_img)


def check_exists(cfg,obj):
    try:
        minioClient = cfg['minio_client']
        minioClient.stat_object(config['bucket_name'],obj)
        return True
    except:
        return False

def get_seconds(b):
    a=datetime.datetime.now()
    return int((a-b).total_seconds())

def request_download(p_root,p_dir,p_url):
    r = requests.get(p_url)
    f = p_url.split('/')[-1]
    p = '/'.join(p_url.split('/')[0:-1]).replace(p_root,'')
    if not os.path.isdir(os.path.join(p_dir,p)):
       os.makedirs(os.path.join(p_dir,p))
       print('\ncreate directory {} ok'.format(os.path.join(p_dir,p)))
    with open(os.path.join(p_dir,p,f), 'wb') as f:
        f.write(r.content)

def dif_time(p_tm,p_cfg):
    now_time = datetime.datetime.now()
    now_time = now_time.strftime('%Y%m%d%H%M%S')
    d1       = datetime.datetime.strptime(p_tm, '%Y%m%d%H%M%S')
    d2       = datetime.datetime.strptime(now_time,'%Y%m%d%H%M%S')
    sec      = (d2 - d1).seconds
    if p_cfg['minio_incr_type'] == 'hour':
       return round(sec/3600,2)

    if p_cfg['minio_incr_type'] == 'min':
       return round(sec/3600*60,2)


def fitler_img(img,cfg):
    filter = []
    if cfg['minio_incr_type'] in('hour','min'):
        for p in img:
            tm = p.split('/')[-1].split('_')[-1].split('.')[0][0:14]
            if dif_time(tm,cfg)<=int(cfg['minio_incr']):
               filter.append(p)
        return filter
    else:
        return img

def downloads_image(config):
    root = config['sync_service']
    url  = config['download_url']
    dir  = config['download_dir']
    img  = []
    start_time = datetime.datetime.now()
    print('Requesting remote image ...')
    get_url(root, url, img)
    img = fitler_img(img,config)

    if len(img) == 0:
        print('Downloading Images amount=0,sync abort!')
        sys.exit(0)

    config['images_amount'] = len(img)

    for p in img:
        print('\rDownloading image:{} to {}'.format(p,dir),end='')
        request_download(root, dir, p)
    print('')
    print('Downloading image amount :{}'.format(config['images_amount']), end='\n')
    config['download_time'] = get_seconds(start_time)


def get_upload_image_amount(config):
    i_counter = 0
    for root, dirs, files in os.walk(config['upload_path']):
        if len(files) > 0:
            for file in files:
                i_counter = i_counter + 1
    return i_counter



def upload_imags(config):
    try:
        minioClient = config['minio_client']
        minioClient.make_bucket(config['bucket_name'], location="cn-north-1")
        print('Bucket {0} created!'.format(config['bucket_name']))
    except BucketAlreadyOwnedByYou as err:
        pass
    except BucketAlreadyExists as err:
        print('Bucket {0} already exists!'.format(config['bucket_name']))
    except ResponseError as err:
        pass

    if config['sync_type'] == '1':
        config['images_amount'] = get_upload_image_amount(config)

    i_counter  = 0
    start_time = datetime.datetime.now()
    print('From {} Uploading...'.format(config['upload_path']))
    for root, dirs, files in os.walk(config['upload_path']):
        if len(files) > 0:
            for file in files:
                try:
                    full_name = root + '/' + file
                    obj_name  = full_name.replace(config['upload_root'], '')
                    i_counter = i_counter + 1
                    if not check_exists(config,obj_name):
                        print('\rUploading Time:{}s,progress:[{}/{}]/{}%'.
                              format(get_seconds(start_time), config['images_amount'], i_counter,
                                     round(i_counter / config['images_amount'] * 100, 2)), end='')
                        minioClient.fput_object(config['bucket_name'],obj_name,full_name,'image/jpeg')
                    else:
                        print('\rUploading Time:{}s,progress:[{}/{}]/{}%,file already exists skip...'.
                              format(get_seconds(start_time), config['images_amount'], i_counter,
                                     round(i_counter / config['images_amount'] * 100, 2)), end='')
                except ResponseError as err:
                    print(err)
    config['upload_time'] = get_seconds(start_time)


'''
  1.下载图片至/tmp目录
  2.上传图片至MinIO Server
'''
def sync(config):

    if config['sync_type'] == '2':
       print('Downloading Images file from web server:{}/Upload/Images'.format(config['sync_service']))
       downloads_image(config)
       print('Uploading Images to MiniIO Server:{} =>bucket:{}'.format(config['minio_server'],config['bucket_name']))
       upload_imags(config)
       write_minio_log(config)

    if config['sync_type'] == '1':
       print('Uploading Images to MiniIO Server:{} =>bucket:{}'.format(config['minio_server'], config['bucket_name']))
       upload_imags(config)
       write_minio_log(config)

    # delete temp file
    os.system('rm -rf  /tmp/downloads/Upload/Images/*')
    print('delete temp file ok!')


if __name__ == "__main__":
    config = ""
    mode   = ""
    warnings.filterwarnings("ignore")
    for p in range(len(sys.argv)):
        if sys.argv[p] == "-tag":
            config = sys.argv[p + 1]
        else:
            pass
    # init
    config = init(config)

    # ping
    ip = 'ping -w 10 {}'.format(config['sync_service'].split('//')[1].split(':')[0])
    print(ip)
    r = os.popen(ip)
    for i in r:
        print(i)

    print_dict(config)

    # sync
    for rq in config['download_rq']:
        config['download_url'] = "{}Upload/Images/{}/".format(config['sync_service'], rq)
        if config['sync_type'] == '2':
           config['upload_path'] = '{}/Upload/Images/{}/'.format(config['download_dir'], rq)
        else:
           config['upload_path'] = '{}/{}/'.format(config['download_dir'], rq)
        sync(config)