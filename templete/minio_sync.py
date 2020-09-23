#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 16:59
# @Author : 马飞
# @File : analysis_slowlog_detail_ecs.py.py
# @Software: PyCharm

import json
import re
import hashlib
import pymysql
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
        config['download_rq']   = '2020/9/23'
        config['download_url']  = "{}Upload/Images/{}/".format(config['sync_service'],config['download_rq'])
        config['download_dir']  = "/tmp/downloads"
        config['upload_root']   = '{}/Upload/Images/'.format(config['download_dir'])
        config['upload_path']   = '{}/Upload/Images/{}/'.format(config['download_dir'],config['download_rq'] )
        config['bucket_name']   = '999'
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

def write_minio_log(item,msg):
    try:
        v_tag = {
            'inst_id'    : item.get('inst_id'),
            'message'    : msg,
            'type'       : item.get('mode')
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
                p_img.append(n_url)
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
       print('create directory {} ok'.format(os.path.join(p_dir,p)))
    with open(os.path.join(p_dir,p,f), 'wb') as f:
        f.write(r.content)

def downloads_image(config):
    root = config['sync_service']
    url  = config['download_url']
    dir  = config['download_dir']
    img  = []
    print('Requesting remote image ...')
    get_url(root, url, img)

    if len(img) == 0:
        print('Downloading Images amount=0,sync abort!')
        sys.exit(0)

    config['images_amount'] = len(img)

    for p in img:
        print('\rDownloading image:{} to {}'.format(p,dir),end='')
        request_download(root, dir, p)
    print('')
    print('Downloading image amount :{}'.format(config['images_amount']), end='\n')


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

    i_counter  = 0
    start_time = datetime.datetime.now()
    for root, dirs, files in os.walk(config['upload_path']):
        if len(files) > 0:
            for file in files:
                try:
                    full_name = root + '/' + file
                    obj_name  = full_name.replace(config['upload_root'], '')

                    if not check_exists(config,obj_name):
                        i_counter = i_counter+1
                        print('\rUploading obj:{},Time:{}s,progress:({}/{}){}%'.
                               format(obj_name,
                                      get_seconds(start_time),
                                      config['images_amount'],
                                      i_counter,
                                      round(i_counter / config['images_amount'] * 100, 2)
                                      ), end='')
                        minioClient.fput_object(config['bucket_name'],obj_name,full_name,'image/jpeg')
                    else:
                        print('\rUploading obj: {0} already exists bucket {1},skipping..'.
                              format(obj_name, config['bucket_name']), end='\n')

                except ResponseError as err:
                    print(err)


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

    if config['sync_type'] == '1':
        pass


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

    # sync
    sync(config)

