#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/7 8:45
# @Author : maf.fei
# @File : mysql_async.py.py
# @Software: PyCharm

import json

from  aiomysql import create_pool,DictCursor

def read_json(file):
    with open(file, 'r') as f:
         cfg = json.loads(f.read())
    return cfg

def capital_to_lower(dict_info):
    new_dict = {}
    for i, j in dict_info.items():
        new_dict[i.lower()] = j
    return new_dict

db = read_json('./config/config.json')

class async_processer:

    async def query_list(p_sql):
        async with create_pool(host=db['db_ip'], port=int(db['db_port']), user=db['db_user'], password=db['db_pass'],
                               db=db['db_service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    v_list = []
                    rs = await cur.fetchall()
                    for r in rs:
                        v_list.append(list(r))
        return v_list

    async def query_list_by_ds(p_ds, p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    v_list = []
                    rs = await cur.fetchall()
                    for r in rs:
                        v_list.append(list(r))
        #print(p_sql)
        return v_list


    async def query_one(p_sql):
        async with create_pool(host=db['db_ip'], port=int(db['db_port']), user=db['db_user'], password=db['db_pass'],
                               db=db['db_service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    rs = await cur.fetchone()
        #print(p_sql)
        return rs

    async def query_one_desc(p_sql):
        async with create_pool(host=db['db_ip'], port=int(db['db_port']), user=db['db_user'], password=db['db_pass'],
                               db=db['db_service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    desc = cur.description
        return desc

    async def query_one_by_ds(p_ds, p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)
                    rs = await cur.fetchone()
        return rs

    async def exec_sql(p_sql):
        async with create_pool(host=db['db_ip'], port=int(db['db_port']), user=db['db_user'], password=db['db_pass'],
                               db=db['db_service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    #print('exec:{}'.format(p_sql))
                    await cur.execute(p_sql)

    async def exec_sql_by_ds(p_ds,p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(p_sql)


    async def query_dict_list(p_sql):
        async with create_pool(host=db['db_ip'], port=int(db['db_port']), user=db['db_user'], password=db['db_pass'],
                               db=db['db_service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(p_sql)
                    v_list = []
                    rs = await cur.fetchall()
                    for r in rs:
                        v_list.append(capital_to_lower(r))
        #print(p_sql)
        return v_list

    async def query_dict_list_by_ds(p_ds, p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(p_sql)
                    v_list = []
                    rs = await cur.fetchall()
                    for r in rs:
                        v_list.append(capital_to_lower(r))
        return v_list

    async def query_dict_one(p_sql):
        async with create_pool(host=db['db_ip'], port=int(db['db_port']), user=db['db_user'], password=db['db_pass'],
                               db=db['db_service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(p_sql)
                    rs = await cur.fetchone()
        #print(p_sql)
        return capital_to_lower(rs)

    async def query_dict_one_by_ds(p_ds,p_sql):
        async with create_pool(host=p_ds['ip'], port=int(p_ds['port']), user=p_ds['user'], password=p_ds['password'],
                               db=p_ds['service'], autocommit=True) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(DictCursor) as cur:
                    await cur.execute(p_sql)
                    rs = await cur.fetchone()
        return None if rs is None else capital_to_lower(rs)