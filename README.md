## 一、功能概述  
  
    1、dbops 数据库自动化平台提供远程访问接口服务(python3+tornado)
    2、提供备份、同步、监控代理或客户端进行远程备份、同步操作


## 二、客户端介绍
         
  - 备份客户端：

| 脚本描述          | 脚本名称         | 备注              |
| ----------------- | :---------------- | ----------------- |
| mysql备份脚本     | mysql_backup.py   | 支持windows,linux |
| sqlserver备份脚本 | mssql_backup.py   | 支持windows,linux |
| oracle备份脚本    | oracle_backup.py  | 支持windows.linux |
| mongo备份脚本     | mongo_backup.py   | 支持linux         |
| elastic备份脚本   | elastic_backup.py | 支持linux         |
| redis备份脚本     | redis_backup.py   | 支持linux         |



- 同步客户端：

| 脚本描述                  | 脚本名称                      | 备注              |
| ------------------------- | :---------------------------- | ----------------- |
| mysql->mysq同步l脚本      | mysql2mysql_sync.py           | 离线同步          |
| sqlserver->mysql同步脚本  | mssql2mysql_sync.py           | 离线同步          |
| pg->mysql同步脚本         | pg2mysql_sync.py              | 离线同步          |
| mysql-->doris同步脚本     | mysql2doris_sync.py           | 离线同步          |
| mysql->mysql实时同步脚本  | mysql2mysql_real_syncer_v3.py | 实时同步-采集日志 |
| mysql->mysql实时同步脚本  | mysql2mysql_real_executer.py  | 实时同步-应用日志 |
| mysql->mysql实时同步脚本  | mysql2mysql_real_clear.py     | 实时同步-日志清理 |
| mysql->clickhouse实时同步 | mysql2clickhouse_syncer_v3.py | 实时同步-采集日志 |
| mysql->clickhouse实时同步 | mysql2clickhouse_executer.py  | 实时同步-应用日志 |
| mysql->clickhouse实时同步 | mysql2clickhouse_clear.py     | 实时同步-日志清理 |
|                           |                               |                   |

- 采集客户端：

- 告警客户端：

- 归档客户端：

- 传输客户端：

- dataX客户端：

## 三、安装部署  

### 3.1 安装python3
    
     yum -y install python3

### 3.2 安装依赖

     pip3 install -r requirements.txt -i https://pypi.douban.com/simple

## 四、停启服务

### 4.1 启动服务  

    start.sh

### 4.2 重启服务  

   restart.sh  

### 4.3 停止服务  

    stop.sh  

## 五、docker部署 

### 5.1 配置数据源

    mkdir /home/dbops
    vi config.json 
    {
        "db_ip"        : "x.x.x.x”,
        "db_port"      : "3306",
        "db_user"      : "puppet",
        "db_pass"      : "Puppet@123",
        "db_service"   : "puppet",
        "db_charset"   : "utf8"
    }

### 5.2 运行容器

    docker run \
      --name dbapi \
      -p 8081:8081 \
      -v /home/dbops/config.json:/opt/dbapi/config/config.json:ro \
      -d mafeicnnui/dbapi:3.0