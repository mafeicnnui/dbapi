## 一、功能概述  
  
    1、dbops 数据库自动化平台提供远程访问接口服务(python3+tornado)
    2、提供备份、同步、监控代理或客户端进行远程备份、同步操作


## 二、客户端介绍
         
  备份客户端：

| 脚本描述          | 功能描述          | 备注              |
| ----------------- | :---------------- | ----------------- |
| mysql备份脚本     | mysql_backup.py   | 支持windows,linux |
| sqlserver备份脚本 | mssql_backup.py   | 支持windows,linux |
| oracle备份脚本    | oracle_backup.py  | 支持windows.linux |
| mongo备份脚本     | mongo_backup.py   | 支持linux         |
| elastic备份脚本   | elastic_backup.py | 支持linux         |
| redis备份脚本     | redis_backup.py   | 支持linux         |


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

### 5.1 获取镜像

    docker pull mafeicnnui/dbapi:2.0

### 5.2 配置数据源

    mkdir /home/dbops
    vi config.json 
    {
        "db_ip"        : "192.168.1.100”,
        "db_port"      : "3306",
        "db_user"      : "puppet",
        "db_pass"      : "Abcd@1234",
        "db_service"   : "puppet",
        "db_charset"   : "utf8"
    }


### 5.3 运行容器

    docker run \
       --name dbapi \
       -p 8081:8081 \
       -v /home/dbapi/config.json:/opt/dbapi/config/config.json:ro \
       -d mafeicnnui/dbapi:2.0
    
### 5.4 测试 dbapi
    
    curl --head http://ip:8081/health
    