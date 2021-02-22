一、概述  

   平台功能： 为Easebase 数据库自动化平台提供API接口服务(python3+tornado)
   

二、安装部署  

2.1 安装python3
    
     yum -y install python3

2.2 安装依赖

     pip3 install -r requirements.txt -i https://pypi.douban.com/simple

三、停启服务

3.1 启动服务  

    start.sh

3.2 重启服务  

   restart.sh  

3.3 停止服务  

    stop.sh  

四、docker部署 

4.1 获取镜像

    docker pull mafeicnnui/dbapi:2.0

4.2 配置数据源

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


4.3 运行容器

    docker run \
       --name dbapi \
       -p 8081:8081 \
       -v /home/dbapi/config.json:/opt/dbapi/config/config.json:ro \
       -d mafeicnnui/dbapi:2.0
    
4.4 测试 dbapi
    
    curl --head http://ip:8081/health
    