一、概述  

   平台功能： 为Easebase 数据库自动化平台提供API接口服务(python3+tornado)
   

二、安装部署  

2.1 安装python3
    
     yum -y install python3

2.2 安装依赖

     pip3 install -r requirements.txt -i https://pypi.douban.com/simple

三、停启服务

3.1 启动服务  

    cd dbapi
    ./start.sh

3.2 重启服务  

    cd dbapi
    ./restart.sh  

3.3 停止服务  

    cd dbapi
    ./stop.sh  

