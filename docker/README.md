一、编写Dockerfile

    FROM python:3.6
    ADD req.txt /opt
    ADD dbapi.tar /opt/
    RUN /usr/local/bin/python \
    -m pip install --upgrade pip \
    && pip3 install -r /opt/req.txt -i https://pypi.douban.com/simple \
    && chmod a+x /opt/dbops/*.sh
    WORKDIR /opt/dbapi
    ENTRYPOINT ["/opt/dbapi/start_docker"]

二、打包为镜像

    docker build -t dbapi:2.0 .

三、进入容器

    docker exec -it 7afe808da8ae /bin/sh

四、上传hub

    docker tag dbapi:2.0 mafeicnnui/dbapi:2.0
    docker push mafeicnnui/dbapi:2.0

五、拉取镜像

    docker pull mafeicnnui/dbapi:2.0

六、配置数据源

    mkdir /home/dbops
    vi config.json
    {
        "db_ip"         : "rm-2ze2k586u0g2hnbaqfo.mysql.rds.aliyuncs.com",
        "db_port"      : "3306",
        "db_user"      : "puppet",
        "db_pass"      : "Puppet@123",
        "db_service"   : "easebase",
        "db_charset"   : "utf8"
    }

七、启动容器

    docker run \
       --name easebase \
       -p 8081:8081 \
       -v /home/dbapi/config.json:/opt/dbapi/config/config.json:ro \
       -d mafeicnnui/dbapi:2.0
       
八、测试 dbapi
    
    curl --head http://ip:8081/health
           