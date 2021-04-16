#!/usr/bin/env bash
export WORKDIR="/home/hopson/apps/usr/webserver/dbapi"
export PYTHONUNBUFFERED="1"
export PYTHONPATH=${WORKDIR}
export PYTHON3_HOME=/usr/local/python3.6
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib

echo "Testing dbapi Server..."
for i in {18161..18200}
do
    if [ `ps -ef |grep dbapi | grep -v grep | grep ${i} | wc -l` == '0' ]
    then
      ${PYTHON3_HOME}/bin/python3 ${WORKDIR}/dbapi.py ${i} &>/dev/null &
      echo "Startup dbapi server ${i} ...ok"
    fi
done
echo "Testing dbapi Server...ok"


if [ `ps -ef |grep nginx | grep -v grep | wc -l` == '0' ]
then
      sudo nginx &
      echo "Startup nginx server...ok"
fi