#!/usr/bin/env bash
export WORKDIR=`pwd`
export PYTHONUNBUFFERED="1"
export PYTHONPATH=${WORKDIR}
export PYTHON3_HOME=/usr/local/python3.6
export LD_LIBRARY_PATH=${PYTHON3_HOME}/lib
echo "Starting dbapi Server..."
for i in {18161..18200}
do
  if [ `ps -ef |grep dbapi | grep ${i} | wc -l` == '1' ]
  then
     echo "dbapi Server ${i} already running..."
  else
     ${PYTHON3_HOME}/bin/python3 -u ${WORKDIR}/dbapi.py ${i} &>/dev/null &
  fi
done
echo "Starting dbapi Server...ok"
