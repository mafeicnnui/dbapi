#!/usr/bin/env bash
export WORKDIR=`pwd`
echo "Starting dbapi Server..."
for i in {8181..8200}
do
  if [ `ps -ef |grep dbapi | grep -v grep | grep ${i} | wc -l` == '1' ]
  then
     echo "Dbops Server ${i} already running..."
  else
     python3 ${WORKDIR}/dbapi.py ${i} &>/dev/null &
  fi
done
echo "Starting dbapi Server...ok"