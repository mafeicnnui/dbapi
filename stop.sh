#!/usr/bin/env bash
echo "Stopping dbapi Server..."
for i in {18161..18200}
do
 if [ `ps -ef |grep dbapi | grep -v grep | grep ${i} | wc -l` == '1' ]
 then
    ps -ef |grep dbapi | grep ${i} | awk '{print $2}' | xargs kill -9
 fi
done
echo "Stopping dbapi Server...ok"