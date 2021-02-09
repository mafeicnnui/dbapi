#!/usr/bin/env bash
source /home/hopson/.bash_profile
export PYTHON3_HOME=/home/hopson/apps/usr/webserver/dba/python3.6.0
export LD_LIBRARY_PATH=/home/hopson/apps/usr/webserver/dba/python3.6.0/lib
export SCRIPT_PATH=/home/hopson/apps/usr/webserver/dba/script
$PYTHON3_HOME/bin/python3 $SCRIPT_PATH/$1 -tag $2