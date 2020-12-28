#!/usr/bin/env bash
export PYTHON3_HOME=$$PYTHON3_HOME$$
export LD_LIBRARY_PATH=$$PYTHON3_HOME$$/lib
export SCRIPT_PATH=$$SCRIPT_PATH$$
i_counter=`ps -ef | grep db_agent.py | grep -v grep | grep -v vi | wc -l`

if [ "${i_counter}" -eq "0" ]; then
   echo 'starting db_agent...'
   nohup $PYTHON3_HOME/bin/python3 $SCRIPT_PATH/db_agent.py $$PORT$$ &
fi


