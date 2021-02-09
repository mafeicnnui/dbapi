#!/usr/bin/env bash
export PYTHON3_HOME=$$PYTHON3_HOME$$
export LD_LIBRARY_PATH=$$PYTHON3_HOME$$/lib
export SCRIPT_PATH=$$SCRIPT_PATH$$
$PYTHON3_HOME/bin/python3 $SCRIPT_PATH/$1 -tag $2