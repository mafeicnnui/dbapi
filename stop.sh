#!/usr/bin/env bash
ps -ef |grep dbapi |awk '{print $2}' | xargs kill -9
