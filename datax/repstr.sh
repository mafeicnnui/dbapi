#!/usr/bin/env bash
cat $1 | tr -d '\r' >$1.tmp
rm $1
mv $1.tmp $1