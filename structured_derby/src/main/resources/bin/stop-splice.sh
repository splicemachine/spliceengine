#!/bin/bash

echo "Shutting down Splice Machine..."

PID=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
if [ ! -z "$PID" ]; then
    kill -15 ${PID}
    sleep 30
fi

PID=$(ps ax | grep -v grep | grep 'SpliceSinglePlatform' | awk '{print $1}')
if [ ! -z "$PID" ]; then
    kill -15 $PID
fi
