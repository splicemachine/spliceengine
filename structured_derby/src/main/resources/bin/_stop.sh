#!/bin/bash

PID=$(ps ax | grep -v grep | grep 'SpliceSinglePlatform' | awk '{print $1}')
if [ -n "$PID" ]; then
    #echo "Spliiiiceee!"
    kill -15 $PID
    sleep 60
fi

PID=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
if [ -n "$PID" ]; then
    #echo "Zoooooo1"
    kill -15 ${PID}
    sleep 15
fi

PID=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
if [ -n "$PID" ]; then
    #echo "Zoooooo2"
    kill -9 ${PID}
fi
