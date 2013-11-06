#!/bin/bash

CYGWIN=`uname -s`
if [[ ${CYGWIN} == CYGWIN* ]]; then
    PID=$(ps ax | grep -v grep | grep 'java' | awk '{print $1}')
    [[ -n ${PID} ]] && for p in ${PID}; do kill -15 `echo ${p}`; done
else
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
fi
