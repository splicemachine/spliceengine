#!/bin/bash

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down zookeeper at $currentDateTime" >> zoo.log

pid=$(ps ax | grep -v grep | grep 'exec:exec -Dzoo' | awk '{print $1}')
if [ ! -z "$pid" ]; then
    #    kill -9 $pid
    kill -15 $pid
    sleep 30
else
    echo "zoo is not running!!" >> zoo.log
fi

pid=$(ps ax | grep -v grep | grep 'ZooKeeperServerMain' | awk '{print $1}')
if [ ! -z "$pid" ]; then
    echo "Zookeeper pid identified $pid" >> zoo.log
    #    kill -9 $pid
    kill -15 $pid
    sleep 30
fi

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp zoo.log logs/$currentDateTime.zoo.log
