#!/bin/bash

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down zookeeper at $currentDateTime" >> zoo.log

if ps ax | grep -v grep | grep 'exec:java' > /dev/null 
then
 pid=$(ps ax | grep -v grep | grep 'exec:java' | awk '{print $1}')
 kill -9 $pid
 sleep 60
else
 echo "zoo is not running!!"
fi

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp zoo.log logs/$currentDateTime.zoo.log
