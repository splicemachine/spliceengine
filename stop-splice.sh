#!/bin/bash

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down splice at $currentDateTime" >> server.log

if ps ax | grep -v grep | grep 'SpliceTestPlatform' > /dev/null
then
 pid=$(ps ax | grep -v grep | grep 'SpliceTestPlatform' | awk '{print $1}')
 kill -9 $pid
 sleep 60
else
 echo "splice is not running!!"
fi


if ps ax | grep -v grep | grep 'SpliceTestPlatform' > /dev/null 
then
 echo "Test pid identified $pid"
 pid=$(ps ax | grep -v grep | grep 'SpliceTestPlatform' | awk '{print $1}')
 kill -9 $pid
 sleep 60
fi

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp server.log logs/$currentDateTime.server.log
