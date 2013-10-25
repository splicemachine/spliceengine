#!/bin/bash

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')

echo "Shutting down splice at $currentDateTime" >> server.log

pid=$(ps ax | grep -v grep | grep 'exec:exec' | grep 'DspliceCI' | awk '{print $1}')
if [ ! -z "$pid" ]; then
    #    kill -9 $pid
    kill -15 $pid
 sleep 30
else
 echo "splice is not running!!" >> server.log
fi


pid=$(ps ax | grep -v grep | grep 'SpliceTestPlatform' | awk '{print $1}')
if [ ! -z "$pid" ]; then
 echo "Test pid identified $pid" >> server.log
#    kill -9 $pid
    kill -15 $pid
 sleep 30
fi

if [ ! -d "logs" ]; then
  mkdir logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')
cp server.log logs/$currentDateTime.server.log
cp derby.log logs/$currentDateTime.derby.log
