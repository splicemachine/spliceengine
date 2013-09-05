#!/bin/bash

if ps ax | grep -v grep | grep 'exec:java' > /dev/null 
then
 pid=$(ps ax | grep -v grep | grep 'exec:java' | awk '{print $1}')
 kill -9 $pid
 sleep 60
else
 echo "zoo is not running!!"
fi

if ps ax | grep -v grep | grep 'exec:exec' > /dev/null 
then
 pid=$(ps ax | grep -v grep | grep 'exec:exec' | awk '{print $1}')
 kill -9 $pid
 sleep 60
else
 echo "splice is not running!!"
fi


if ps ax | grep -v grep | grep 'SpliceEngine-Build-Test' > /dev/null 
then
 echo "Test pid identified $pid"
 pid=$(ps ax | grep -v grep | grep 'SpliceEngine-Build-Test' | awk '{print $1}')
 kill -9 $pid
 sleep 60
fi

if [ ! -d "structured_derby/logs" ]; then
  mkdir structured_derby/logs
fi
currentDateTime=$(date +'%m-%d-%Y-%H_%M_%S')

cp structured_derby/zoo.log structured_derby/logs/$currentDateTime.zoo.log
cp structured_derby/server.log structured_derby/logs/$currentDateTime.server.log
