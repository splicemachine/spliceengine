#!/bin/bash

if ps ax | grep -v grep | grep 'exec:java' > /dev/null 
then
 pid=$(ps ax | grep -v grep | grep 'exec:java' | gawk '{print $1}')
 kill -9 $pid
 sleep 60
else
 echo "zoo is not running!!"
fi

if ps ax | grep -v grep | grep 'exec:exec' > /dev/null 
then
 pid=$(ps ax | grep -v grep | grep 'exec:exec' | gawk '{print $1}')
 kill -9 $pid
 sleep 60
else
 echo "splice is not running!!"
fi

cp structured_derby/zoo.log structured_derby/logs/zoo.log.$currentDateTime

cp structured_derby/server.log structured_derby/logs/server.log.$currentDateTime