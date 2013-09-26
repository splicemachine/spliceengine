#!/bin/bash

mvn exec:java -Dzoo > zoo.log &

if ps ax | grep -v grep | grep 'exec:java' > /dev/null 
then
 sleep 15
else
 echo "zoo is not running!!"
fi
