#!/bin/bash

currentDateTime=$(date +'%m-%d-%Y:%H:%M:%S')
cd structured_derby
mvn exec:exec -DspliceCI > server.log &

if ps ax | grep -v grep | grep 'exec:exec' > /dev/null 
then
 sleep 300
else
 echo "splice is not running!!"
fi
